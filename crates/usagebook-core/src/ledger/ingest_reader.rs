//! Fixed worker slots and a single bounded, killable fallback reader process.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex, OnceLock,
};
const FRAME_LIMIT: usize = 32 * 1024 * 1024;
static HELPER_ACTIVE: AtomicBool = AtomicBool::new(false);
static IS_HELPER: AtomicBool = AtomicBool::new(false);
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Job {
    pub task: String,
    pub path: PathBuf,
    pub root: String,
    pub object_id: i64,
    pub generation: String,
    pub len: i64,
    pub mtime: Option<i64>,
    pub offset: i64,
    pub audit: bool,
    pub stream: bool,
    pub proof: Option<PrefixProof>,
    pub identity: Option<IdentityJob>,
    #[serde(skip)]
    pub cancel: Option<Arc<AtomicBool>>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct PrefixProof {
    pub len: i64,
    pub digest: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct FileVersion {
    device: u64,
    file: u64,
    change: Option<(i64, i64)>,
}
#[cfg(unix)]
fn file_version(_: Option<&File>, meta: &Metadata) -> Result<FileVersion> {
    use std::os::unix::fs::MetadataExt;
    Ok(FileVersion {
        device: meta.dev(),
        file: meta.ino(),
        change: Some((meta.ctime(), meta.ctime_nsec())),
    })
}
#[cfg(windows)]
fn file_version(file: Option<&File>, _: &Metadata) -> Result<FileVersion> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };
    let file = file.ok_or(LedgerError::StaleObservation)?;
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) } == 0 {
        return Err(source_io(std::io::Error::last_os_error()));
    }
    let info = unsafe { info.assume_init() };
    Ok(FileVersion {
        device: info.dwVolumeSerialNumber as u64,
        file: ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64,
        change: None,
    })
}
#[cfg(not(any(unix, windows)))]
fn file_version(_: Option<&File>, _: &Metadata) -> Result<FileVersion> {
    Err(LedgerError::InvalidInput(
        "SourceIdentityUnsupported".into(),
    ))
}
pub(super) fn require_version(
    path: &Path,
    len: i64,
    mtime: Option<i64>,
    expected: &FileVersion,
) -> Result<()> {
    #[cfg(windows)]
    let file = Some(File::open(path).map_err(source_io)?);
    #[cfg(not(windows))]
    let file: Option<File> = None;
    let meta = fs::metadata(path).map_err(source_io)?;
    if metadata_stamp(&meta) != (len, mtime) || file_version(file.as_ref(), &meta)? != *expected {
        return Err(LedgerError::StaleObservation);
    }
    Ok(())
}
fn require_open_version(file: &File, job: &Job, expected: &FileVersion) -> Result<()> {
    let meta = file.metadata().map_err(source_io)?;
    if metadata_stamp(&meta) != (job.len, job.mtime)
        || file_version(Some(file), &meta)? != *expected
    {
        return Err(LedgerError::StaleObservation);
    }
    require_version(&job.path, job.len, job.mtime, expected)
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct IdentityJob {
    pub ledger: PathBuf,
    pub observation: Observation,
    pub purpose: IdentityPurpose,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
pub(super) enum IdentityPurpose {
    CounterpartAlreadySuppressed,
    IncomingAlreadySuppressed,
}
#[derive(Serialize, Deserialize)]
pub(super) struct IdentityProof {
    pub counterpart: String,
    pub guard_digest: String,
    pub epoch: String,
}
#[derive(Serialize, Deserialize)]
pub(super) enum Event {
    Progress {
        task: String,
    },
    Prefix {
        task: String,
        accepted: bool,
        version: FileVersion,
    },
    Identity {
        task: String,
        proof: Option<IdentityProof>,
    },
    Batch {
        task: String,
        observations: Vec<Observation>,
        offset: i64,
        audit_bytes: Option<String>,
        ignored: u64,
        version: Option<FileVersion>,
    },
    Done {
        task: String,
        bytes: u64,
        parsed_bytes: u64,
        seal: Option<Vec<u8>>,
        version: Option<FileVersion>,
        more: bool,
        error: Option<String>,
    },
}
impl Event {
    pub fn task(&self) -> &str {
        match self {
            Self::Batch { task, .. }
            | Self::Done { task, .. }
            | Self::Progress { task }
            | Self::Identity { task, .. }
            | Self::Prefix { task, .. } => task,
        }
    }
}
pub(super) struct Pool {
    sender: mpsc::SyncSender<Job>,
    receiver: mpsc::Receiver<Event>,
    pub slots: usize,
    pub occupied: std::collections::BTreeSet<String>,
    helper: Option<std::process::Child>,
    helper_task: Option<String>,
    helper_sender: mpsc::SyncSender<Event>,
    cancellations: std::collections::BTreeMap<String, Arc<AtomicBool>>,
}
impl Pool {
    fn new() -> Self {
        let slots = std::env::var("USAGEBOOK_LEDGER_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2)
            .clamp(2, 4);
        let (sender, jobs) = mpsc::sync_channel::<Job>(slots);
        let jobs = Arc::new(Mutex::new(jobs));
        // At most two <=32 MiB encoded batches wait for the writer: <=64 MiB.
        let (events, receiver) = mpsc::sync_channel(2);
        for index in 0..slots {
            let jobs = jobs.clone();
            let events = events.clone();
            std::thread::Builder::new()
                .name(format!("ledger-reader-{index}"))
                .spawn(move || loop {
                    let job = match jobs.lock().unwrap_or_else(|e| e.into_inner()).recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    };
                    run(&job, |event| {
                        events.send(event).map_err(|_| LedgerError::Cancelled)
                    });
                })
                .expect("bounded ledger worker thread");
        }
        Self {
            sender,
            receiver,
            slots,
            occupied: Default::default(),
            helper: None,
            helper_task: None,
            helper_sender: events,
            cancellations: Default::default(),
        }
    }
    pub fn dispatch(&mut self, mut job: Job) -> Result<()> {
        let cancel = Arc::new(AtomicBool::new(false));
        self.cancellations.insert(job.task.clone(), cancel.clone());
        job.cancel = Some(cancel);
        self.occupied.insert(job.task.clone());
        self.sender.send(job).map_err(|_| LedgerError::Cancelled)
    }
    pub fn available(&self) -> usize {
        self.slots.saturating_sub(self.occupied.len())
    }
    pub fn next(&mut self) -> Option<Event> {
        match self
            .receiver
            .recv_timeout(std::time::Duration::from_millis(25))
        {
            Ok(e) => {
                if matches!(e, Event::Done { .. }) {
                    self.occupied.remove(e.task());
                    self.cancellations.remove(e.task());
                    if self.helper_task.as_deref() == Some(e.task()) {
                        self.finish_helper();
                    }
                }
                Some(e)
            }
            Err(_) => None,
        }
    }
    pub fn cancel(&mut self, task: &str) -> Result<()> {
        if let Some(cancel) = self.cancellations.get(task) {
            cancel.store(true, Ordering::Relaxed);
        }
        self.cancel_helper(task)
    }
    pub fn has_quarantined_reader(&self) -> bool {
        self.cancellations
            .values()
            .any(|cancel| cancel.load(Ordering::Relaxed))
    }
    pub fn helper_available(&self) -> bool {
        self.helper.is_none()
    }
    pub fn dispatch_helper(&mut self, job: Job) -> Result<()> {
        if self.helper.is_some() {
            return Err(LedgerError::Busy);
        }
        let binary = std::env::var_os("USAGEBOOK_LEDGER_READER_BINARY")
            .map(PathBuf::from)
            .unwrap_or(std::env::current_exe()?);
        let child = std::process::Command::new(binary)
            .arg("__ledger-reader")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        // Install ownership immediately: every subsequent failure closes/reaps this child,
        // retaining the slot if the operating system has not confirmed process exit.
        self.helper_task = Some(job.task.clone());
        self.helper = Some(child);
        HELPER_ACTIVE.store(true, Ordering::Relaxed);
        let task_key = job.task.clone();
        let setup = (|| -> Result<()> {
            let child = self.helper.as_mut().ok_or(LedgerError::Cancelled)?;
            let mut input = child.stdin.take().ok_or(LedgerError::Cancelled)?;
            let mut output = child.stdout.take().ok_or(LedgerError::Cancelled)?;
            let sender = self.helper_sender.clone();
            let task = job.task.clone();
            std::thread::Builder::new()
                .name("ledger-helper-output".into())
                .spawn(move || {
                    if write_frame(&mut input, &job).is_err() {
                        let _ = sender.send(Event::Done {
                            task: task.clone(),
                            bytes: 0,
                            parsed_bytes: 0,
                            seal: None,
                            version: None,
                            more: false,
                            error: Some("ReaderHelperFailed".into()),
                        });
                        return;
                    }
                    drop(input);
                    loop {
                        match read_frame::<Event>(&mut output) {
                            Ok(e) => {
                                let done = matches!(e, Event::Done { .. });
                                if sender.send(e).is_err() || done {
                                    break;
                                }
                            }
                            Err(_) => {
                                let _ = sender.send(Event::Done {
                                    task: task.clone(),
                                    bytes: 0,
                                    parsed_bytes: 0,
                                    seal: None,
                                    version: None,
                                    more: false,
                                    error: Some("ReaderHelperFailed".into()),
                                });
                                break;
                            }
                        }
                    }
                })?;
            Ok(())
        })();
        if let Err(error) = setup {
            self.cancel_helper(&task_key)?;
            return Err(error);
        }
        Ok(())
    }
    fn finish_helper(&mut self) {
        if let Some(task) = self.helper_task.clone() {
            let _ = self.cancel_helper(&task);
        }
    }
    /// Confirmation is mandatory before a replacement helper can be started.
    pub fn cancel_helper(&mut self, task: &str) -> Result<()> {
        if self.helper_task.as_deref() != Some(task) {
            return Ok(());
        }
        if let Some(child) = self.helper.as_mut() {
            if child.try_wait()?.is_none() {
                child.kill()?;
            }
            let started = Instant::now();
            while child.try_wait()?.is_none() {
                if started.elapsed().as_secs() >= 2 {
                    return Err(LedgerError::IncompleteEvidence);
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        self.helper = None;
        self.helper_task = None;
        HELPER_ACTIVE.store(false, Ordering::Relaxed);
        Ok(())
    }
}
pub(super) fn pool() -> std::sync::MutexGuard<'static, Pool> {
    static POOL: OnceLock<Mutex<Pool>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(Pool::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}
fn write_frame<T: Serialize>(out: &mut impl std::io::Write, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| LedgerError::CorruptLedger)?;
    if bytes.len() > FRAME_LIMIT {
        return Err(LedgerError::RecordTooLarge);
    }
    out.write_all(&(bytes.len() as u32).to_be_bytes())?;
    out.write_all(&bytes)?;
    out.flush()?;
    Ok(())
}
fn read_frame<T: serde::de::DeserializeOwned>(input: &mut impl Read) -> Result<T> {
    let mut header = [0; 4];
    input.read_exact(&mut header)?;
    let size = u32::from_be_bytes(header) as usize;
    if size > FRAME_LIMIT {
        return Err(LedgerError::RecordTooLarge);
    }
    let mut bytes = vec![0; size];
    input.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|_| LedgerError::CorruptLedger)
}
pub(super) fn helper_main() -> Result<()> {
    IS_HELPER.store(true, Ordering::Relaxed);
    let job: Job = read_frame(&mut std::io::stdin().lock())?;
    if job.task.len() > 64 || job.generation.len() > 64 || job.offset < 0 || job.len < job.offset {
        return Err(LedgerError::InvalidInput("invalid reader job".into()));
    }
    let mut output = std::io::stdout().lock();
    run(&job, |event| write_frame(&mut output, &event));
    Ok(())
}
fn run(job: &Job, mut emit: impl FnMut(Event) -> Result<()>) {
    if job.identity.is_some() {
        run_identity(job, emit);
        return;
    }
    if job.stream {
        run_stream(job, emit);
        return;
    }
    let mut read_bytes = 0u64;
    let mut budget_bytes = 0u64;
    let mut heartbeat = Instant::now();
    let mut result = (|| -> Result<bool> {
        require_stamp(&job.path, job.len, job.mtime)?;
        let mut file = File::open(&job.path).map_err(source_io)?;
        file.seek(SeekFrom::Start(job.offset as u64))
            .map_err(source_io)?;
        let mut input =
            BufReader::with_capacity(64 * 1024, file.take((job.len - job.offset) as u64));
        let started = Instant::now();
        let mut offset = job.offset;
        let mut batch = Vec::new();
        let mut projected = 0usize;
        let mut raw = Vec::new();
        let mut ignored = 0u64;
        let flush = |batch: &mut Vec<Observation>,
                     raw: &mut Vec<u8>,
                     ignored: &mut u64,
                     offset: i64,
                     emit: &mut dyn FnMut(Event) -> Result<()>|
         -> Result<()> {
            require_stamp(&job.path, job.len, job.mtime)?;
            let audit_bytes = if job.audit {
                Some(STANDARD.encode(&*raw))
            } else {
                None
            };
            raw.clear();
            emit(Event::Batch {
                task: job.task.clone(),
                observations: std::mem::take(batch),
                offset,
                audit_bytes,
                ignored: std::mem::take(ignored),
                version: None,
            })
        };
        loop {
            check_cancelled(job)?;
            let mut line = Vec::new();
            let n = read_bounded_line(&mut input, &mut line, &mut |n| {
                read_bytes += n;
                check_cancelled(job)?;
                if job.audit {
                    budget_bytes += n;
                    if budget_bytes >= 64 * 1024 {
                        throttle_read(job, budget_bytes, &mut emit)?;
                        budget_bytes = 0;
                    }
                }
                if heartbeat.elapsed().as_millis() >= 250 {
                    emit(Event::Progress {
                        task: job.task.clone(),
                    })?;
                    heartbeat = Instant::now();
                }
                Ok(())
            })?;
            if n == 0 {
                flush(&mut batch, &mut raw, &mut ignored, offset, &mut emit)?;
                return Ok(false);
            }
            if n > MAX_LINE_BYTES {
                flush(&mut batch, &mut raw, &mut ignored, offset, &mut emit)?;
                return Err(LedgerError::RecordTooLarge);
            }
            if !line.ends_with(b"\n") {
                flush(&mut batch, &mut raw, &mut ignored, offset, &mut emit)?;
                return Ok(false);
            }
            let next = offset
                .checked_add(n as i64)
                .ok_or(LedgerError::NumericOverflow)?;
            let parsed = claude::visit_line(&line, |mut o| {
                o.object_id = job.object_id;
                o.content_generation = job.generation.clone();
                o.record_key = offset.to_string();
                o.observed_ms = now_ms();
                if o.candidate_key.is_empty() {
                    o.candidate_key = hash_parts(
                        "usagebook/unidentified",
                        &[
                            &job.root,
                            &path_text(&job.path)?,
                            &job.generation,
                            &o.record_key,
                        ],
                    );
                }
                o.normalized_hash = claude::normalized_hash(&o)?;
                let size = claude::normalized_bytes(&o)?.len();
                if batch.len() >= BATCH_ROWS || projected + size > SLICE_BYTES as usize {
                    flush(&mut batch, &mut raw, &mut ignored, offset, &mut emit)?;
                    projected = 0;
                }
                projected += size;
                batch.push(o);
                Ok(())
            });
            match parsed {
                Ok(n) => ignored += n,
                Err(e) => {
                    flush(&mut batch, &mut raw, &mut ignored, offset, &mut emit)?;
                    return Err(e);
                }
            }
            if job.audit {
                raw.extend_from_slice(&line);
            }
            offset = next;
            if offset - job.offset >= SLICE_BYTES as i64 || started.elapsed().as_millis() >= 100 {
                flush(&mut batch, &mut raw, &mut ignored, offset, &mut emit)?;
                require_stamp(&job.path, job.len, job.mtime)?;
                return Ok(offset < job.len);
            }
        }
    })();
    if budget_bytes > 0 {
        if let Err(error) = throttle_read(job, budget_bytes, &mut emit) {
            result = Err(error);
        }
    }
    let (more, error) = match result {
        Ok(more) => (more, None),
        Err(e) => (false, Some(e.code().to_owned())),
    };
    let _ = emit(Event::Done {
        task: job.task.clone(),
        bytes: read_bytes,
        parsed_bytes: read_bytes,
        seal: None,
        version: None,
        more,
        error,
    });
}
// A fixed large target stays in one slot/handle across all progress slices. The
// SHA value below is the internal state, never a finalized digest used as input.
// One such job is scheduled at a time; another slot remains available for small
// healthy files. 64 GiB is explicit: larger targets report a backed-off limit.
const MAX_STREAM_BYTES: i64 = 64 * 1024 * 1024 * 1024;
fn run_stream(job: &Job, mut emit: impl FnMut(Event) -> Result<()>) {
    let mut bytes = 0u64;
    let mut parsed_bytes = 0u64;
    let mut seal = None;
    let mut final_version = None;
    let started = Instant::now();
    // Budget scales to the fixed target (1 MiB/s + 60 s, <= 18h 13m). The
    // independent five-second no-progress watchdog owns slow-I/O helper fallback.
    let budget = std::time::Duration::from_secs(60 + job.len.max(0) as u64 / (1024 * 1024));
    let result = (|| -> Result<()> {
        if job.len > MAX_STREAM_BYTES {
            return Err(LedgerError::InvalidInput("SourceReadBudgetExceeded".into()));
        }
        if job.offset < 0 || job.offset > job.len {
            return Err(LedgerError::StaleObservation);
        }
        let check = || -> Result<()> {
            check_cancelled(job)?;
            if started.elapsed() > budget {
                return Err(LedgerError::InvalidInput("SourceReadBudgetExceeded".into()));
            }
            Ok(())
        };
        check()?;
        let mut file = File::open(&job.path).map_err(source_io)?;
        let meta = file.metadata().map_err(source_io)?;
        let version = file_version(Some(&file), &meta)?;
        require_open_version(&file, job, &version)?;
        let mut hash = Sha256::new();
        if let Some(proof) = &job.proof {
            if proof.len != job.offset || proof.digest.len() != 32 {
                return Err(LedgerError::StaleObservation);
            }
            let mut left = proof.len as u64;
            let mut buffer = [0u8; 64 * 1024];
            let mut progress = Instant::now();
            let mut slice = 0u64;
            while left > 0 {
                check()?;
                let cap = left.min(buffer.len() as u64) as usize;
                let n = file.read(&mut buffer[..cap]).map_err(source_io)?;
                if n == 0 {
                    return Err(LedgerError::StaleObservation);
                }
                hash.update(&buffer[..n]);
                bytes += n as u64;
                if job.audit {
                    throttle_read(job, n as u64, &mut emit)?;
                }
                left -= n as u64;
                slice += n as u64;
                if slice >= SLICE_BYTES || progress.elapsed().as_millis() >= 250 {
                    require_open_version(&file, job, &version)?;
                    emit(Event::Progress {
                        task: job.task.clone(),
                    })?;
                    std::thread::yield_now();
                    slice = 0;
                    progress = Instant::now();
                }
            }
            require_open_version(&file, job, &version)?;
            let accepted = hash.clone().finalize().as_slice() == proof.digest.as_slice();
            emit(Event::Prefix {
                task: job.task.clone(),
                accepted,
                version: version.clone(),
            })?;
            if !accepted {
                return Ok(());
            }
            super::super::types::regression_stop("prefix_after_verified");
        } else if job.offset != 0 {
            return Err(LedgerError::StaleObservation);
        }
        let mut input =
            BufReader::with_capacity(64 * 1024, file.take((job.len - job.offset) as u64));
        let mut offset = job.offset;
        let mut batch = Vec::new();
        let mut projected = 0usize;
        let mut ignored = 0u64;
        let mut slice_bytes = 0u64;
        let mut progress = Instant::now();
        let flush = |batch: &mut Vec<Observation>,
                     ignored: &mut u64,
                     offset: i64,
                     input: &BufReader<std::io::Take<File>>,
                     emit: &mut dyn FnMut(Event) -> Result<()>|
         -> Result<()> {
            check()?;
            require_open_version(input.get_ref().get_ref(), job, &version)?;
            emit(Event::Batch {
                task: job.task.clone(),
                observations: std::mem::take(batch),
                offset,
                audit_bytes: None,
                ignored: std::mem::take(ignored),
                version: Some(version.clone()),
            })
        };
        loop {
            check()?;
            let mut line = Vec::new();
            let n = read_bounded_line(&mut input, &mut line, &mut |n| {
                bytes += n;
                parsed_bytes += n;
                if job.audit {
                    throttle_read(job, n, &mut emit)?;
                }
                check()?;
                if progress.elapsed().as_millis() >= 250 {
                    emit(Event::Progress {
                        task: job.task.clone(),
                    })?;
                    progress = Instant::now();
                }
                Ok(())
            })?;
            if n == 0 {
                // A proof-only audit leaves its existing checkpoint and stamp intact.
                if offset != job.offset || !batch.is_empty() || ignored != 0 || job.proof.is_none()
                {
                    flush(&mut batch, &mut ignored, offset, &input, &mut emit)?;
                }
                require_open_version(input.get_ref().get_ref(), job, &version)?;
                if offset == job.len {
                    seal = Some(hash.finalize().to_vec());
                    final_version = Some(version);
                }
                return Ok(());
            }
            if n > MAX_LINE_BYTES {
                flush(&mut batch, &mut ignored, offset, &input, &mut emit)?;
                return Err(LedgerError::RecordTooLarge);
            }
            if !line.ends_with(b"\n") {
                flush(&mut batch, &mut ignored, offset, &input, &mut emit)?;
                return Ok(());
            }
            let next = offset
                .checked_add(n as i64)
                .ok_or(LedgerError::NumericOverflow)?;
            let parsed = claude::visit_line(&line, |mut o| {
                o.object_id = job.object_id;
                o.content_generation = job.generation.clone();
                o.record_key = offset.to_string();
                o.observed_ms = now_ms();
                if o.candidate_key.is_empty() {
                    o.candidate_key = hash_parts(
                        "usagebook/unidentified",
                        &[
                            &job.root,
                            &path_text(&job.path)?,
                            &job.generation,
                            &o.record_key,
                        ],
                    );
                }
                o.normalized_hash = claude::normalized_hash(&o)?;
                let size = claude::normalized_bytes(&o)?.len();
                if batch.len() >= BATCH_ROWS || projected + size > SLICE_BYTES as usize {
                    flush(&mut batch, &mut ignored, offset, &input, &mut emit)?;
                    projected = 0;
                }
                projected += size;
                batch.push(o);
                Ok(())
            });
            match parsed {
                Ok(n) => ignored += n,
                Err(error) => {
                    flush(&mut batch, &mut ignored, offset, &input, &mut emit)?;
                    return Err(error);
                }
            }
            hash.update(&line);
            offset = next;
            slice_bytes += n as u64;
            if slice_bytes >= SLICE_BYTES || progress.elapsed().as_millis() >= 100 {
                flush(&mut batch, &mut ignored, offset, &input, &mut emit)?;
                projected = 0;
                slice_bytes = 0;
                progress = Instant::now();
                std::thread::yield_now();
            }
        }
    })();
    let error = result.err().map(|error| match error {
        LedgerError::InvalidInput(ref message) if message == "SourceReadBudgetExceeded" => {
            message.clone()
        }
        _ => error.code().to_owned(),
    });
    let _ = emit(Event::Done {
        task: job.task.clone(),
        bytes,
        parsed_bytes,
        seal,
        version: final_version,
        more: false,
        error,
    });
}

fn check_cancelled(job: &Job) -> Result<()> {
    if job
        .cancel
        .as_ref()
        .is_some_and(|flag| flag.load(Ordering::Relaxed))
    {
        return Err(LedgerError::Cancelled);
    }
    Ok(())
}
// Audits and exceptional identity reads share the normal 4 MiB/s budget.
// While the one fallback helper is owned, each process uses half that budget.
// No IPC budget service is needed; the existing pool owns the helper lifecycle.
// Ordinary incremental readers never wait for this budget. Cancellation and
// progress continue during deliberate throttling, so it cannot mimic a stuck IO.
fn throttle_read(job: &Job, bytes: u64, emit: &mut impl FnMut(Event) -> Result<()>) -> Result<()> {
    static NEXT: OnceLock<Mutex<Instant>> = OnceLock::new();
    let deadline = {
        let mut next = NEXT
            .get_or_init(|| Mutex::new(Instant::now()))
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let rate = if HELPER_ACTIVE.load(Ordering::Relaxed) || IS_HELPER.load(Ordering::Relaxed) {
            SLICE_BYTES / 2
        } else {
            SLICE_BYTES
        };
        *next = (*next).max(Instant::now())
            + std::time::Duration::from_secs_f64(bytes as f64 / rate as f64);
        *next
    };
    let mut heartbeat = Instant::now();
    while Instant::now() < deadline {
        check_cancelled(job)?;
        if heartbeat.elapsed().as_millis() >= 250 {
            emit(Event::Progress {
                task: job.task.clone(),
            })?;
            heartbeat = Instant::now();
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(std::time::Duration::from_millis(25)),
        );
    }
    check_cancelled(job)
}
fn run_identity(job: &Job, mut emit: impl FnMut(Event) -> Result<()>) {
    let mut bytes = 0u64;
    let mut budget_bytes = 0u64;
    let mut heartbeat = Instant::now();
    let result = (|| -> Result<()> {
        check_cancelled(job)?;
        let identity = job
            .identity
            .as_ref()
            .ok_or(LedgerError::IncompleteEvidence)?;
        // Each worker owns its connection and maintenance lease. Guarded
        // short queries release the main read snapshot before slow source I/O.
        let ledger = Ledger::open(&identity.ledger, false)?;
        require_stamp(&job.path, job.len, job.mtime)?;
        super::super::types::regression_stop("identity_before_body");
        let proof = prove_identity_connection_with(
            &ledger.conn,
            &identity.observation,
            identity.purpose,
            &mut |n| {
                check_cancelled(job)?;
                bytes += n;
                budget_bytes += n;
                if budget_bytes >= 64 * 1024 {
                    throttle_read(job, budget_bytes, &mut emit)?;
                    budget_bytes = 0;
                }
                if heartbeat.elapsed().as_millis() >= 250 {
                    emit(Event::Progress {
                        task: job.task.clone(),
                    })?;
                    heartbeat = Instant::now();
                }
                Ok(())
            },
        )?;
        if budget_bytes > 0 {
            throttle_read(job, budget_bytes, &mut emit)?;
        }
        check_cancelled(job)?;
        require_stamp(&job.path, job.len, job.mtime)?;
        emit(Event::Identity {
            task: job.task.clone(),
            proof,
        })?;
        Ok(())
    })();
    let _ = emit(Event::Done {
        task: job.task.clone(),
        bytes,
        parsed_bytes: 0,
        seal: None,
        version: None,
        more: false,
        error: result.err().map(|e| e.code().to_owned()),
    });
}
pub(super) fn audit_bytes(encoded: &str) -> Result<Vec<u8>> {
    STANDARD
        .decode(encoded)
        .map_err(|_| LedgerError::CorruptLedger)
}

#[cfg(feature = "regression-hooks")]
pub(super) fn stale_helper_completion_regression() -> bool {
    let mut pool = Pool::new();
    pool.helper_task = Some("new-task".into());
    pool.helper_sender
        .send(Event::Done {
            task: "old-task".into(),
            bytes: 0,
            parsed_bytes: 0,
            seal: None,
            version: None,
            more: false,
            error: None,
        })
        .unwrap();
    let event = pool.next();
    let valid = event.is_some() && pool.helper_task.as_deref() == Some("new-task");
    pool.helper_task = None;
    valid
}

#[cfg(feature = "regression-hooks")]
pub(super) fn helper_input_backpressure_regression() -> Result<bool> {
    let mut pool = Pool::new();
    let mut observation = Observation::default();
    observation.native_session_id = Some("\u{1}".repeat(4096));
    observation.native_message_id = observation.native_session_id.clone();
    observation.native_request_id = observation.native_session_id.clone();
    let job = Job {
        task: "backpressure".into(),
        path: PathBuf::from("unused"),
        root: "root".into(),
        object_id: 0,
        generation: "test".into(),
        len: 0,
        mtime: None,
        offset: 0,
        audit: false,
        stream: false,
        proof: None,
        cancel: None,
        identity: Some(IdentityJob {
            ledger: PathBuf::from("unused"),
            observation,
            purpose: IdentityPurpose::CounterpartAlreadySuppressed,
        }),
    };
    let started = Instant::now();
    pool.dispatch_helper(job)?;
    let prompt = started.elapsed().as_millis() < 1000;
    pool.cancel("backpressure")?;
    Ok(prompt && pool.helper_available())
}

#[cfg(feature = "regression-hooks")]
pub(super) fn audit_heartbeat_regression(path: &Path) -> Result<bool> {
    // Called only in its isolated test subprocess; exercise the helper's slower
    // budget without replacing the real reader or depending on a CLI build.
    IS_HELPER.store(true, Ordering::Relaxed);
    let (len, mtime) = metadata_stamp(&fs::metadata(path)?);
    let job = Job {
        task: "audit-heartbeat".into(),
        path: path.to_owned(),
        root: "test".into(),
        object_id: 0,
        generation: "test".into(),
        len,
        mtime,
        offset: 0,
        audit: true,
        stream: false,
        proof: None,
        identity: None,
        cancel: None,
    };
    let mut progress_before_commit = 0;
    let mut committed = false;
    let mut completed = false;
    run(&job, |event| {
        match event {
            Event::Progress { .. } if !committed => progress_before_commit += 1,
            Event::Batch {
                offset,
                observations,
                ..
            } => {
                if offset != len || !observations.is_empty() {
                    return Err(LedgerError::IncompleteEvidence);
                }
                committed = true;
            }
            Event::Done {
                bytes, more, error, ..
            } => completed = bytes == len as u64 && !more && error.is_none(),
            _ => {}
        }
        Ok(())
    });
    Ok(progress_before_commit >= 2 && committed && completed)
}

#[cfg(feature = "regression-hooks")]
pub(super) fn stream_reader_regression(
    path: &Path,
    prefix_len: i64,
    digest: Vec<u8>,
    mut callback: impl FnMut(&str) -> Result<bool>,
) -> Result<serde_json::Value> {
    let (len, mtime) = metadata_stamp(&fs::metadata(path)?);
    let cancel = Arc::new(AtomicBool::new(false));
    let job = Job {
        task: "stream-regression".into(),
        path: path.to_owned(),
        root: "test".into(),
        object_id: 1,
        generation: "test".into(),
        len,
        mtime,
        offset: prefix_len,
        audit: false,
        stream: true,
        proof: (prefix_len > 0).then_some(PrefixProof {
            len: prefix_len,
            digest,
        }),
        identity: None,
        cancel: Some(cancel.clone()),
    };
    let mut events = Vec::new();
    run(&job, |event| {
        let phase = match &event {
            Event::Progress { .. } => "progress",
            Event::Prefix { .. } => "prefix",
            Event::Batch { .. } => "batch",
            Event::Done { .. } => "done",
            _ => "identity",
        };
        if callback(phase)? {
            cancel.store(true, Ordering::Relaxed);
        }
        events.push(serde_json::to_value(&event).map_err(|_| LedgerError::CorruptLedger)?);
        Ok(())
    });
    Ok(serde_json::Value::Array(events))
}

#[cfg(feature = "regression-hooks")]
pub(super) fn helper_stream_regression(
    path: &Path,
    prefix_len: i64,
    digest: Vec<u8>,
) -> Result<serde_json::Value> {
    let (len, mtime) = metadata_stamp(&fs::metadata(path)?);
    let mut job = Job {
        task: "stream-helper-old".into(),
        path: path.to_owned(),
        root: "test".into(),
        object_id: 1,
        generation: "test".into(),
        len,
        mtime,
        offset: prefix_len,
        audit: false,
        stream: true,
        proof: Some(PrefixProof {
            len: prefix_len,
            digest,
        }),
        identity: None,
        cancel: None,
    };
    let mut pool = Pool::new();
    pool.dispatch_helper(job.clone())?;
    let mut events = Vec::new();
    let mut restarted = false;
    let start = Instant::now();
    while start.elapsed().as_secs() < 15 {
        if let Some(event) = pool.next() {
            if !restarted && event.task() == "stream-helper-old" {
                pool.cancel("stream-helper-old")?;
                job.task = "stream-helper-new".into();
                pool.dispatch_helper(job.clone())?;
                restarted = true;
                continue;
            }
            if event.task() == "stream-helper-new" {
                let done = matches!(event, Event::Done { .. });
                events.push(serde_json::to_value(event).map_err(|_| LedgerError::CorruptLedger)?);
                if done {
                    return Ok(serde_json::Value::Array(events));
                }
            }
        }
    }
    pool.cancel(&job.task)?;
    Err(LedgerError::IncompleteEvidence)
}
