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
    pub identity: Option<IdentityJob>,
    #[serde(skip)]
    pub cancel: Option<Arc<AtomicBool>>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct IdentityJob {
    pub ledger: PathBuf,
    pub observation: Observation,
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
    },
    Done {
        task: String,
        bytes: u64,
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
            | Self::Identity { task, .. } => task,
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
        more,
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
        let proof =
            prove_identity_connection_with(&ledger.conn, &identity.observation, &mut |n| {
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
            })?;
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
        cancel: None,
        identity: Some(IdentityJob {
            ledger: PathBuf::from("unused"),
            observation,
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
