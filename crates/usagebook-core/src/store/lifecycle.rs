//! One stable lock file outside the replaceable cache directory.
//!
//! Every SQLite handle owns a shared lease for its entire lifetime. Recovery
//! closes its handles before releasing that lease and trying an exclusive one.
//! We never unlink this file: doing so would split users across lock inodes.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub(super) struct Lease {
    _file: File,
}

impl Lease {
    pub(super) fn shared(path: &Path) -> io::Result<Self> {
        Self::acquire(path, false)
    }

    pub(super) fn exclusive(path: &Path) -> io::Result<Self> {
        Self::acquire(path, true)
    }

    fn acquire(path: &Path, exclusive: bool) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        try_lock(&file, exclusive)?;
        Ok(Self { _file: file })
    }
}

#[cfg(unix)]
fn try_lock(file: &File, exclusive: bool) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let operation = if exclusive { libc::LOCK_EX } else { libc::LOCK_SH };
    loop {
        // SAFETY: flock receives a live file descriptor; the File outlives the lease.
        if unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(windows)]
fn try_lock(file: &File, exclusive: bool) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY};
    use windows_sys::Win32::System::IO::OVERLAPPED;
    let flags = LOCKFILE_FAIL_IMMEDIATELY | if exclusive { LOCKFILE_EXCLUSIVE_LOCK } else { 0 };
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    // SAFETY: the synchronous file handle is live, OVERLAPPED is valid for this
    // call, and the one-byte lock is released when the owning File closes.
    let result = unsafe { LockFileEx(file.as_raw_handle(), flags, 0, 1, 0, &mut overlapped) };
    if result == 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

#[cfg(not(any(unix, windows)))]
fn try_lock(_file: &File, _exclusive: bool) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "cache lifecycle locks are unsupported"))
}
