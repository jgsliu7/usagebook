//! Device-scoped opaque identifiers. Native keys never cross the network boundary.
use super::{types::*, Result};
#[cfg(windows)]
#[path = "sync_windows.rs"]
mod windows;
use hmac::{Hmac, Mac};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn hmac(secret: &[u8], bytes: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(bytes);
    format!("{:x}", mac.finalize().into_bytes())
}
pub fn new_secret() -> Vec<u8> {
    let mut secret = vec![0u8; 32];
    getrandom::fill(&mut secret).expect("operating system randomness unavailable");
    secret
}
pub fn identity_key_tag(secret: &[u8]) -> String {
    hmac(secret, b"identity-key-tag-v1")
}
#[allow(clippy::too_many_arguments)]
pub fn local_key(
    secret: &[u8],
    ledger: &str,
    domain: &str,
    source: &str,
    family: &str,
    namespace: &str,
    kind: &str,
    key: &str,
    occurrence: usize,
) -> String {
    hmac(
        secret,
        &serde_json::to_vec(&serde_json::json!([
            "ub-local-key-v1",
            ledger,
            domain,
            source,
            family,
            namespace,
            kind,
            key,
            occurrence
        ]))
        .expect("JSON array"),
    )
}
pub fn contribution_id(secret: &[u8], account: &str, device: &str, local: &str) -> String {
    hmac(
        secret,
        &serde_json::to_vec(&serde_json::json!([
            "ub-contribution-v1",
            account,
            device,
            local
        ]))
        .expect("JSON array"),
    )
}
/// Existing cloud history makes aggregate equality insufficient for route changes.
/// A caller may proceed only after a proved lineage bridge has been installed.
pub(crate) fn guard_change(c: &Connection, source: &str) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM sync_source_exports e JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND e.source=?1) OR EXISTS(SELECT 1 FROM sync_lineages l JOIN sync_bindings b USING(binding_id) WHERE b.is_current=1 AND l.source=?1 AND l.domain='live')", [source], |r| r.get(0))?)
}
pub(crate) fn inherited_id(c: &Connection, binding: &str, local: &str) -> Result<Option<String>> {
    Ok(c.query_row(
        "SELECT contribution_id FROM sync_lineages WHERE binding_id=?1 AND local_key=?2",
        params![binding, local],
        |r| r.get(0),
    )
    .optional()?)
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_lineage(
    c: &Connection,
    binding: &str,
    secret: &[u8],
    ledger: &str,
    source: &str,
    family: &str,
    namespace: &str,
    kind: &str,
    key: &str,
) -> Result<Option<String>> {
    let mut q = c.prepare("SELECT alias_key FROM identity_alias WHERE identity_family=?1 AND namespace_key=?2 AND canonical_key=?3 UNION SELECT ?3")?;
    let mut ids = std::collections::BTreeSet::new();
    for row in q.query_map(params![family, namespace, key], |r| r.get::<_, String>(0))? {
        let local = local_key(
            secret, ledger, "ledger", source, family, namespace, kind, &row?, 0,
        );
        if let Some(id) = inherited_id(c, binding, &local)? {
            ids.insert(id);
        }
    }
    if ids.len() > 1 {
        return Err(LedgerError::InvalidInput(
            "SYNC_ALIAS_MERGE_REQUIRES_BRIDGE".into(),
        ));
    }
    Ok(ids.into_iter().next())
}

/// Credential/host files are never included in database backup. Refuse symlink
/// substitution and shared permissions; writing uses fsync + atomic rename.
pub fn write_private_json(path: &std::path::Path, value: &serde_json::Value) -> Result<()> {
    use std::io::Write;
    let parent = path
        .parent()
        .ok_or_else(|| LedgerError::InvalidInput("CREDENTIAL_PERMISSIONS".into()))?;
    if !parent.exists() {
        #[cfg(not(windows))]
        std::fs::create_dir(parent)?;
        #[cfg(windows)]
        windows::create_dir(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    check_private(parent, true)?;
    if path.symlink_metadata().is_ok() {
        check_private(path, false)?;
    }
    let tmp = parent.join(format!(".{}.tmp", new_epoch()));
    #[cfg(not(windows))]
    let mut opts = std::fs::OpenOptions::new();
    #[cfg(not(windows))]
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let result = (|| -> Result<()> {
        #[cfg(not(windows))]
        let mut f = opts.open(&tmp)?;
        #[cfg(windows)]
        let mut f = windows::create_file(&tmp)?;
        f.write_all(&serde_json::to_vec(value).map_err(|_| LedgerError::CorruptLedger)?)?;
        f.sync_all()?;
        drop(f);
        #[cfg(not(windows))]
        std::fs::rename(&tmp, path)?;
        #[cfg(windows)]
        windows::replace(&tmp, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}
fn check_private(path: &std::path::Path, dir: bool) -> Result<()> {
    let m = path.symlink_metadata()?;
    if m.file_type().is_symlink() || (dir && !m.is_dir()) || (!dir && !m.is_file()) {
        return Err(LedgerError::InvalidInput("CREDENTIAL_PERMISSIONS".into()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if m.uid() != unsafe { libc::geteuid() }
            || m.permissions().mode()
                & (if dir && path.file_name().is_some_and(|n| n != "cloud-credentials") {
                    0o022
                } else {
                    0o077
                })
                != 0
        {
            return Err(LedgerError::InvalidInput("CREDENTIAL_PERMISSIONS".into()));
        }
    }
    #[cfg(windows)]
    windows::check(path, dir)?;
    Ok(())
}
pub fn read_private_json(path: &std::path::Path) -> Result<serde_json::Value> {
    if let Some(parent) = path.parent() {
        check_private(parent, true)?;
    }
    check_private(path, false)?;
    let bytes = std::fs::read(path)?;
    if bytes.len() > 16384 {
        return Err(LedgerError::InvalidInput("CREDENTIAL_PERMISSIONS".into()));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| LedgerError::InvalidInput("CREDENTIAL_INVALID".into()))
}

pub fn available_bytes(path: &std::path::Path) -> Result<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| LedgerError::InvalidInput("invalid path".into()))?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let stat = unsafe { stat.assume_init() };
        // f_bavail/f_frsize 在 macOS 为 u32/u64、Linux 为 u64/u64,as 是跨平台统一写法
        #[allow(clippy::unnecessary_cast)]
        Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
    }
    #[cfg(windows)]
    {
        windows::available_bytes(path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Ok(u64::MAX)
    }
}
