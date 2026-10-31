//! Windows private files are created with a protected DACL, before secret bytes
//! are written. Only the process user and LocalSystem receive access. Existing
//! credentials are checked rather than silently repairing a shared file.
use super::{LedgerError, Result};
use std::{
    ffi::c_void,
    fs::File,
    os::windows::{ffi::OsStrExt, fs::MetadataExt, io::FromRawHandle},
    path::Path,
    ptr::null_mut,
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, LocalFree, GENERIC_WRITE, INVALID_HANDLE_VALUE},
    Security::{
        Authorization::{
            ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
            GetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT,
        },
        EqualSid, GetAce, GetSecurityDescriptorControl, GetTokenInformation, IsWellKnownSid,
        TokenUser, WinBuiltinAdministratorsSid, WinLocalSystemSid, ACCESS_ALLOWED_ACE, ACE_HEADER,
        ACL, DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
        SECURITY_ATTRIBUTES, SE_DACL_PROTECTED, TOKEN_QUERY, TOKEN_USER,
    },
    Storage::FileSystem::{
        CreateDirectoryW, CreateFileW, GetDiskFreeSpaceExW, MoveFileExW, CREATE_NEW,
        FILE_ALL_ACCESS, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    },
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};
fn denied() -> LedgerError {
    LedgerError::InvalidInput("CREDENTIAL_PERMISSIONS".into())
}
fn wide(path: &Path) -> Result<Vec<u16>> {
    let value: Vec<_> = path.as_os_str().encode_wide().collect();
    if value.contains(&0) {
        return Err(denied());
    }
    Ok(value.into_iter().chain(Some(0)).collect())
}
struct Local(PSECURITY_DESCRIPTOR);
impl Drop for Local {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                LocalFree(self.0);
            }
        }
    }
}
struct User(Vec<u64>);
impl User {
    fn get() -> Result<Self> {
        unsafe {
            let mut token = null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let mut size = 0;
            GetTokenInformation(token, TokenUser, null_mut(), 0, &mut size);
            if size == 0 || size > 65536 {
                CloseHandle(token);
                return Err(denied());
            }
            // u64 storage gives TOKEN_USER and its embedded SID the required alignment.
            let mut buffer = vec![0u64; (size as usize + 7) / 8];
            let ok = GetTokenInformation(
                token,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                size,
                &mut size,
            );
            CloseHandle(token);
            if ok == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(Self(buffer))
        }
    }
    fn sid(&self) -> PSID {
        unsafe { (*(self.0.as_ptr().cast::<TOKEN_USER>())).User.Sid }
    }
    fn text(&self) -> Result<String> {
        unsafe {
            let mut output = null_mut();
            if ConvertSidToStringSidW(self.sid(), &mut output) == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let allocated = Local(output.cast());
            let mut len = 0;
            while *output.add(len) != 0 {
                len += 1;
            }
            let result =
                String::from_utf16(std::slice::from_raw_parts(output, len)).map_err(|_| denied());
            drop(allocated);
            result
        }
    }
}
fn descriptor(directory: bool) -> Result<Local> {
    let user = User::get()?.text()?;
    let inherit = if directory { "OICI" } else { "" };
    let sddl = format!("O:{user}D:P(A;{inherit};FA;;;{user})(A;{inherit};FA;;;SY)");
    let sddl: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
    let mut result = null_mut();
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &mut result,
            null_mut(),
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(Local(result))
}
pub fn create_dir(path: &Path) -> Result<()> {
    let security = descriptor(true)?;
    let attrs = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: security.0,
        bInheritHandle: 0,
    };
    if unsafe { CreateDirectoryW(wide(path)?.as_ptr(), &attrs) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
pub fn create_file(path: &Path) -> Result<File> {
    let security = descriptor(false)?;
    let attrs = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: security.0,
        bInheritHandle: 0,
    };
    let handle = unsafe {
        CreateFileW(
            wide(path)?.as_ptr(),
            GENERIC_WRITE,
            0,
            &attrs,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(unsafe { File::from_raw_handle(handle) })
}
pub fn replace(from: &Path, to: &Path) -> Result<()> {
    if unsafe {
        MoveFileExW(
            wide(from)?.as_ptr(),
            wide(to)?.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
pub fn check(path: &Path, dir: bool) -> Result<()> {
    let metadata = path.symlink_metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(denied());
    }
    let strict = !dir
        || path
            .file_name()
            .is_some_and(|name| name == "cloud-credentials");
    let user = User::get()?;
    let mut owner: PSID = null_mut();
    let mut acl: *mut ACL = null_mut();
    let mut sd = null_mut();
    let status = unsafe {
        GetNamedSecurityInfoW(
            wide(path)?.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            &mut acl,
            null_mut(),
            &mut sd,
        )
    };
    let security = Local(sd);
    if status != 0 {
        return Err(std::io::Error::from_raw_os_error(status as i32).into());
    }
    if owner.is_null() || acl.is_null() {
        return Err(denied());
    }
    unsafe {
        // A normal profile/ledger parent may include Administrators. Credential files
        // and their dedicated directory always require the actual user as owner.
        if EqualSid(owner, user.sid()) == 0
            && (strict || IsWellKnownSid(owner, WinBuiltinAdministratorsSid) == 0)
        {
            return Err(denied());
        }
        let mut control = 0;
        let mut revision = 0;
        if GetSecurityDescriptorControl(security.0, &mut control, &mut revision) == 0
            || strict && control & SE_DACL_PROTECTED == 0
        {
            return Err(denied());
        }
        let mut user_access = 0u32;
        for index in 0..(*acl).AceCount {
            let mut ace: *mut c_void = null_mut();
            if GetAce(acl, index as u32, &mut ace) == 0 || ace.is_null() {
                return Err(denied());
            }
            let header = &*ace.cast::<ACE_HEADER>();
            // INHERIT_ONLY_ACE grants no access to this object. A normal profile
            // parent often has a CREATOR OWNER rule applying only to descendants.
            // Our new files/directories receive their own protected DACL.
            if u32::from(header.AceFlags) & windows_sys::Win32::Security::INHERIT_ONLY_ACE != 0 {
                continue;
            }
            // ACCESS_DENIED_ACE_TYPE=1 does not grant access. Reject conditional/object
            // ACEs because they need a different SID layout and access evaluation.
            if header.AceType == 1 {
                continue;
            }
            if header.AceType != 0 {
                return Err(denied());
            }
            if (header.AceSize as usize) < std::mem::size_of::<ACCESS_ALLOWED_ACE>() {
                return Err(denied());
            }
            let allowed = &*ace.cast::<ACCESS_ALLOWED_ACE>();
            let sid = std::ptr::addr_of!(allowed.SidStart).cast_mut().cast();
            if EqualSid(sid, user.sid()) != 0 {
                user_access |= allowed.Mask;
                continue;
            }
            if IsWellKnownSid(sid, WinLocalSystemSid) != 0 {
                continue;
            }
            if !strict && IsWellKnownSid(sid, WinBuiltinAdministratorsSid) != 0 {
                continue;
            }
            // Other users may read an ordinary parent but may never change its entries.
            const MUTATING: u32 = 0x40000000 | 0x10000000 | 0x000d0156;
            if strict || allowed.Mask & MUTATING != 0 {
                return Err(denied());
            }
        }
        if strict && user_access & FILE_ALL_ACCESS != FILE_ALL_ACCESS {
            return Err(denied());
        }
    }
    Ok(())
}
pub fn available_bytes(path: &Path) -> Result<u64> {
    let path = path.parent().unwrap_or(path);
    let mut available = 0;
    if unsafe { GetDiskFreeSpaceExW(wide(path)?.as_ptr(), &mut available, null_mut(), null_mut()) }
        == 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(available)
}
