//! Platform-owned privacy enforcement for log artifact paths.
//!
//! An artifact path is prepared before content reaches it. Platform failures are
//! intentionally collapsed to `PrivacyNotGuaranteed`: callers must fail closed
//! rather than deciding that a particular ACL failure is safe to ignore.

use crate::error::LogStoreError;
use std::fs;
use std::path::Path;

/// Platform-specific privacy preparation for artifact paths.
///
/// Production capture uses [`PlatformArtifactPrivacy`]. The trait is public so
/// host-runtime integration tests can inject an enforcement failure and prove
/// that artifact capture alone fails open without taking metadata storage
/// down with it.
#[doc(hidden)]
pub trait ArtifactPrivacy: Send + Sync {
    fn prepare_directory(&self, path: &Path) -> Result<(), LogStoreError>;
    fn prepare_file(&self, path: &Path) -> Result<(), LogStoreError>;
}

#[derive(Debug, Default)]
pub struct PlatformArtifactPrivacy;

impl ArtifactPrivacy for PlatformArtifactPrivacy {
    fn prepare_directory(&self, path: &Path) -> Result<(), LogStoreError> {
        reject_symlink(path)?;
        platform::prepare_directory(path)
    }

    fn prepare_file(&self, path: &Path) -> Result<(), LogStoreError> {
        reject_symlink(path)?;
        platform::prepare_file(path)
    }
}

fn reject_symlink(path: &Path) -> Result<(), LogStoreError> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(LogStoreError::PathUnsafe {
            // Paths can carry usernames and application-state locations. This
            // error is surfaced by the fail-open health path, so keep its
            // reason stable and deliberately path-free.
            segment: "symlink_not_allowed".to_string(),
        });
    }
    Ok(())
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    pub(super) fn prepare_directory(path: &Path) -> Result<(), LogStoreError> {
        set_mode(path, 0o700)
    }

    pub(super) fn prepare_file(path: &Path) -> Result<(), LogStoreError> {
        set_mode(path, 0o600)
    }

    fn set_mode(path: &Path, mode: u32) -> Result<(), LogStoreError> {
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions)?;
        Ok(())
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::ffi::c_void;
    use std::mem::{align_of, size_of};
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::{null, null_mut};
    use windows_sys::Win32::Foundation::{CloseHandle, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        GetNamedSecurityInfoW, SE_FILE_OBJECT, SetNamedSecurityInfoW,
    };
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, AddAccessAllowedAceEx, CONTAINER_INHERIT_ACE,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetLengthSid, GetSecurityDescriptorControl,
        GetTokenInformation, InitializeAcl, OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
        TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    pub(super) fn prepare_directory(path: &Path) -> Result<(), LogStoreError> {
        apply_and_verify(path, true)
    }

    pub(super) fn prepare_file(path: &Path) -> Result<(), LogStoreError> {
        apply_and_verify(path, false)
    }

    #[cfg(test)]
    pub(super) fn verify_current_user_only(
        path: &Path,
        is_directory: bool,
    ) -> Result<(), LogStoreError> {
        let expected_ace_flags = if is_directory {
            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
        } else {
            0
        };
        with_current_user_sid(|sid| verify_user_only_dacl(path, sid, expected_ace_flags))
    }

    fn apply_and_verify(path: &Path, is_directory: bool) -> Result<(), LogStoreError> {
        with_current_user_sid(|sid| {
            let acl_bytes = size_of::<ACL>() + size_of::<ACCESS_ALLOWED_ACE>() - size_of::<u32>()
                + unsafe { GetLengthSid(sid) as usize };
            let words = acl_bytes.div_ceil(size_of::<u64>());
            let mut acl_storage = vec![0_u64; words];
            let acl = acl_storage.as_mut_ptr().cast::<ACL>();
            let ace_flags = if is_directory {
                OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
            } else {
                0
            };

            unsafe {
                if InitializeAcl(acl, acl_bytes as u32, ACL_REVISION) == 0
                    || AddAccessAllowedAceEx(acl, ACL_REVISION, ace_flags, FILE_ALL_ACCESS, sid)
                        == 0
                {
                    return Err(LogStoreError::PrivacyNotGuaranteed);
                }
            }

            let path_wide = to_wide(path);
            let result = unsafe {
                SetNamedSecurityInfoW(
                    path_wide.as_ptr(),
                    SE_FILE_OBJECT,
                    OWNER_SECURITY_INFORMATION
                        | DACL_SECURITY_INFORMATION
                        | PROTECTED_DACL_SECURITY_INFORMATION,
                    sid,
                    null_mut(),
                    acl,
                    null(),
                )
            };
            if result != 0 {
                return Err(LogStoreError::PrivacyNotGuaranteed);
            }

            verify_user_only_dacl(path, sid, ace_flags)
        })
    }

    fn with_current_user_sid<T>(
        f: impl FnOnce(PSID) -> Result<T, LogStoreError>,
    ) -> Result<T, LogStoreError> {
        let mut token = null_mut();
        unsafe {
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return Err(LogStoreError::PrivacyNotGuaranteed);
            }
        }
        let _token = TokenHandle(token);

        let mut bytes = 0_u32;
        unsafe {
            let _ = GetTokenInformation(token, TokenUser, null_mut(), 0, &mut bytes);
        }
        if bytes == 0 {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }

        let words = (bytes as usize).div_ceil(align_of::<usize>());
        let mut buffer = vec![0_usize; words];
        let ok = unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buffer.as_mut_ptr().cast::<c_void>(),
                bytes,
                &mut bytes,
            )
        };
        if ok == 0 {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }

        let token_user = buffer.as_ptr().cast::<TOKEN_USER>();
        let sid = unsafe { (*token_user).User.Sid };
        if sid.is_null() {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }
        f(sid)
    }

    fn verify_user_only_dacl(
        path: &Path,
        current_user: PSID,
        expected_ace_flags: u32,
    ) -> Result<(), LogStoreError> {
        let path_wide = to_wide(path);
        let mut owner = null_mut();
        let mut dacl = null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
        let result = unsafe {
            GetNamedSecurityInfoW(
                path_wide.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut descriptor,
            )
        };
        if result != 0 || owner.is_null() || dacl.is_null() || descriptor.is_null() {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }
        let _descriptor = SecurityDescriptor(descriptor);

        if unsafe { EqualSid(owner, current_user) } == 0 {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }

        let mut control = 0_u16;
        let mut revision = 0_u32;
        let control_ok =
            unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) != 0 };
        if !control_ok || control & SE_DACL_PROTECTED == 0 {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }

        if unsafe { (*dacl).AceCount } != 1 {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }

        let mut ace = null_mut();
        if unsafe { GetAce(dacl, 0, &mut ace) } == 0 || ace.is_null() {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }
        let allowed = ace.cast::<ACCESS_ALLOWED_ACE>();
        let is_current_user = unsafe {
            (*allowed).Header.AceType == 0
                && (*allowed).Header.AceFlags as u32 == expected_ace_flags
                && EqualSid(
                    (&(*allowed).SidStart as *const u32)
                        .cast_mut()
                        .cast::<c_void>(),
                    current_user,
                ) != 0
        };

        if !is_current_user || unsafe { (*allowed).Mask } != FILE_ALL_ACCESS {
            return Err(LogStoreError::PrivacyNotGuaranteed);
        }
        Ok(())
    }

    fn to_wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    struct TokenHandle(*mut c_void);

    impl Drop for TokenHandle {
        fn drop(&mut self) {
            unsafe {
                let _ = CloseHandle(self.0);
            }
        }
    }

    struct SecurityDescriptor(PSECURITY_DESCRIPTOR);

    impl Drop for SecurityDescriptor {
        fn drop(&mut self) {
            unsafe {
                let _ = LocalFree(self.0);
            }
        }
    }
}

#[cfg(all(test, windows))]
pub(crate) fn verify_current_user_only_storage_path(
    path: &Path,
    is_directory: bool,
) -> Result<(), LogStoreError> {
    platform::verify_current_user_only(path, is_directory)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn symlink_rejection_uses_a_path_free_reason() {
        let root = tempfile::tempdir().expect("temporary root");
        let target = root.path().join("target");
        let link = root.path().join("sensitive-local-path");
        fs::write(&target, b"target").expect("create target");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink");

        let error = reject_symlink(&link).expect_err("symlink must be rejected");
        assert!(matches!(
            error,
            LogStoreError::PathUnsafe { ref segment } if segment == "symlink_not_allowed"
        ));
        assert!(!error.to_string().contains(&link.display().to_string()));
    }
}

#[cfg(not(any(unix, windows)))]
mod platform {
    use super::*;

    pub(super) fn prepare_directory(_path: &Path) -> Result<(), LogStoreError> {
        Err(LogStoreError::PrivacyNotGuaranteed)
    }

    pub(super) fn prepare_file(_path: &Path) -> Result<(), LogStoreError> {
        Err(LogStoreError::PrivacyNotGuaranteed)
    }
}
