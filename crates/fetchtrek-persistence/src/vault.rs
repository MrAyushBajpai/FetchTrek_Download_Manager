use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

use crate::db::Database;
use crate::error::PersistenceError;

pub trait CredentialVault: Send + Sync {
    fn store_secret(&self, key: &str, secret: &[u8]) -> Result<(), PersistenceError>;
    fn retrieve_secret(&self, key: &str) -> Result<Option<Vec<u8>>, PersistenceError>;
    fn delete_secret(&self, key: &str) -> Result<bool, PersistenceError>;
    fn is_hardware_or_os_backed(&self) -> bool;
}

#[derive(Debug, Default)]
pub struct InMemoryVault {
    store: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryVault {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(HashMap::new()),
        }
    }
}

impl CredentialVault for InMemoryVault {
    fn store_secret(&self, key: &str, secret: &[u8]) -> Result<(), PersistenceError> {
        let mut guard = self
            .store
            .write()
            .map_err(|e| PersistenceError::Vault(format!("InMemoryVault lock poisoned: {}", e)))?;
        guard.insert(key.to_string(), secret.to_vec());
        Ok(())
    }

    fn retrieve_secret(&self, key: &str) -> Result<Option<Vec<u8>>, PersistenceError> {
        let guard = self
            .store
            .read()
            .map_err(|e| PersistenceError::Vault(format!("InMemoryVault lock poisoned: {}", e)))?;
        Ok(guard.get(key).cloned())
    }

    fn delete_secret(&self, key: &str) -> Result<bool, PersistenceError> {
        let mut guard = self
            .store
            .write()
            .map_err(|e| PersistenceError::Vault(format!("InMemoryVault lock poisoned: {}", e)))?;
        Ok(guard.remove(key).is_some())
    }

    fn is_hardware_or_os_backed(&self) -> bool {
        false
    }
}

#[cfg(windows)]
extern "system" {
    fn LocalFree(hmem: *mut core::ffi::c_void) -> *mut core::ffi::c_void;
}

#[cfg(windows)]
pub fn dpapi_encrypt(plaintext: &[u8]) -> Result<Vec<u8>, PersistenceError> {
    use std::ptr::{null, null_mut};
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Security::Cryptography::{
        CryptProtectData, CRYPTPROTECT_UI_FORBIDDEN, CRYPT_INTEGER_BLOB,
    };

    let in_blob = CRYPT_INTEGER_BLOB {
        cbData: plaintext.len() as u32,
        pbData: plaintext.as_ptr() as *mut u8,
    };
    let mut out_blob = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: null_mut(),
    };

    let success = unsafe {
        CryptProtectData(
            &in_blob,
            null(),
            null(),
            null(),
            null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut out_blob,
        )
    };

    if success == 0 {
        let err = unsafe { GetLastError() };
        return Err(PersistenceError::Vault(format!(
            "CryptProtectData failed with Win32 error code: {}",
            err
        )));
    }

    let ciphertext =
        unsafe { std::slice::from_raw_parts(out_blob.pbData, out_blob.cbData as usize).to_vec() };

    unsafe {
        LocalFree(out_blob.pbData as _);
    }

    Ok(ciphertext)
}

#[cfg(windows)]
pub fn dpapi_decrypt(ciphertext: &[u8]) -> Result<Vec<u8>, PersistenceError> {
    use std::ptr::{null, null_mut};
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Security::Cryptography::{
        CryptUnprotectData, CRYPTPROTECT_UI_FORBIDDEN, CRYPT_INTEGER_BLOB,
    };

    let in_blob = CRYPT_INTEGER_BLOB {
        cbData: ciphertext.len() as u32,
        pbData: ciphertext.as_ptr() as *mut u8,
    };
    let mut out_blob = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: null_mut(),
    };

    let success = unsafe {
        CryptUnprotectData(
            &in_blob,
            null_mut(),
            null(),
            null(),
            null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut out_blob,
        )
    };

    if success == 0 {
        let err = unsafe { GetLastError() };
        return Err(PersistenceError::Vault(format!(
            "CryptUnprotectData failed with Win32 error code: {}",
            err
        )));
    }

    let plaintext =
        unsafe { std::slice::from_raw_parts(out_blob.pbData, out_blob.cbData as usize).to_vec() };

    unsafe {
        LocalFree(out_blob.pbData as _);
    }

    Ok(plaintext)
}

#[cfg(windows)]
pub struct DpapiVault {
    db: Mutex<Database>,
}

#[cfg(windows)]
impl DpapiVault {
    pub fn new(db: Database) -> Result<Self, PersistenceError> {
        db.connection().execute(
            "CREATE TABLE IF NOT EXISTS vault_store (
                key TEXT PRIMARY KEY,
                ciphertext BLOB NOT NULL
            );",
            [],
        )?;
        Ok(Self { db: Mutex::new(db) })
    }
}

#[cfg(windows)]
impl CredentialVault for DpapiVault {
    fn store_secret(&self, key: &str, secret: &[u8]) -> Result<(), PersistenceError> {
        let ciphertext = dpapi_encrypt(secret)?;
        let db = self
            .db
            .lock()
            .map_err(|e| PersistenceError::Vault(e.to_string()))?;
        db.connection().execute(
            "INSERT OR REPLACE INTO vault_store (key, ciphertext) VALUES (?1, ?2);",
            rusqlite::params![key, ciphertext],
        )?;
        Ok(())
    }

    fn retrieve_secret(&self, key: &str) -> Result<Option<Vec<u8>>, PersistenceError> {
        let db = self
            .db
            .lock()
            .map_err(|e| PersistenceError::Vault(e.to_string()))?;
        let mut stmt = db
            .connection()
            .prepare("SELECT ciphertext FROM vault_store WHERE key = ?1;")?;
        let mut rows = stmt.query([key])?;
        if let Some(row) = rows.next()? {
            let ciphertext: Vec<u8> = row.get(0)?;
            let plaintext = dpapi_decrypt(&ciphertext)?;
            Ok(Some(plaintext))
        } else {
            Ok(None)
        }
    }

    fn delete_secret(&self, key: &str) -> Result<bool, PersistenceError> {
        let db = self
            .db
            .lock()
            .map_err(|e| PersistenceError::Vault(e.to_string()))?;
        let rows = db
            .connection()
            .execute("DELETE FROM vault_store WHERE key = ?1;", [key])?;
        Ok(rows > 0)
    }

    fn is_hardware_or_os_backed(&self) -> bool {
        true
    }
}

pub fn default_vault(db: Option<Database>) -> Box<dyn CredentialVault> {
    #[cfg(windows)]
    {
        if let Some(database) = db {
            if let Ok(vault) = DpapiVault::new(database) {
                return Box::new(vault);
            }
        }
    }

    #[cfg(not(windows))]
    let _ = db;

    Box::new(InMemoryVault::new())
}
