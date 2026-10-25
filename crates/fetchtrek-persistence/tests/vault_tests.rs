#[cfg(windows)]
use std::path::PathBuf;
#[cfg(windows)]
use std::time::{SystemTime, UNIX_EPOCH};

use fetchtrek_persistence::{default_vault, CredentialVault, InMemoryVault};

#[cfg(windows)]
use fetchtrek_persistence::PersistenceError;

#[cfg(windows)]
use fetchtrek_persistence::{dpapi_decrypt, dpapi_encrypt, Database, DpapiVault};

#[cfg(windows)]
struct TempDbGuard {
    path: PathBuf,
}

#[cfg(windows)]
impl TempDbGuard {
    fn new(name: &str) -> Self {
        let mut path = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique = format!(
            "fetchtrek_vault_test_{}_{}_{}.sqlite3",
            std::process::id(),
            timestamp,
            name
        );
        path.push(unique);
        Self { path }
    }
}

#[cfg(windows)]
impl Drop for TempDbGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
    }
}

#[test]
fn test_in_memory_vault_lifecycle() {
    let vault = InMemoryVault::new();
    assert!(!vault.is_hardware_or_os_backed());

    // Initially absent
    assert_eq!(vault.retrieve_secret("nonexistent").unwrap(), None);
    assert!(!vault.delete_secret("nonexistent").unwrap());

    // Store secret
    let secret = b"my-super-secret-bearer-token-123";
    vault.store_secret("auth_token", secret).unwrap();

    // Retrieve and assert bit-for-bit parity
    let retrieved = vault.retrieve_secret("auth_token").unwrap();
    assert_eq!(retrieved, Some(secret.to_vec()));

    // Overwrite existing secret
    let updated_secret = b"my-newly-rotated-bearer-token-456";
    vault.store_secret("auth_token", updated_secret).unwrap();
    let retrieved_updated = vault.retrieve_secret("auth_token").unwrap();
    assert_eq!(retrieved_updated, Some(updated_secret.to_vec()));

    // Delete secret
    assert!(vault.delete_secret("auth_token").unwrap());
    assert!(!vault.delete_secret("auth_token").unwrap());

    // Assert retrieval returns None after deletion
    assert_eq!(vault.retrieve_secret("auth_token").unwrap(), None);

    // Factory test with None database returns InMemoryVault
    let def_vault = default_vault(None);
    def_vault.store_secret("k", b"v").unwrap();
    assert_eq!(def_vault.retrieve_secret("k").unwrap(), Some(b"v".to_vec()));
}

#[cfg(windows)]
#[test]
fn test_windows_dpapi_round_trip() {
    let plaintext = b"Basic dXNlcm5hbWU6c3VwZXJfc2VjcmV0X3Bhc3N3b3JkXzEyMzQ=";

    // Encrypt with DPAPI CryptProtectData
    let ciphertext = dpapi_encrypt(plaintext).expect("dpapi_encrypt failed");

    // Assert ciphertext does not contain plaintext bytes
    assert_ne!(ciphertext, plaintext);
    assert!(
        !ciphertext.windows(plaintext.len()).any(|w| w == plaintext),
        "DPAPI ciphertext must not contain plaintext credentials"
    );

    // Decrypt with CryptUnprotectData and verify exact bit-for-bit parity
    let decrypted = dpapi_decrypt(&ciphertext).expect("dpapi_decrypt failed");
    assert_eq!(decrypted, plaintext);

    // Tampered ciphertext must fail decryption with PersistenceError::Vault
    let mut tampered = ciphertext.clone();
    if !tampered.is_empty() {
        tampered[0] ^= 0xFF;
        let last = tampered.len() - 1;
        tampered[last] ^= 0xFF;
    }
    let tampered_err = dpapi_decrypt(&tampered).unwrap_err();
    assert!(matches!(tampered_err, PersistenceError::Vault(_)));

    // Truncated ciphertext must also fail
    let truncated_err = dpapi_decrypt(&[1, 2, 3]).unwrap_err();
    assert!(matches!(truncated_err, PersistenceError::Vault(_)));
}

#[cfg(windows)]
#[test]
fn test_zero_plaintext_in_database_integration() {
    let temp = TempDbGuard::new("zero_plaintext");
    let db = Database::open(&temp.path).expect("open database failed");

    let vault = DpapiVault::new(db).expect("create DpapiVault failed");
    assert!(vault.is_hardware_or_os_backed());

    let cred_key = "cf_clearance_session";
    let cred_secret = b"SUPER_SECRET_CLOUDFLARE_CLEARANCE_SESSION_TOKEN_987654321_ALICE_BOB";

    // Store credential in database-backed vault
    vault.store_secret(cred_key, cred_secret).unwrap();

    // Verify retrieval works accurately
    let retrieved = vault.retrieve_secret(cred_key).unwrap().unwrap();
    assert_eq!(retrieved, cred_secret);

    // Read the raw bytes directly from the SQLite database file and WAL file
    let db_bytes = std::fs::read(&temp.path).expect("read db file failed");
    let wal_path = format!("{}-wal", temp.path.display());
    let wal_bytes = std::fs::read(&wal_path).unwrap_or_default();

    // Raw binary string scan across SQLite disk pages: verify plaintext NEVER appears
    assert!(
        !db_bytes.windows(cred_secret.len()).any(|w| w == cred_secret),
        "CRITICAL SECURITY INVARIANT VIOLATION: Plaintext credential found in SQLite database file!"
    );

    assert!(
        !wal_bytes
            .windows(cred_secret.len())
            .any(|w| w == cred_secret),
        "CRITICAL SECURITY INVARIANT VIOLATION: Plaintext credential found in SQLite WAL journal file!"
    );

    // Delete secret and verify absence
    assert!(vault.delete_secret(cred_key).unwrap());
    assert_eq!(vault.retrieve_secret(cred_key).unwrap(), None);
}
