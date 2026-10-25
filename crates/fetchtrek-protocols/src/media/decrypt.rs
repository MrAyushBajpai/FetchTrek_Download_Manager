use aes::Aes128;
use cbc::cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use fetchtrek_types::MediaError;

type Aes128CbcDec = cbc::Decryptor<Aes128>;
type Aes128CbcEnc = cbc::Encryptor<Aes128>;

/// Decrypts AES-128-CBC encrypted data with PKCS#7 unpadding according to RFC 8216 §5.2.
pub fn decrypt_aes128_cbc(
    encrypted_data: &[u8],
    key: &[u8; 16],
    iv: &[u8; 16],
) -> Result<Vec<u8>, MediaError> {
    if encrypted_data.is_empty() || !encrypted_data.len().is_multiple_of(16) {
        return Err(MediaError::MalformedManifest);
    }

    let decryptor = Aes128CbcDec::new(key.into(), iv.into());
    let mut buf = encrypted_data.to_vec();
    let pt = decryptor
        .decrypt_padded_mut::<Pkcs7>(&mut buf)
        .map_err(|_| MediaError::MalformedManifest)?;

    Ok(pt.to_vec())
}

/// Helper for encrypting data with AES-128-CBC and PKCS#7 padding (useful for testing and stream synthesis).
pub fn encrypt_aes128_cbc(plaintext: &[u8], key: &[u8; 16], iv: &[u8; 16]) -> Vec<u8> {
    let encryptor = Aes128CbcEnc::new(key.into(), iv.into());
    let mut buf = vec![0u8; plaintext.len() + 16];
    let ct = encryptor
        .encrypt_padded_b2b_mut::<Pkcs7>(plaintext, &mut buf)
        .expect("buffer sized to hold padded ciphertext");
    ct.to_vec()
}
