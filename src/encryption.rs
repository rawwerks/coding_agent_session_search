use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use hkdf::Hkdf;
use sha2::Sha256;

pub use argon2::Params as Argon2Params;

const AES_GCM_KEY_LEN: usize = 32;
const AES_GCM_NONCE_LEN: usize = 12;
const AES_GCM_TAG_LEN: usize = 16;

fn validate_length(label: &str, actual: usize, expected: usize) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{} length invalid: expected {} bytes, got {}",
            label, expected, actual
        ))
    }
}

pub fn aes_gcm_encrypt(
    key: &[u8],
    nonce: &[u8],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), String> {
    validate_length("AES-GCM key", key.len(), AES_GCM_KEY_LEN)?;
    validate_length("AES-GCM nonce", nonce.len(), AES_GCM_NONCE_LEN)?;

    let key = Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(nonce);
    let payload = Payload {
        msg: plaintext,
        aad,
    };

    // aes-gcm returns ciphertext + tag appended.
    let ciphertext_with_tag = cipher
        .encrypt(nonce, payload)
        .map_err(|e| format!("encryption failure: {}", e))?;

    if ciphertext_with_tag.len() < AES_GCM_TAG_LEN {
        return Err("encryption failure: ciphertext too short".to_string());
    }

    // Tag is 16 bytes for AES-256-GCM
    let split_idx = ciphertext_with_tag.len() - AES_GCM_TAG_LEN;
    let (cipher, tag) = ciphertext_with_tag.split_at(split_idx);

    Ok((cipher.to_vec(), tag.to_vec()))
}

pub fn aes_gcm_decrypt(
    key: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
    aad: &[u8],
    tag: &[u8],
) -> Result<Vec<u8>, String> {
    validate_length("AES-GCM key", key.len(), AES_GCM_KEY_LEN)?;
    validate_length("AES-GCM nonce", nonce.len(), AES_GCM_NONCE_LEN)?;
    validate_length("AES-GCM tag", tag.len(), AES_GCM_TAG_LEN)?;

    let key = Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(nonce);

    // Combine ciphertext and tag for decryption (aes-gcm crate expects them together)
    let mut payload_vec = Vec::with_capacity(ciphertext.len() + tag.len());
    payload_vec.extend_from_slice(ciphertext);
    payload_vec.extend_from_slice(tag);

    let payload = Payload {
        msg: &payload_vec,
        aad,
    };

    cipher
        .decrypt(nonce, payload)
        .map_err(|e| format!("decryption failed: {}", e))
}

pub fn argon2id_hash(
    password: &[u8],
    salt: &[u8],
    params: &Argon2Params,
) -> Result<Vec<u8>, String> {
    let argon2 = argon2::Argon2::new(
        argon2::Algorithm::Argon2id,
        argon2::Version::V0x13,
        params.clone(),
    );

    let mut output = vec![0u8; params.output_len().unwrap_or(32)];
    argon2
        .hash_password_into(password, salt, &mut output)
        .map_err(|e| format!("argon2 hashing failed: {}", e))?;
    Ok(output)
}

pub fn hkdf_expand(ikm: &[u8], salt: &[u8], info: &[u8], len: usize) -> Result<Vec<u8>, String> {
    let hk = Hkdf::<Sha256>::new(Some(salt), ikm);
    let mut okm = vec![0u8; len];
    hk.expand(info, &mut okm)
        .map_err(|e| format!("hkdf expand failed: {}", e))?;
    Ok(okm)
}

pub fn hkdf_extract(salt: &[u8], ikm: &[u8]) -> Vec<u8> {
    let (prk, _) = Hkdf::<Sha256>::extract(Some(salt), ikm);
    prk.to_vec()
}
