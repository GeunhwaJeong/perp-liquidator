// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The liquidator's signing key.
//!
//! Read from a file, never from the command line, where it would show in the process list and
//! the shell history. The file may be the Haneul CLI's `haneul.keystore` (a JSON list of base64
//! `flag || key` entries, from which the one for `--address` is taken), a single `haneulprivkey`
//! string, or a single base64 entry. Only Ed25519 keys are supported.

use std::path::Path;

use anyhow::{Context, bail};
use haneul_crypto::ed25519::Ed25519PrivateKey;
use haneul_sdk_types::Address;
use tracing::warn;

pub struct Key {
    pub private: Ed25519PrivateKey,
    pub address: Address,
}

impl Key {
    fn new(private: Ed25519PrivateKey) -> Self {
        let address = private.public_key().derive_address();
        Self { private, address }
    }
}

pub fn load(path: &Path, address: Option<Address>) -> anyhow::Result<Key> {
    warn_if_readable_by_others(path);
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read the key file {}", path.display()))?;
    parse(&text, address).with_context(|| format!("No usable key in {}", path.display()))
}

pub fn parse(text: &str, address: Option<Address>) -> anyhow::Result<Key> {
    let text = text.trim();
    let keys: Vec<Key> = if text.starts_with('[') {
        let entries: Vec<String> =
            serde_json::from_str(text).context("A keystore is a JSON list of strings")?;
        // Entries of other schemes are skipped: the keystore may hold them for other uses.
        entries
            .iter()
            .filter_map(|entry| Ed25519PrivateKey::from_base64(entry).ok())
            .map(Key::new)
            .collect()
    } else if text.starts_with("haneulprivkey") {
        let key = Ed25519PrivateKey::from_haneulprivkey(text)
            .map_err(|e| anyhow::anyhow!("Not an Ed25519 haneulprivkey: {e}"))?;
        vec![Key::new(key)]
    } else {
        let key = Ed25519PrivateKey::from_base64(text)
            .map_err(|e| anyhow::anyhow!("Not a base64 Ed25519 key: {e}"))?;
        vec![Key::new(key)]
    };

    match address {
        Some(address) => keys
            .into_iter()
            .find(|key| key.address == address)
            .with_context(|| format!("No Ed25519 key for {address}")),
        None => {
            if keys.len() > 1 {
                bail!(
                    "The file holds {} Ed25519 keys; choose one with --address",
                    keys.len()
                );
            }
            keys.into_iter()
                .next()
                .context("The file holds no Ed25519 key")
        }
    }
}

#[cfg(unix)]
fn warn_if_readable_by_others(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = std::fs::metadata(path)
        && metadata.permissions().mode() & 0o077 != 0
    {
        warn!(
            path = %path.display(),
            "The key file can be read by other users of this machine; chmod 600 it"
        );
    }
}

#[cfg(not(unix))]
fn warn_if_readable_by_others(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> Ed25519PrivateKey {
        Ed25519PrivateKey::new([byte; 32])
    }

    #[test]
    fn picks_the_keystore_entry_for_the_address() {
        let (a, b) = (key(1), key(2));
        let keystore = serde_json::to_string(&[a.to_base64(), b.to_base64()]).unwrap();
        let wanted = b.public_key().derive_address();
        let found = parse(&keystore, Some(wanted)).unwrap();
        assert_eq!(found.address, wanted);

        let err = parse(&keystore, None).err().unwrap().to_string();
        assert!(err.contains("2 Ed25519 keys"), "{err}");
        let missing = key(3).public_key().derive_address();
        assert!(parse(&keystore, Some(missing)).is_err());
    }

    #[test]
    fn reads_a_single_key_in_either_encoding() {
        let k = key(7);
        let address = k.public_key().derive_address();
        assert_eq!(parse(&k.to_base64(), None).unwrap().address, address);
        let bech32 = k.to_haneulprivkey().unwrap();
        assert_eq!(
            parse(&format!("{bech32}\n"), None).unwrap().address,
            address
        );
        assert!(parse("not a key", None).is_err());
    }
}
