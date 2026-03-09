use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub(crate) fn write_artifact_with_hash(path: &Path, bytes: &[u8]) -> Result<String, String> {
    std::fs::write(path, bytes)
        .map_err(|err| format!("failed to write artifact {}: {err}", path.display()))?;

    let digest = sha256_hex(bytes);
    std::fs::write(hash_path(path), format!("{digest}\n"))
        .map_err(|err| format!("failed to write hash sidecar {}: {err}", path.display()))?;

    Ok(digest)
}

fn hash_path(path: &Path) -> PathBuf {
    let suffix = match path.file_name().and_then(|name| name.to_str()) {
        Some(name) => format!("{name}.sha256"),
        None => "artifact.sha256".to_string(),
    };
    path.with_file_name(suffix)
}
