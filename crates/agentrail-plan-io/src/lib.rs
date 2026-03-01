use std::fs;
use std::io::Write;
use std::path::Path;

use agentrail_core::Plan;
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

pub fn load_plan(path: &Path) -> Result<(Plan, String)> {
    let raw = fs::read(path).with_context(|| format!("read plan: {}", path.display()))?;
    let hash = hash_bytes(&raw);
    let plan: Plan =
        serde_yaml::from_slice(&raw).with_context(|| format!("parse yaml: {}", path.display()))?;
    Ok((plan, hash))
}

pub fn save_plan(plan: &Plan, path: &Path, expected_hash: Option<&str>) -> Result<String> {
    if let Some(expected) = expected_hash {
        let current = fs::read(path).with_context(|| format!("read plan: {}", path.display()))?;
        let current_hash = hash_bytes(&current);
        if current_hash != expected {
            bail!("CAS conflict: plan changed since load");
        }
    }

    let yaml = serde_yaml::to_string(plan).context("serialize plan to yaml")?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create parent dir: {}", parent.display()))?;

    let mut tmp = NamedTempFile::new_in(parent).context("create temp file")?;
    tmp.write_all(yaml.as_bytes()).context("write temp file")?;
    tmp.flush().context("flush temp file")?;
    tmp.persist(path)
        .map_err(|e| e.error)
        .with_context(|| format!("persist temp file: {}", path.display()))?;

    Ok(hash_bytes(yaml.as_bytes()))
}

fn hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}
