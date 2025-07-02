use anyhow::{anyhow, Context as _};
use aya_build::cargo_metadata;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let cargo_metadata::Metadata {
        packages,
        workspace_root,
        ..
    } = cargo_metadata::MetadataCommand::new()
        .no_deps()
        .exec()
        .context("MetadataCommand::exec")?;

    // Look for the local aya-ebpf package specifically
    let ebpf_package = packages
        .into_iter()
        .find(|pkg| {
            pkg.name == "aya-ebpf-local"
                && pkg.source.is_none()
                && pkg.manifest_path.starts_with(&workspace_root)
        })
        .ok_or_else(|| anyhow!("local aya-ebpf-local package not found"))?;

    println!(
        "cargo:rerun-if-changed={}/src",
        ebpf_package.manifest_path.parent().unwrap()
    );

    aya_build::build_ebpf([ebpf_package])
}
