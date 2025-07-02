use libbpf_cargo::SkeletonBuilder;
use std::env;
use std::path::PathBuf;

fn main() {
    let src = "ebpf/redirect.bpf.c";
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    // Compile eBPF program and generate Rust skeleton
    SkeletonBuilder::new()
        .source(src)
        .build_and_generate(&out_dir.join("redirect.skel.rs"))
        .unwrap();

    println!("cargo:rerun-if-changed={}", src);
}
