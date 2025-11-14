#[cfg(target_os = "macos")]
fn main() {
    // Allow unresolved Python symbols to be satisfied at load time by the interpreter.
    println!("cargo:rustc-cdylib-link-arg=-undefined");
    println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
}

#[cfg(not(target_os = "macos"))]
fn main() {}
