fn main() {
    // Let tests and binaries find libpython without extra environment variables.
    pyo3_build_config::add_libpython_rpath_link_args();

    #[cfg(target_os = "macos")]
    {
        // Manual cargo builds on macOS still need dynamic lookup for the extension module.
        pyo3_build_config::add_extension_module_link_args();
        pyo3_build_config::add_python_framework_link_args();
    }
}
