//! TLS helper: generates a self-signed certificate and key.
//!
//! This binary writes `server_cert.pem` and `server_key.pem` into the current working directory.
//! It is used as a lightweight helper for local/dev setups that need PEM artifacts.

use rcgen::generate_simple_self_signed;
use std::fs::File;
use std::io::Write;

fn main() {
    let certified_key = generate_simple_self_signed(vec!["Nextmini".into()]).unwrap();
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    File::create("server_cert.pem")
        .unwrap()
        .write_all(cert_pem.as_bytes())
        .unwrap();
    File::create("server_key.pem")
        .unwrap()
        .write_all(key_pem.as_bytes())
        .unwrap();
}
