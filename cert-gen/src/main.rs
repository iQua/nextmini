use rcgen::generate_simple_self_signed;
use std::fs::File;
use std::io::Write;

fn main() {
    let certified_key = generate_simple_self_signed(vec!["Strato".into()]).unwrap();
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.key_pair.serialize_pem();
    File::create("server_cert.pem")
        .unwrap()
        .write_all(cert_pem.as_bytes())
        .unwrap();
    File::create("server_key.pem")
        .unwrap()
        .write_all(key_pem.as_bytes())
        .unwrap();
}