pub fn normalize_private_network_name(name: &str) -> Option<String> {
    let name = name.trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_owned())
    }
}

pub fn shares_private_network(a: Option<&str>, b: Option<&str>) -> bool {
    matches!(
        (a.map(str::trim), b.map(str::trim)),
        (Some(a), Some(b)) if !a.is_empty() && a == b
    )
}
