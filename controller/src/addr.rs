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

#[cfg(test)]
mod tests {
    use super::{normalize_private_network_name, shares_private_network};

    #[test]
    fn shares_private_network_requires_non_empty_matching_names() {
        assert!(shares_private_network(Some("cluster-a"), Some("cluster-a")));
        assert!(shares_private_network(
            Some("  cluster-a  "),
            Some("cluster-a")
        ));
        assert!(!shares_private_network(Some(""), Some("")));
        assert!(!shares_private_network(Some("cluster-a"), Some("cluster-b")));
        assert!(!shares_private_network(Some("cluster-a"), None));
    }

    #[test]
    fn normalize_private_network_name_drops_empty_values() {
        assert_eq!(
            normalize_private_network_name("  cluster-a  "),
            Some("cluster-a".to_string())
        );
        assert_eq!(normalize_private_network_name(""), None);
        assert_eq!(normalize_private_network_name("   "), None);
    }
}
