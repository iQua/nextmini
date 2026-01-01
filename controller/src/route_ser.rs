/// Utilities for serializing and deseriazing routes, which can be directed acyclic graphs.
use serde::Deserialize;
use serde::de::Deserializer;
use serde::de::Error;

/// Deserializes route edges from various input formats
pub fn deserialize_route_edges<'de, D>(deserializer: D) -> Result<Vec<(u32, u32)>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RouteRepr {
        NodeSequence(Vec<u32>),
        EdgePairs(Vec<Vec<u32>>),
    }

    // deserializes the route from two alternative input formats
    let format = RouteRepr::deserialize(deserializer)?;

    match format {
        //  for format `route = [[1, 2], [2, 3], [3, 4]]` in the controller configuration
        RouteRepr::EdgePairs(edge_pairs) => {
            let mut edges = Vec::with_capacity(edge_pairs.len());
            for pair in edge_pairs {
                if pair.len() != 2 {
                    return Err(Error::custom("Each edge must have exactly two nodes."));
                }
                edges.push((pair[0], pair[1]));
            }
            Ok(edges)
        }
        // for format `route = [1, 2, 3, 4]` in the controller configuration
        RouteRepr::NodeSequence(nodes) => {
            if nodes.len() < 2 {
                return Err(Error::custom("Route must have at least two nodes."));
            }
            let mut edges = Vec::with_capacity(nodes.len().saturating_sub(1));
            for window in nodes.windows(2) {
                edges.push((window[0], window[1]));
            }
            Ok(edges)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestRouteEdgePairs {
        #[serde(deserialize_with = "deserialize_route_edges")]
        edges: Vec<(u32, u32)>,
    }

    #[derive(Debug, Deserialize)]
    struct TestRouteNodeSeq {
        #[serde(deserialize_with = "deserialize_route_edges")]
        edges: Vec<(u32, u32)>,
    }

    #[test]
    fn test_deserialize_edge_pairs_format() {
        // Test format: [[1, 2], [2, 3], [3, 4]]
        let toml_str = r#"edges = [[1, 2], [2, 3], [3, 4]]"#;
        let result: TestRouteEdgePairs = toml::from_str(toml_str).unwrap();

        assert_eq!(result.edges.len(), 3);
        assert_eq!(result.edges[0], (1, 2));
        assert_eq!(result.edges[1], (2, 3));
        assert_eq!(result.edges[2], (3, 4));
    }

    #[test]
    fn test_deserialize_node_sequence_format() {
        // Test format: [1, 2, 3, 4]
        let toml_str = r#"edges = [1, 2, 3, 4]"#;
        let result: TestRouteNodeSeq = toml::from_str(toml_str).unwrap();

        assert_eq!(result.edges.len(), 3);
        assert_eq!(result.edges[0], (1, 2));
        assert_eq!(result.edges[1], (2, 3));
        assert_eq!(result.edges[2], (3, 4));
    }

    #[test]
    fn test_deserialize_single_edge() {
        // Single edge route
        let toml_str = r#"edges = [[1, 2]]"#;
        let result: TestRouteEdgePairs = toml::from_str(toml_str).unwrap();

        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0], (1, 2));
    }

    #[test]
    fn test_deserialize_single_edge_node_seq() {
        // Single edge as node sequence
        let toml_str = r#"edges = [1, 2]"#;
        let result: TestRouteNodeSeq = toml::from_str(toml_str).unwrap();

        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0], (1, 2));
    }

    #[test]
    fn test_deserialize_invalid_edge_too_few_nodes() {
        // Edge with only one node should fail
        let toml_str = r#"edges = [[1]]"#;
        let result: Result<TestRouteEdgePairs, _> = toml::from_str(toml_str);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exactly two nodes"));
    }

    #[test]
    fn test_deserialize_invalid_edge_too_many_nodes() {
        // Edge with three nodes should fail
        let toml_str = r#"edges = [[1, 2, 3]]"#;
        let result: Result<TestRouteEdgePairs, _> = toml::from_str(toml_str);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exactly two nodes"));
    }

    #[test]
    fn test_deserialize_invalid_node_seq_too_short() {
        // Node sequence with only one node should fail
        let toml_str = r#"edges = [1]"#;
        let result: Result<TestRouteNodeSeq, _> = toml::from_str(toml_str);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("at least two nodes"));
    }

    #[test]
    fn test_deserialize_empty_route_rejected() {
        let toml_str = r#"edges = []"#;
        let result: Result<TestRouteNodeSeq, _> = toml::from_str(toml_str);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("at least two nodes"));
    }

    #[test]
    fn test_deserialize_invalid_mixed_element_types() {
        let toml_str = r#"edges = [[1, 2], 3]"#;
        let result: Result<TestRouteEdgePairs, _> = toml::from_str(toml_str);

        assert!(result.is_err());
    }
}
