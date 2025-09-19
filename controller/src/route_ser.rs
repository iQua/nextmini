use serde::Deserialize;
/// Route serialization and deserialization utilities
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
        EdgePairs(Vec<Vec<u32>>),
        NodeSequence(Vec<u32>),
    }

    // deserializes the route from the input format
    let format = RouteRepr::deserialize(deserializer)?;

    match format {
        //  for route = [[1, 2], [2, 3], [3, 4]] in controller config
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
        // for route = [1, 2, 3, 4] in controller config
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
