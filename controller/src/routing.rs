// Determines routes by implementing the routing protocols.
use petgraph::algo::astar;
use petgraph::graph::{DiGraph, NodeIndex};

/// Defines the interface for all routing protocols.
pub trait RoutingProtocol {
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex>;
}

#[derive(Debug, Clone)]
pub struct ShortestPath {
    graph: DiGraph<u32, ()>,
}

impl ShortestPath {
    pub fn new(graph: DiGraph<u32, ()>) -> ShortestPath {
        ShortestPath { graph }
    }
}

impl RoutingProtocol for ShortestPath {
    /// Returns a shortest path between two nodes in the graph using A*.
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        let path = astar(
            &self.graph,
            start,
            |n| n == end,
            |_| 1, // Uniform cost
            |_| 0, // Heuristic ignored for uniform cost
        );

        match path {
            Some((_, path)) => path,
            None => {
                tracing::warn!(
                    "No path found from node {:?} to node {:?} - topology may be incomplete",
                    start, end
                );
                Vec::new()
            }
        }
    }
}
