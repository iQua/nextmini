"""Tests for CF-Tree algorithm."""

import sys
from pathlib import Path

# Add parent to path for direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.lp.graph import Graph
from examples.lp.cf_tree import (
    cf_tree_direct,
    cf_tree_from_lp,
    compute_basic_weights,
    compute_cf_weights,
    compute_edge_importance,
    extract_lp_solution,
    build_cf_tree,
    compute_tree_rate,
    LPSolution,
)
from examples.lp.solver import compute_tree_edges, build_graph_from_controller_config


def test_basic_tree_simple():
    """Test basic tree construction on a simple topology."""
    # Simple 4-node topology: 1 -> 2 -> 3, 1 -> 4
    nodes = [1, 2, 3, 4]
    edges = [(1, 2), (2, 1), (2, 3), (3, 2), (1, 4), (4, 1), (2, 4), (4, 2)]
    capacities = {e: 100.0 for e in edges}

    graph = Graph(nodes, edges, capacities)

    result = cf_tree_direct(graph, src=1, terminals=[2, 3, 4], hop_limit=3)

    print(f"Basic Tree Test:")
    print(f"  Edges: {result.edges}")
    print(f"  Tree rate: {result.tree_rate}")
    print(f"  Nodes: {result.nodes_in_tree}")

    # Verify all terminals are reachable
    assert 2 in result.nodes_in_tree
    assert 3 in result.nodes_in_tree
    assert 4 in result.nodes_in_tree
    assert len(result.edges) >= 3  # At least 3 edges to reach 3 terminals

    print("  PASSED\n")


def test_hop_limit():
    """Test that hop limit is enforced."""
    # Linear chain: 1 -> 2 -> 3 -> 4 -> 5
    nodes = [1, 2, 3, 4, 5]
    edges = []
    for i in range(1, 5):
        edges.extend([(i, i + 1), (i + 1, i)])
    capacities = {e: 100.0 for e in edges}

    graph = Graph(nodes, edges, capacities)

    # With hop_limit=2, node 4 and 5 should be unreachable from node 1
    try:
        result = cf_tree_direct(graph, src=1, terminals=[2, 3, 4], hop_limit=2)
        # Node 3 is at hop 2, should be reachable
        # Node 4 is at hop 3, should fail
        assert False, "Should have raised ValueError for unreachable terminal"
    except ValueError as e:
        assert "unreachable" in str(e).lower()
        print(f"Hop Limit Test:")
        print(f"  Correctly raised error: {e}")
        print("  PASSED\n")


def test_cf_weights():
    """Test CF weight computation."""
    nodes = [1, 2, 3]
    edges = [(1, 2), (2, 1), (2, 3), (3, 2), (1, 3), (3, 1)]
    capacities = {e: 100.0 for e in edges}

    graph = Graph(nodes, edges, capacities)

    # Simulate LP solution with edge (1,2) having high importance
    lp_sol = LPSolution(
        f_star=50.0,
        edge_flows={(1, 2): 50.0, (2, 3): 50.0, (1, 3): 10.0},
        path_flows={},
    )

    importance = compute_edge_importance(lp_sol)
    print(f"CF Weights Test:")
    print(f"  Edge importance: {importance}")

    # Edge (1,2) and (2,3) should have importance ~1.0
    assert importance[(1, 2)] > 0.9
    assert importance[(2, 3)] > 0.9
    # Edge (1,3) should have lower importance
    assert importance[(1, 3)] < 0.5

    weights = compute_cf_weights(graph, importance, eta=0.1)
    print(f"  CF weights: {weights}")

    # Edge (1,2) should have lower weight (more preferred)
    assert weights[(1, 2)] < weights[(1, 3)]

    print("  PASSED\n")


def test_tree_rate():
    """Test tree rate computation."""
    nodes = [1, 2, 3]
    edges = [(1, 2), (2, 3)]
    capacities = {(1, 2): 100.0, (2, 1): 100.0, (2, 3): 50.0, (3, 2): 50.0}

    graph = Graph(nodes, edges, capacities)

    tree_edges = [(1, 2), (2, 3)]
    tree_nodes = {1: 0, 2: 1, 3: 2}

    rate = compute_tree_rate(graph, tree_edges, tree_nodes)

    print(f"Tree Rate Test:")
    print(f"  Computed rate: {rate}")

    # Rate should be min(80.0, 100.0, 50.0) = 50.0
    assert rate == 50.0

    print("  PASSED\n")


def test_lp_node_budget_affects_f_star():
    """Ensure node egress budgets are enforced in the LP backend."""
    nodes = [1, 2, 3]
    edges = [(1, 2), (2, 1), (1, 3), (3, 1)]
    capacities = {e: 100.0 for e in edges}
    graph = Graph(nodes, edges, capacities)

    # With two terminals and an upload budget of 50, the LP's common rate is <= 25.
    result = compute_tree_edges(
        graph,
        src=1,
        destinations=[2, 3],
        algorithm="cf_tree",
        hop_limit=1,
        node_egress_budgets={1: 50.0},
    )

    assert result.lp_f_star is not None
    assert abs(result.lp_f_star - 25.0) <= 1e-3
    assert result.throughput is not None
    assert abs(result.throughput - 25.0) <= 1e-3

    print("LP Node Budget Test:")
    print(f"  LP f_star: {result.lp_f_star}")
    print(f"  Tree throughput: {result.throughput}")
    print("  PASSED\n")


def test_unified_interface():
    """Test the unified compute_tree_edges interface."""
    nodes = [1, 2, 3, 4]
    # Ensure the source has direct reachability to all terminals when terminals are not allowed to forward.
    edges = [
        (1, 2),
        (2, 1),
        (2, 3),
        (3, 2),
        (2, 4),
        (4, 2),
        (1, 3),
        (3, 1),
        (1, 4),
        (4, 1),
    ]
    capacities = {e: 100.0 for e in edges}

    graph = Graph(nodes, edges, capacities)

    print("Unified Interface Test:")

    # Test basic_tree
    result = compute_tree_edges(
        graph, src=1, destinations=[2, 3, 4], algorithm="basic_tree", hop_limit=3
    )
    print(f"  basic_tree: {result.edges}, throughput={result.throughput}")
    assert len(result.edges) >= 3
    assert result.algorithm == "basic_tree"

    # Test cf_tree_mwu (solver-free conceptual-flow approximation)
    result = compute_tree_edges(
        graph, src=1, destinations=[2, 3, 4], algorithm="cf_tree_mwu", hop_limit=3
    )
    print(f"  cf_tree_mwu: {result.edges}, throughput={result.throughput}")
    assert len(result.edges) >= 3
    assert result.algorithm == "cf_tree_mwu"
    assert result.lp_f_star is not None

    # Test cf_bottleneck_mwu
    result = compute_tree_edges(
        graph,
        src=1,
        destinations=[2, 3, 4],
        algorithm="cf_bottleneck_mwu",
        hop_limit=3,
    )
    print(
        f"  cf_bottleneck_mwu: {result.edges}, throughput={result.throughput}"
    )
    assert len(result.edges) >= 3
    assert result.algorithm == "cf_bottleneck_mwu"
    assert result.lp_f_star is not None

    # Test cf_tree (needs cvxopt)
    try:
        result = compute_tree_edges(
            graph, src=1, destinations=[2, 3, 4], algorithm="cf_tree", hop_limit=3
        )
        print(f"  cf_tree: {result.edges}, throughput={result.throughput}")
        print(f"    LP f_star: {result.lp_f_star}")
        assert len(result.edges) >= 3
        assert result.algorithm == "cf_tree"
    except ImportError as e:
        print(f"  cf_tree: SKIPPED (cvxopt not available)")

    # Test cf_bottleneck (needs cvxopt)
    try:
        result = compute_tree_edges(
            graph,
            src=1,
            destinations=[2, 3, 4],
            algorithm="cf_bottleneck",
            hop_limit=3,
        )
        print(f"  cf_bottleneck: {result.edges}, throughput={result.throughput}")
        print(f"    LP f_star: {result.lp_f_star}")
        assert len(result.edges) >= 3
        assert result.algorithm == "cf_bottleneck"
    except ImportError:
        print(f"  cf_bottleneck: SKIPPED (cvxopt not available)")

    # Test mflow
    try:
        result = compute_tree_edges(
            graph, src=1, destinations=[2, 3, 4], algorithm="mflow"
        )
        print(f"  mflow: {result.edges}, throughput={result.throughput}")
        assert result.algorithm == "mflow"
    except ImportError as e:
        print(f"  mflow: SKIPPED (cvxopt not available)")

    print("  PASSED\n")


def test_with_controller_config():
    """Test with actual controller config if available."""
    # Try to find a controller config
    config_paths = [
        Path(__file__).resolve().parents[2] / "examples/rl/configs-docker/controller-config.toml",
        Path(__file__).resolve().parents[2] / "examples/multicast-docker/controller-config.toml",
    ]

    config_path = None
    for p in config_paths:
        if p.exists():
            config_path = p
            break

    if config_path is None:
        print("Controller Config Test: SKIPPED (no config found)\n")
        return

    print(f"Controller Config Test ({config_path.name}):")

    graph = build_graph_from_controller_config(str(config_path))
    print(f"  Nodes: {graph.nodes}")
    print(f"  Edges: {len(graph.edges)}")

    if len(graph.nodes) >= 3:
        src = graph.nodes[0]
        dests = graph.nodes[1:3]

        result = compute_tree_edges(
            graph, src=src, destinations=dests, algorithm="basic_tree", hop_limit=3
        )
        print(f"  basic_tree from {src} to {dests}: {result.edges}")

        try:
            result = compute_tree_edges(
                graph, src=src, destinations=dests, algorithm="cf_tree", hop_limit=3
            )
            print(f"  cf_tree from {src} to {dests}: {result.edges}")
            print(f"    LP f_star: {result.lp_f_star}")
        except ImportError:
            print(f"  cf_tree: SKIPPED (cvxopt not available)")

    print("  PASSED\n")


if __name__ == "__main__":
    print("=" * 60)
    print("CF-Tree Algorithm Tests")
    print("=" * 60 + "\n")

    test_basic_tree_simple()
    test_hop_limit()
    test_cf_weights()
    test_tree_rate()
    test_lp_node_budget_affects_f_star()
    test_unified_interface()
    test_with_controller_config()

    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)
