"""Unit tests for tree_conversion.py

Tests the convert_to_multicast_trees() algorithm to ensure:
1. Highest throughput paths are processed first
2. Trees are constructed optimally
3. Path pruning works correctly
4. Edge cases are handled properly
"""

from tree_conversion import convert_to_multicast_trees, paths_to_edges


def test_basic_tree_conversion():
    """Test basic tree conversion with multiple paths."""
    # Simulate LP solution with 3 paths
    variables = {
        'x': 0,
        'p_1_2_4': 1,      # Path 1->2->4 with 10 Mbps
        'p_1_2_3_5': 2,    # Path 1->2->3->5 with 10 Mbps
        'p_1_2_5': 3,      # Path 1->2->5 with 5 Mbps
    }
    sol = [10.0, 10.0, 10.0, 5.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    
    # Assertions
    assert len(sources) == 1, f"Expected 1 source, got {len(sources)}"
    assert sources[0] == 1, f"Expected source=1, got {sources[0]}"
    
    trees = session_trees[0]
    assert len(trees) == 2, f"Expected 2 trees, got {len(trees)}"
    
    # First tree should have highest throughput (10 Mbps)
    tree1_paths, tree1_tput = trees[0]
    assert tree1_tput == 10.0, f"Expected first tree throughput=10.0, got {tree1_tput}"
    assert len(tree1_paths) == 2, f"Expected 2 paths in first tree, got {len(tree1_paths)}"
    
    # Second tree should have remaining throughput (5 Mbps)
    tree2_paths, tree2_tput = trees[1]
    assert tree2_tput == 5.0, f"Expected second tree throughput=5.0, got {tree2_tput}"
    assert len(tree2_paths) == 1, f"Expected 1 path in second tree, got {len(tree2_paths)}"
    
    print("✅ test_basic_tree_conversion passed")


def test_highest_throughput_first():
    """Test that highest throughput paths are processed first."""
    variables = {
        'x': 0,
        'p_1_2': 1,      # 5 Mbps
        'p_1_3': 2,      # 10 Mbps
        'p_1_4': 3,      # 15 Mbps
    }
    sol = [0.0, 5.0, 10.0, 15.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    trees = session_trees[0]
    
    # Trees should be ordered by throughput descending
    assert trees[0][1] == 15.0, f"First tree should have 15.0 Mbps, got {trees[0][1]}"
    assert trees[1][1] == 10.0, f"Second tree should have 10.0 Mbps, got {trees[1][1]}"
    assert trees[2][1] == 5.0, f"Third tree should have 5.0 Mbps, got {trees[2][1]}"
    
    print("✅ test_highest_throughput_first passed")


def test_path_merging():
    """Test that compatible paths are merged into same tree."""
    variables = {
        'x': 0,
        'p_1_2_3': 1,    # 10 Mbps to destination 3
        'p_1_2_4': 2,    # 10 Mbps to destination 4 (shares prefix 1->2)
    }
    sol = [0.0, 10.0, 10.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    trees = session_trees[0]
    
    # Should create 1 tree with both paths (they share prefix and have same throughput)
    assert len(trees) == 1, f"Expected 1 merged tree, got {len(trees)}"
    tree_paths, tree_tput = trees[0]
    assert len(tree_paths) == 2, f"Expected 2 paths in tree, got {len(tree_paths)}"
    assert tree_tput == 10.0, f"Expected throughput=10.0, got {tree_tput}"
    
    print("✅ test_path_merging passed")


def test_incompatible_paths_not_merged():
    """Test that paths with different parents for same node are not merged."""
    variables = {
        'x': 0,
        'p_1_2_4': 1,    # 10 Mbps: 1->2->4
        'p_1_3_4': 2,    # 10 Mbps: 1->3->4 (node 4 has different parent)
    }
    sol = [0.0, 10.0, 10.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    trees = session_trees[0]
    
    # Should create 2 separate trees (incompatible due to single-parent rule)
    assert len(trees) == 2, f"Expected 2 separate trees, got {len(trees)}"
    assert trees[0][1] == 10.0, f"First tree throughput should be 10.0, got {trees[0][1]}"
    assert trees[1][1] == 10.0, f"Second tree throughput should be 10.0, got {trees[1][1]}"
    
    print("✅ test_incompatible_paths_not_merged passed")


def test_duplicate_destination_not_merged():
    """Test that paths to same destination are not merged into one tree."""
    variables = {
        'x': 0,
        'p_1_2_3': 1,    # 10 Mbps to destination 3
        'p_1_4_3': 2,    # 10 Mbps to destination 3 (different path, same dest)
    }
    sol = [0.0, 10.0, 10.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    trees = session_trees[0]
    
    # Should create 2 separate trees (can't have duplicate destinations)
    assert len(trees) == 2, f"Expected 2 trees, got {len(trees)}"
    
    print("✅ test_duplicate_destination_not_merged passed")


def test_throughput_pruning():
    """Test that path throughput is correctly pruned after tree creation."""
    variables = {
        'x': 0,
        'p_1_2_3': 1,    # 15 Mbps
        'p_1_2_4': 2,    # 10 Mbps (shares prefix 1->2)
    }
    sol = [0.0, 15.0, 10.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    trees = session_trees[0]
    
    # First tree: both paths at 10 Mbps (limited by lower throughput)
    # Second tree: remaining 5 Mbps from p_1_2_3
    assert len(trees) == 2, f"Expected 2 trees after pruning, got {len(trees)}"
    assert trees[0][1] == 15.0, f"First tree should be 15.0 Mbps, got {trees[0][1]}"
    assert trees[1][1] == 10.0, f"Second tree should be 10.0 Mbps, got {trees[1][1]}"
    
    print("✅ test_throughput_pruning passed")


def test_zero_throughput_filtered():
    """Test that paths with zero throughput are filtered out."""
    variables = {
        'x': 0,
        'p_1_2_3': 1,    # 10 Mbps
        'p_1_2_4': 2,    # 0 Mbps (should be filtered)
        'p_1_2_5': 3,    # 5 Mbps
    }
    sol = [0.0, 10.0, 0.0, 5.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    trees = session_trees[0]
    
    # Should only have 2 trees (zero throughput path filtered)
    assert len(trees) == 2, f"Expected 2 trees (zero filtered), got {len(trees)}"
    
    # Verify no zero-throughput trees
    for tree_paths, tree_tput in trees:
        assert tree_tput > 0, f"Found tree with zero throughput: {tree_tput}"
    
    print("✅ test_zero_throughput_filtered passed")


def test_multiple_sources():
    """Test tree conversion with multiple sources."""
    variables = {
        'x': 0,
        'p_1_2_3': 1,    # Source 1 -> 3
        'p_1_2_4': 2,    # Source 1 -> 4
        'p_5_6_7': 3,    # Source 5 -> 7
    }
    sol = [0.0, 10.0, 10.0, 8.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    
    # Should have 2 sources
    assert len(sources) == 2, f"Expected 2 sources, got {len(sources)}"
    assert sources == [1, 5], f"Expected sources [1, 5], got {sources}"
    
    # Each source should have its own trees
    assert len(session_trees) == 2, f"Expected 2 session_trees, got {len(session_trees)}"
    assert len(session_trees[0]) >= 1, "Source 1 should have at least 1 tree"
    assert len(session_trees[1]) >= 1, "Source 5 should have at least 1 tree"
    
    print("✅ test_multiple_sources passed")


def test_paths_to_edges():
    """Test conversion of paths to unique edges."""
    paths = [
        [1, 2, 3],
        [1, 2, 4],
        [1, 5, 6],
    ]
    
    edges = paths_to_edges(paths)
    
    # Expected edges: (1,2), (2,3), (2,4), (1,5), (5,6)
    expected = [(1, 2), (1, 5), (2, 3), (2, 4), (5, 6)]
    assert edges == expected, f"Expected {expected}, got {edges}"
    
    # Test deduplication
    paths_with_dup = [
        [1, 2, 3],
        [1, 2, 4],  # Edge (1,2) appears twice
    ]
    edges = paths_to_edges(paths_with_dup)
    expected = [(1, 2), (2, 3), (2, 4)]
    assert edges == expected, f"Expected deduplicated {expected}, got {edges}"
    
    print("✅ test_paths_to_edges passed")


def test_single_path():
    """Test edge case with single path."""
    variables = {
        'x': 0,
        'p_1_2_3': 1,
    }
    sol = [0.0, 10.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    trees = session_trees[0]
    
    assert len(trees) == 1, f"Expected 1 tree, got {len(trees)}"
    assert trees[0][1] == 10.0, f"Expected throughput=10.0, got {trees[0][1]}"
    assert len(trees[0][0]) == 1, f"Expected 1 path, got {len(trees[0][0])}"
    
    print("✅ test_single_path passed")


def test_empty_solution():
    """Test edge case with no paths (all zero throughput)."""
    variables = {
        'x': 0,
        'p_1_2_3': 1,
        'p_1_2_4': 2,
    }
    sol = [0.0, 0.0, 0.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    
    # Should have no sources (all paths filtered)
    assert len(sources) == 0, f"Expected 0 sources, got {len(sources)}"
    assert len(session_trees) == 0, f"Expected 0 session_trees, got {len(session_trees)}"
    
    print("✅ test_empty_solution passed")


def test_regression_pop_order():
    """Regression test: ensure we pop from front (highest throughput first).
    
    This is the specific bug that was fixed: current.pop() -> current.pop(0)
    """
    variables = {
        'x': 0,
        'p_1_2_4': 1,      # 10 Mbps
        'p_1_2_3_5': 2,    # 10 Mbps
        'p_1_2_5': 3,      # 5 Mbps
    }
    sol = [10.0, 10.0, 10.0, 5.0]
    
    sources, session_trees = convert_to_multicast_trees(variables, sol)
    trees = session_trees[0]
    
    # CRITICAL: First tree must have 10.0 Mbps (not 5.0)
    # If this fails, the pop order bug has regressed
    assert trees[0][1] == 10.0, \
        f"REGRESSION: First tree has {trees[0][1]} Mbps instead of 10.0. " \
        f"Bug may have been reintroduced (check pop order)."
    
    # Should create 2 trees (not 3)
    assert len(trees) == 2, \
        f"REGRESSION: Created {len(trees)} trees instead of 2. " \
        f"Suboptimal tree construction detected."
    
    # First tree should merge the two 10 Mbps paths
    assert len(trees[0][0]) == 2, \
        f"REGRESSION: First tree has {len(trees[0][0])} paths instead of 2. " \
        f"Path merging not working correctly."
    
    print("✅ test_regression_pop_order passed")


def run_all_tests():
    """Run all test functions."""
    print("=" * 70)
    print("Running tree_conversion.py tests...")
    print("=" * 70)
    
    test_functions = [
        test_basic_tree_conversion,
        test_highest_throughput_first,
        test_path_merging,
        test_incompatible_paths_not_merged,
        test_duplicate_destination_not_merged,
        test_throughput_pruning,
        test_zero_throughput_filtered,
        test_multiple_sources,
        test_paths_to_edges,
        test_single_path,
        test_empty_solution,
        test_regression_pop_order,
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            print(f"❌ {test_func.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"❌ {test_func.__name__} ERROR: {e}")
            failed += 1
    
    print("=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70)
    
    if failed == 0:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"⚠️  {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(run_all_tests())

