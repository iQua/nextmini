use std::collections::HashSet;

use controller::utils::{build_group_routes_for_node_multitree, compute_multitree_route_id};
use nextmini_messages::{ControllerToDataplane, GroupRouteTree, MULTITREE_STRIDE};

#[test]
fn multitree_install_payloads_are_stable_and_complete() {
    let group_id = 37usize;
    let src_node_id = 1u32;

    // Intentionally unsorted input to verify canonical tree ordering.
    let trees = vec![
        GroupRouteTree {
            tree_id: 5,
            weight: Some(0.2),
            edges: vec![(1, 2), (2, 5)],
        },
        GroupRouteTree {
            tree_id: 1,
            weight: Some(0.6),
            edges: vec![(1, 2), (2, 4)],
        },
        GroupRouteTree {
            tree_id: 3,
            weight: Some(0.2),
            edges: vec![(1, 3), (3, 4)],
        },
    ];

    let members = HashSet::from([4u32]);

    // Validate complete per-tree payload for a member node.
    let routes_a =
        build_group_routes_for_node_multitree(group_id, src_node_id, &trees, 4, &members).unwrap();
    let routes_b =
        build_group_routes_for_node_multitree(group_id, src_node_id, &trees, 4, &members).unwrap();

    assert_eq!(routes_a, routes_b, "payload must be stable across rebuilds");
    assert_eq!(
        routes_a.len(),
        3,
        "every configured tree must be represented"
    );

    let expected_route_ids = vec![
        compute_multitree_route_id(group_id, 1).unwrap(),
        compute_multitree_route_id(group_id, 3).unwrap(),
        compute_multitree_route_id(group_id, 5).unwrap(),
    ];
    let actual_route_ids: Vec<usize> = routes_a.iter().map(|r| r.route_id).collect();
    assert_eq!(actual_route_ids, expected_route_ids);

    for route in &routes_a {
        assert_eq!(route.group_id, group_id);
        assert_eq!(route.src_node_id, src_node_id as usize);
        assert_eq!(
            route.next_hops,
            vec![4],
            "member must retain local delivery"
        );
    }

    let message = ControllerToDataplane::InstallGroupRoutes {
        group_id,
        src_node_id: src_node_id as usize,
        routes: routes_a,
    };
    let encoded_a = rmp_serde::to_vec(&message).unwrap();
    let encoded_b = rmp_serde::to_vec(&message).unwrap();
    assert_eq!(
        encoded_a, encoded_b,
        "serialized payload must be deterministic"
    );
}

#[test]
fn single_tree_payload_maps_to_tree_zero_route_id() {
    let group_id = 5usize;
    let members = HashSet::new();
    let trees = [GroupRouteTree {
        tree_id: 0,
        weight: None,
        edges: vec![(1, 2), (2, 3)],
    }];
    let routes = build_group_routes_for_node_multitree(group_id, 1, &trees, 1, &members).unwrap();
    let route = routes.first().unwrap();

    assert_eq!(
        route.route_id,
        compute_multitree_route_id(group_id, 0).unwrap()
    );
    assert_eq!(route.route_id / MULTITREE_STRIDE, group_id);
}
