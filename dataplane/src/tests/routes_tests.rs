use std::sync::Arc;

use crate::dataplane::node_interface::NodeManager;
use crate::dataplane::routes;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_route_new() {
    let _ = routes::Route::new(10, 1);
}

#[tokio::test]
async fn test_route_from_json() {
    let object = json::object! {
        weight: 10,
        next_hop: 1,
    };
    let _ = routes::Route::from_json(&object);
}

#[tokio::test]
async fn test_route_to_json() {
    let object = json::object! {
        weight: 10,
        next_hop: 1,
    };
    let route = routes::Route::from_json(&object);
    let object1 = route.to_json();
    assert_eq!(object, object1);
}

#[tokio::test]
async fn test_flow_from_json() {
    let flow = json::object! {
        flow_id: [0, 0, 0, 0, 0, 0, 0, 42],
        weight: 10,
        routes: [
            {
                weight: 10,
                next_hop: 1,
            },
            {
                weight: 10,
                next_hop: 2,
            },
        ],
    };
    let _ = routes::Flow::from_json(&flow);
}

#[tokio::test]
async fn test_flow_to_json() {
    let flow = json::object! {
        flow_id: [0, 0, 0, 0, 0, 0, 0, 42],
        weight: 10,
        routes: [
            {
                weight: 10,
                next_hop: 1,
            },
            {
                weight: 10,
                next_hop: 2,
            },
        ],
    };

    let expected_flow = json::object! {
        flow_id: 42,
        weight: 10,
        routes: [
            {
                weight: 10,
                next_hop: 1,
            },
            {
                weight: 10,
                next_hop: 2,
            },
        ],
    };

    let flow1 = routes::Flow::from_json(&flow);
    let flow2 = flow1.to_json();
    assert_eq!(expected_flow, flow2);
}

#[tokio::test]
async fn test_routing_table_new() {
    let _ = routes::RoutingTable::new();
}

#[tokio::test]
async fn test_routing_table_add_flow() {
    let mut routing_table = routes::RoutingTable::new();
    let flow = json::object! {
        flow_id: [0, 0, 0, 0, 0, 0, 0, 42],
        weight: 10,
        routes: [
            {
                weight: 10,
                next_hop: 1,
            },
            {
                weight: 10,
                next_hop: 2,
            },
        ],
    };
    let flow1 = routes::Flow::from_json(&flow);
    routing_table.add_flow(flow1);
}

// #[tokio::test]
// async fn test_get_flow(){
//     let mut routing_table = routes::RoutingTable::new();
//     let flow = json::object!{
//         flow_id: [0, 0, 0, 0, 0, 0, 0, 42],
//         weight: 10,
//         routes: [
//             {
//                 weight: 10,
//                 next_hop: 1,
//             },
//             {
//                 weight: 10,
//                 next_hop: 2,
//             },
//         ],
//     };
//     let flow1 = routes::Flow::from_json(&flow);
//     routing_table.add_flow(flow1);

//     let _ = routing_table.get_flow(&42).expect("Should be able to get the Flow");
// }

#[tokio::test]
async fn test_get_flow_mut() {
    let mut routing_table = routes::RoutingTable::new();
    let flow = json::object! {
        flow_id: [0, 0, 0, 0, 0, 0, 0, 42],
        weight: 10,
        routes: [
            {
                weight: 10,
                next_hop: 1,
            },
            {
                weight: 10,
                next_hop: 2,
            },
        ],
    };

    let flow1 = routes::Flow::from_json(&flow);
    routing_table.add_flow(flow1);

    let _ = routing_table
        .get_flow(&42)
        .expect("Should be able to get the Flow");
}

#[tokio::test]
async fn test_schedule_next_hop() {
    let mut routing_table = routes::RoutingTable::new();
    let flow = json::object! {
        flow_id: [0, 0, 0, 0, 0, 0, 0, 42],
        weight: 10,
        routes: [
            {
                weight: 10,
                next_hop: 1,
            },
            {
                weight: 10,
                next_hop: 2,
            },
        ],
    };
    let flow1 = routes::Flow::from_json(&flow);
    routing_table.add_flow(flow1);

    let _ = routing_table.schedule_next_hop(&42, 0);
}

#[tokio::test]
async fn test_scheduler_new() {
    let _ = routes::StrideScheduler::new();
}

#[tokio::test]
async fn test_scheduler_from_routes() {
    let route1 = routes::Route::new(1, 0);
    let route2 = routes::Route::new(2, 1);

    let routes = vec![route1, route2];
    let _ = routes::StrideScheduler::from_routes(&routes);
}

#[tokio::test]
async fn test_scheduler_schedule_next_hop() {
    let route1 = routes::Route::new(2, 0);
    let route2 = routes::Route::new(1, 1);

    let routes = vec![route1.clone(), route2.clone()];
    let mut scheduler = routes::StrideScheduler::from_routes(&routes);

    // Since we set route1 and route2 to have weights 2 and 1 respectively, we expect
    // The scheduler to return route1, route2, route1, route1 in that order
    let hop1 = scheduler.schedule_next_hop(10);
    let hop2 = scheduler.schedule_next_hop(10);
    let hop3 = scheduler.schedule_next_hop(10);
    let hop4 = scheduler.schedule_next_hop(10);

    assert_eq!(hop1, route1.get_next_hop());
    assert_eq!(hop2, route2.get_next_hop());
    assert_eq!(hop3, route1.get_next_hop());
    assert_eq!(hop4, route1.get_next_hop());
}

#[tokio::test]
async fn test_scheduler_handle_pass_overflow() {
    let large_weight: usize = 2 ^ 30 + 1; //2^30 is the max value to trigger pass reset
    let route1 = routes::Route::new(large_weight, 0);
    let route2 = routes::Route::new(large_weight, 1);

    let routes = vec![route1.clone(), route2.clone()];
    let mut scheduler = routes::StrideScheduler::from_routes(&routes);

    //We want to test to see if pass value is reset. Our large weight should trigger pass overflow multiple times. This shouldn't generate an error.
    for _ in 0..10 {
        let _ = scheduler.schedule_next_hop(10);
    }
}

#[tokio::test]
async fn test_router_new() {
    let table = Arc::new(RwLock::new(routes::RoutingTable::new()));
    let _ = routes::Router::new(table);
}

#[tokio::test]
async fn test_router_route() {
    let flow = json::object! {
        flow_id: [0, 0, 0, 0, 0, 0, 0, 42],
        weight: 10,
        routes: [
            {
                weight: 2,
                next_hop: 0,
            },
            {
                weight: 1,
                next_hop: 1,
            },
        ],
    };
    let mut table = routes::RoutingTable::new();
    table.add_flow(routes::Flow::from_json(&flow));
    let rw_table = Arc::new(RwLock::new(table));
    let router = routes::Router::new(rw_table);

    let hop1 = router.next_hop(&42, 10).await;
    let hop2 = router.next_hop(&42, 10).await;
    let hop3 = router.next_hop(&42, 10).await;

    assert_eq!(hop1.unwrap(), 0);
    assert_eq!(hop2.unwrap(), 1);
    assert_eq!(hop3.unwrap(), 0);
}
