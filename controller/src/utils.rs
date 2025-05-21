/// Implements utility functions for the controller.
use std::collections::HashMap;

use strato_messages::{ControllerToDataplane, Flow, MultiPathMethod, Protocol, RouteInfo};

use crate::config::Config;
use crate::models::Route;

/// Creates a new virtual address by adding the node ID to the base address.
pub fn create_new_virtual_addr(
    base_ipv4_addr: [u8; 4],
    ipv4_net_mask: [u8; 4],
    node_id: usize,
) -> Option<[u8; 4]> {
    // makes a copy of the base address
    let base_ip = u32::from_be_bytes(base_ipv4_addr);
    let new_ip = base_ip.wrapping_add(node_id as u32);
    let new_virtual_addr = new_ip.to_be_bytes();
    let net_mask = u32::from_be_bytes(ipv4_net_mask);
    let network = base_ip & net_mask;
    let new_network = new_ip & net_mask;

    // checks if the new virtual address is outside the subnet
    if network != new_network {
        println!("No more nodes can be added to this subnet");
        None
    } else {
        Some(new_virtual_addr)
    }
}

/// Converts the flow ID to source and destination addresses. By Strato's rules, we can obtain the
/// node ID from the last byte of the source/destination address.
pub fn flow_id_2_src_dst_route_id(config: &Config, flow_id: Vec<i32>) -> (i32, i32, i32) {
    if flow_id.len() < 8 {
        panic!(
            "flow_id must have at least 8 elements, but it has {} elements",
            flow_id.len()
        );
    }

    let src_addr = flow_id[0..4].to_vec();
    let dst_addr = flow_id[4..8].to_vec();

    let src_id = src_addr[3] - config.base_ipv4_addr[3] as i32;
    let dst_id = dst_addr[3] - config.base_ipv4_addr[3] as i32;
    let route_id = flow_id[2];

    (src_id, dst_id, route_id)
}

pub fn build_startup_message(
    node_id: usize,
    virtual_addr: [u8; 4],
    net_mask: [u8; 4],
    session_id: [u8; 4],
    num_interfaces: usize,
    protocol: Protocol,
    multi_path_method: MultiPathMethod,
) -> ControllerToDataplane {
    ControllerToDataplane::StartUp {
        node_id,
        addr: virtual_addr,
        net_mask,
        session_id,
        num_interfaces,
        protocol,
        multi_path_method,
    }
}

pub fn build_add_node_message(
    protocol: Protocol,
    node_id: usize,
    addr: String,
) -> ControllerToDataplane {
    ControllerToDataplane::AddNode {
        protocol,
        node_id,
        addr,
    }
}

pub fn build_install_routes_message(
    config: &Config,
    routes: Vec<Route>,
    node_id: i32,
) -> Option<ControllerToDataplane> {
    let mut flows_map: HashMap<String, Flow> = HashMap::new();

    for route in routes {
        let src_node_addr = create_new_virtual_addr(
            config.base_ipv4_addr,
            config.ipv4_net_mask,
            route.src_node_id as usize,
        )?;

        let dst_node_addr = create_new_virtual_addr(
            config.base_ipv4_addr,
            config.ipv4_net_mask,
            route.dst_node_id as usize,
        )?;

        // the flow ID is constructed by concatenating the source and destination node addresses
        let flow_id = src_node_addr
            .iter()
            .chain(dst_node_addr.iter())
            .copied()
            .collect::<Vec<u8>>();
        let flow_id_str = flow_id
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(".");

        let idx = route.hops.iter().position(|&x| x == node_id);

        let next_hop = match idx {
            // the node is the destination of the flow
            Some(i) if i == route.hops.len() - 1 => route.hops[i] as usize,
            // the node is in the middle of the path
            Some(i) => route.hops[i + 1] as usize,
            // the flow is not part of this path; this node will be skipped
            None => continue,
        };

        let empty_str = String::from("[]");

        let streams_content = route.streams.as_ref().unwrap_or(&empty_str);

        let streams = match serde_json::from_str(streams_content) {
            Ok(streams) => {
                println!(
                    "Successfully deserialized streams: '{}' -> {:?}",
                    streams_content, streams
                );
                streams
            }
            Err(e) => {
                println!(
                    "Failed to deserialize streams: {}. Content: '{}', Route ID: {}, Src: {}, Dst: {}",
                    e, streams_content, route.route_id, route.src_node_id, route.dst_node_id
                );

                vec![]
            }
        };

        let route_info = RouteInfo {
            id: route.route_id as usize,
            next_hop,
            streams,
        };

        flows_map
            .entry(flow_id_str)
            .or_insert(Flow {
                flow_id: flow_id.clone(),
                routes: vec![],
            })
            .routes
            .push(route_info);
    }

    let flows: Vec<Flow> = flows_map.into_values().collect();
    if flows.is_empty() {
        None
    } else {
        Some(ControllerToDataplane::InstallFlow { flows })
    }
}

#[cfg(test)]
mod tests {
    use strato_messages::{ControllerToDataplane, Flow, MultiPathMethod, Protocol, RouteInfo};

    use super::*;
    use crate::config::Config;
    use crate::models::Route;

    #[test]
    fn test_create_new_virtual_addr() {
        let base_addr = [10, 0, 0, 0];
        let net_mask = [255, 255, 255, 0];
        let node_id = 5;
        let expected = Some([10, 0, 0, 5]);

        assert_eq!(
            create_new_virtual_addr(base_addr, net_mask, node_id),
            expected
        );
    }

    #[test]
    fn test_create_new_virtual_addr_invalid() {
        let base_addr = [10, 0, 0, 0];
        let net_mask = [255, 255, 255, 0];
        let node_id = 256;

        assert_eq!(create_new_virtual_addr(base_addr, net_mask, node_id), None);
    }

    #[test]
    fn test_build_startup_message() {
        let node_id = 5;
        let virtual_addr = [10, 0, 0, 5];
        let net_mask = [255, 255, 255, 0];
        let session_id = [1, 2, 3, 4];
        let num_interfaces = 1;
        let protocol = Protocol::Tcp;
        let multi_path_method = MultiPathMethod::Stream;

        let msg = build_startup_message(
            node_id,
            virtual_addr,
            net_mask,
            session_id,
            num_interfaces,
            protocol.clone(),
            multi_path_method.clone(),
        );

        let expected = ControllerToDataplane::StartUp {
            node_id,
            addr: virtual_addr,
            net_mask,
            session_id,
            num_interfaces,
            protocol,
            multi_path_method,
        };

        assert_eq!(msg, expected);
    }

    #[test]
    fn test_build_add_node_message() {
        let protocol = Protocol::Tcp;
        let node_id = 5;
        let addr = "172.10.0.2:8080".to_string();

        let msg = build_add_node_message(protocol.clone(), node_id, addr.clone());

        let expected = ControllerToDataplane::AddNode {
            protocol,
            node_id,
            addr,
        };

        assert_eq!(msg, expected);
    }

    #[test]
    fn test_build_install_routes_message() {
        let config = Config {
            base_ipv4_addr: [10, 0, 0, 0],
            ipv4_net_mask: [255, 255, 255, 0],
            ..Default::default()
        };

        let routes = vec![
            Route {
                src_node_id: 0,
                dst_node_id: 4,
                route_id: 0,
                hops: vec![0, 1, 2, 3, 4],
                streams: Some("[]".to_string()),
            },
            Route {
                src_node_id: 0,
                dst_node_id: 4,
                route_id: 1,
                hops: vec![0, 1, 2, 5, 4],
                streams: Some("[]".to_string()),
            },
        ];

        let node_id = 2;

        let msg = build_install_routes_message(&config, routes, node_id);

        let expected_flow_id = vec![10, 0, 0, 0, 10, 0, 0, 4];
        let expected_routes = vec![
            RouteInfo {
                id: 0,
                next_hop: 3,
                streams: vec![],
            },
            RouteInfo {
                id: 1,
                next_hop: 5,
                streams: vec![],
            },
        ];

        let expected = Some(ControllerToDataplane::InstallFlow {
            flows: vec![Flow {
                flow_id: expected_flow_id,
                routes: expected_routes,
            }],
        });

        assert_eq!(msg, expected);
    }
}
