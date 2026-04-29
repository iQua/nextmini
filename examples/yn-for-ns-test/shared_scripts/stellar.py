"""
Implementation of the stellar algorithm.
"""

import os
import re
import json
import time
from typing import List

import numpy as np


from priority import optimize_completion_time
from allocation import optimize_flow_rates, optimize_lp_flow_rates
from generic import (
    BaseFlow,
    BaseLink,
    BaseContainer,
    FlowLinkHolder,
    FlowLinkSendHolder,
    CollectiveGroupContainer,
    FlowCGHolder,
)


def create_fl_containers(info_data: dict):
    """Creating the holder for the flow and link."""
    flows = [
        BaseFlow(
            id=key,
            data_volume=value["total"] // 8 // 1024 // 1024,
            src=value["src"],
            dst=value["dst"],
            links=value["links"],
            dependent_flow_ids=value["dependencies"],
            collective_id=value["collective_id"],
            group_id=value["group_id"],
        )
        for key, value in info_data.items()
        if bool(re.fullmatch(r"\d+", key))
    ]
    for flow in flows:
        flow.update_dependent_flow_ids()

    flow_container = BaseContainer(
        item_objs=flows,
        item_ids=[flow.get_unique_id() for flow in flows],
    )
    flow_container.sort_items()
    flow_container.check_validity()

    # Create the link holder
    net_links = [
        BaseLink(
            id=int(link_id),
            capacity=info_data["link_capacities"][link_id] // 8 // 1024 // 1024,
        )
        for link_id in info_data["link_capacities"]
    ]
    link_container = BaseContainer(
        item_objs=net_links,
        item_ids=[base_link.id for base_link in net_links],
    )
    link_container.sort_items()
    link_container.check_validity()
    return flow_container, link_container


def create_fl_holder(
    flow_container: BaseContainer, link_container: BaseContainer
) -> FlowLinkHolder:
    """Creating the holder for the flow and link."""

    f2l_mapper = {flow.get_unique_id(): flow.links for flow in flow_container.item_objs}

    fl_holder = FlowLinkHolder(
        flow_ids=flow_container.item_ids,
        link_ids=link_container.item_ids,
        f2l_mapper=f2l_mapper,
    )

    fl_holder.create_mapper2matrix()
    fl_holder.get_numbers()
    return fl_holder


def create_fl_send_holder(
    fl_holder: FlowLinkHolder, f_container: BaseContainer, l_container: BaseContainer
) -> FlowLinkSendHolder:
    """Creating the holder for the send of flows in links"""
    fl_sender = FlowLinkSendHolder(fl_holder=fl_holder)

    matrix1 = np.zeros_like(fl_holder.matrix)
    matrix2 = np.zeros_like(fl_holder.matrix)
    for i in range(fl_holder.F):
        for j in range(fl_holder.E):
            if fl_holder.matrix[i, j] == 1:
                matrix1[i, j] = l_container.item_objs[j].capacity
                matrix2[i, j] = f_container.item_objs[i].data_volume
    fl_sender.capacity_matrix = matrix1
    fl_sender.data_matrix = matrix2

    return fl_sender


def create_cg_holder(flow_container: BaseContainer):
    """Create the holder for the collective and group."""
    collective_ids = np.array([flow.collective_id for flow in flow_container.item_objs])
    unique_coll_ids = np.unique(collective_ids)
    group_ids = np.array([flow.group_id for flow in flow_container.item_objs])

    coll_group_ids = [
        np.unique(group_ids[collective_ids == coll_id]) for coll_id in unique_coll_ids
    ]

    cg_holder = CollectiveGroupContainer(
        collective_ids=unique_coll_ids,
        group_ids=coll_group_ids,
    )
    cg_holder.sort_collective_groups()
    cg_holder.get_numbers()
    return cg_holder


def create_fcgd_holder(flow_container: BaseContainer) -> FlowCGHolder:
    """Creating a holder for the flow-collective-group-dependency order."""

    n_flows = len(flow_container.item_ids)
    matrix = np.zeros((n_flows, 3), dtype=int)
    mapper = dict()
    for idx, flow in enumerate(flow_container.item_objs):
        matrix[idx, 0] = flow.collective_id
        matrix[idx, 1] = flow.group_id
        matrix[idx, 2] = flow.get_dependency_order()
        mapper["collective"] = flow.collective_id
        mapper["group"] = flow.group_id
        mapper["order"] = flow.get_dependency_order()

    fcg_holder = FlowCGHolder(
        flow_ids=flow_container.item_ids, f2cgd_mapper=mapper, matrix=matrix
    )

    return fcg_holder


def extract_information(config_path: str, filename: str):
    """Extracting the information from the configuration file."""
    # load the json to dict
    file_path = os.path.join(config_path, filename)
    with open(file_path, "r", encoding="utf-8") as f:
        info_data = json.load(f)

    f_container, l_container = create_fl_containers(info_data)
    fl_holder = create_fl_holder(f_container, l_container)
    fl_s_holder = create_fl_send_holder(fl_holder, f_container, l_container)

    # Create the collective groups holder
    cg_holder = create_cg_holder(flow_container=f_container)

    # Create the collective group flow holder
    fcg_holder = create_fcgd_holder(flow_container=f_container)

    return f_container, l_container, fl_s_holder, cg_holder, fcg_holder


def match_flow_tau(
    big_tau: List[List[int]],
    fl_s_holder: FlowLinkSendHolder,
    fcg_holder: FlowCGHolder,
    cg_container: CollectiveGroupContainer,
):
    """Match each flow with the corresponding tau."""
    F = fl_s_holder.fl_holder.F
    matrix = np.zeros(F)

    for idx in range(F):
        coll_id, group_id = fcg_holder.matrix[idx, 0:2]
        # Find the coll and group index
        coll_idx = np.where(cg_container.collective_ids == coll_id)[0][0]
        groups = cg_container.group_ids[coll_idx]
        group_idx = np.where(groups == group_id)[0][0]
        tau = big_tau[coll_idx][group_idx]
        matrix[idx] = tau

    return matrix


def perform_steller(
    flow_container: BaseContainer,
    link_container: BaseContainer,
    fl_s_holder: FlowLinkSendHolder,
    cg_container: CollectiveGroupContainer,
    fcg_holder: FlowCGHolder,
    opt_config: dict,
):
    """
    Performing the stellar algorithm toward optimizing the flow rates of groups to minimize the average completion time of collectives.

    :param flow_container: A BaseContainer containing the flows.
     With len(flow_ids) = N

    :param link_container: A BaseContainer containing the links.
     With len(link_ids) = E

    :param fl_holder: A FlowLinkHolder containing the flow-link relation.
     With len(fl_holder.matrix) = [N, E]

    :param cg_holder: A CollectiveGroupContainer containing the collective-group relation.
     With len(cg_holder.collective_ids) = K
     With cg_holder.group_ids[k].shape[0] = N^k

    :param fcg_holder: A FlowCGHolder containing the flow-collective-group relation.
     With fcg_holder.matrix: [N, 3]
    """
    start_op = time.time()

    # Stage 1. Optimizing the completion times of groups
    _, _, big_tau = optimize_completion_time(
        link_container,
        fl_s_holder,
        cg_container,
        fcg_holder,
        opt_parameters=opt_config,
    )
    end_op = time.time()
    # Stage 2. Optimizing the flow rates of groups
    # optimal_sol, _, _ = optimize_flow_rates(
    #     flow_container,
    #     big_tau,
    #     link_container,
    #     fl_s_holder,
    #     cg_container,
    #     fcg_holder,
    #     opt_parameters=opt_config,
    # )
    start_or = time.time()
    optimal_sol, ablation_sol, _ = optimize_lp_flow_rates(
        flow_container,
        big_tau,
        link_container,
        fl_s_holder,
        cg_container,
        fcg_holder,
        opt_parameters=opt_config,
    )
    end_or = time.time()

    best_kn_rates = optimal_sol[0]
    # Toward ablation study, we get the flow rates based on the big tau
    # of the OP - Stage 1 of our algorithm
    ablation_kn_rates = ablation_sol[0]
    time_cost = {"OP-Time": end_op - start_op, "OR-Time": end_or - start_or}
    print(f"time cost: {end_or - start_op}")
    return best_kn_rates, time_cost
