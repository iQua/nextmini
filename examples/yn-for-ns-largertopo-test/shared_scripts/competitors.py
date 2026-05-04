"""
Implementations of the competitors for the Stellar project.
"""

import os
import time
import numpy as np
import logging
from typing import List

from generic import (
    BaseContainer,
    FlowLinkSendHolder,
    CollectiveGroupContainer,
    FlowCGHolder,
)
from opt_utils import v_kne, compute_average_completion_time, get_group_flows
from utils import save_alloc_solutions
from common_utils import get_link_groups

baseline_factory = ["averageAlloc", "dataAwareAlloc", "groupdataAwareAlloc"]


def average_bandwidth(link_capacity: float, n_groups: int, n_average: int):
    """Allocating the bandwidth equally among group."""
    return np.ones(n_groups) * link_capacity / n_average


def group_data_aware_bandwidth(
    col_groups,
    flow_container,
    link_capacity: float,
    fcg_holder: FlowCGHolder,
    cg_container: CollectiveGroupContainer,
):
    """Allocating the bandwidth based on the group data."""
    col_groups_data = []
    for collective_idx, group_idx in col_groups:
        group_data = [
            flow_container.item_objs[flow_idx].data_volume
            for flow_idx in get_group_flows(
                collective_idx, group_idx, fcg_holder.matrix, cg_container
            )
        ]
        col_groups_data.append(sum(group_data))

    ratio = np.array(col_groups_data) / sum(col_groups_data)
    return ratio * link_capacity


def data_aware_link_allocation(
    col_groups: List[tuple],
    link_idx: int,
    fcg_holder: FlowCGHolder,
    cg_container: CollectiveGroupContainer,
    fl_s_holder: FlowLinkSendHolder,
    link_capacity: float,
):
    """
    Allocating the bandwidth based on the data size of the group on the link.
    """
    group_data = []
    for collective_idx, group_idx in col_groups:
        group_link_data = v_kne(
            collective_idx,
            group_idx,
            link_idx,
            fl_s_holder.data_matrix,
            fcg_holder.matrix,
            cg_container,
        )
        group_data.append(group_link_data)

    ratio = np.array(group_data, dtype=float) / sum(group_data)
    return ratio * link_capacity


def baseline_allocation(
    flow_container: BaseContainer,
    link_container: BaseContainer,
    fl_s_holder: FlowLinkSendHolder,
    cg_container: CollectiveGroupContainer,
    fcg_holder: FlowCGHolder,
    opt_config: dict,
    method_name: str = "averageAlloc",
):
    """Allocating the bandwidth equally among group."""
    # Set the save path
    save_path = opt_config["model_path"]
    os.makedirs(save_path, exist_ok=True)

    start_time = time.time()

    capacities = np.array(
        [link.capacity for link in link_container.item_objs]
    )
    sort_index = np.argsort(capacities)
    l2h_capacities = capacities[sort_index]

    K = cg_container.K
    Nks = cg_container.Nks

    big_R = [[0 for _ in range(Nks[k])] for k in range(K)]

    for idx, min_capacity in enumerate(l2h_capacities):
        link_idx = sort_index[idx]
        # 2. Get the flows that are sent on this link
        link_flows = fl_s_holder.fl_holder.matrix[:, link_idx]
        flow_indxes = np.where(link_flows == 1)[0]

        # We skip the allocation once the link is not used
        # by any flows and if it is used, the compete flows
        # should be more than 2.
        if len(flow_indxes) <= 1:
            continue

        # 3. Get the groups for these flows
        # with shape [len(flow_indxes), 2]
        # [:,0]: collective id, [: 1]: group id
        coll_groups = fcg_holder.matrix[flow_indxes, :2]
        coll_groups = np.unique(coll_groups, axis=0)
        # We skip the allocation once the there is no compete
        # in the link
        if len(coll_groups) == 1:
            continue

        # get their indexes
        coll_groups_idx = []
        for coll_id, group_id in coll_groups:
            col_idx = np.where(cg_container.collective_ids == coll_id)[0][0]
            group_idx = np.where(cg_container.group_ids[col_idx] == group_id)[0][0]
            coll_groups_idx.append((col_idx, group_idx))

        # 3.1. Check if the groups are already allocated and minus the capacity by them
        allocated_capacity = sum(
            [big_R[coll_idx][group_idx] for coll_idx, group_idx in coll_groups_idx]
        )
        min_capacity -= allocated_capacity
        # 3.2. Get the number of groups that have not been allocated
        to_allocated_groups = [
            (coll_idx, group_idx)
            for coll_idx, group_idx in coll_groups_idx
            if big_R[coll_idx][group_idx] == 0
        ]
        # 4. Allocate the bandwidth equally among the groups
        # Get allocations: A list holding the allocated bandwidth
        # for each group
        if method_name == "dataAwareAlloc":
            allocations = data_aware_allocation(
                to_allocated_groups,
                link_idx,
                fcg_holder,
                cg_container,
                fl_s_holder,
                min_capacity,
            )
        if method_name == "averageAlloc":
            allocations = average_bandwidth(
                min_capacity, len(to_allocated_groups), len(to_allocated_groups)
            )

        for i, (coll_idx, group_idx) in enumerate(to_allocated_groups):
            if big_R[coll_idx][group_idx] == 0:
                big_R[coll_idx][group_idx] = allocations[i]

    flow_datas = np.array([flow.data_volume for flow in flow_container.item_objs])

    big_R_obj = compute_average_completion_time(
        big_R, fcg_holder, cg_container, flow_datas, K, Nks
    )
    solution = (big_R, big_R_obj)
    save_alloc_solutions(
        os.path.join(save_path, "optimized_flow_rates.json"),
        [solution],
    )

    end_time = time.time()
    logging.info("time cost: %s", end_time - start_time)
    logging.info("objective value: %s", big_R_obj)
    return big_R, end_time - start_time


def barrier_aware_allocation(
    flow_container: BaseContainer,
    link_container: BaseContainer,
    fl_s_holder: FlowLinkSendHolder,
    cg_container: CollectiveGroupContainer,
    fcg_holder: FlowCGHolder,
    opt_config: dict,
):
    """
    Performing the allocation based on the barrier-aware approach.
    """
    start_time = time.time()

    # Set the save path
    save_path = opt_config["model_path"]
    os.makedirs(save_path, exist_ok=True)

    # Get the basic numbers
    K = cg_container.K
    Nks = cg_container.Nks
    F = fl_s_holder.fl_holder.F
    E = fl_s_holder.fl_holder.E

    capacities = np.array([link.capacity for link in link_container.item_objs])
    big_R = [[0 for _ in range(Nks[k])] for k in range(K)]
    allocated_collectives = []
    visited_link_indexes = []
    while len(allocated_collectives) != K:

        # Always reduce the link capacity for those allocated collectives and groups
        for k in range(K):
            for n in range(Nks[k]):
                if big_R[k][n] != 0:
                    for e in range(E):
                        for flow_idx in get_group_flows(
                            k, n, fcg_holder.matrix, cg_container
                        ):
                            if fl_s_holder.data_matrix[flow_idx, e] > 0:
                                capacities[e] -= big_R[k][n]

        left_link_indexes = [e for e in range(E) if e not in visited_link_indexes]

        left_collective_indexes = [
            k for k in range(K) if k not in allocated_collectives
        ]

        a_E = np.zeros(len(left_link_indexes), dtype=float)
        b_E = np.zeros(len(left_link_indexes), dtype=float)

        for idx, e in enumerate(left_link_indexes):
            capacity = capacities[e]
            for k in left_collective_indexes:
                for n in range(Nks[k]):
                    for flow_idx in range(F):
                        group_flows = get_group_flows(
                            k, n, fcg_holder.matrix, cg_container
                        )
                        if flow_idx in group_flows:
                            if fl_s_holder.data_matrix[flow_idx, e] > 0:
                                # find the e in the position of left_link_indexes
                                a_E[idx] += fl_s_holder.data_matrix[flow_idx, e]

            # We need to replace the 0 term in a_E (when no flow is sent on the link)
            a_E[idx] = 0.1 if a_E[idx] == 0 else a_E[idx]
            b_E[idx] = capacity / a_E[idx]

        lambda_optimal = np.min(b_E)

        # Obtain Bottleneck Links the that satisfies: b_e == lambda_optimal
        bottle_link_indexes = [
            e for idx, e in enumerate(left_link_indexes) if b_E[idx] == lambda_optimal
        ]

        # Obtain Bottleneck groups containing the flows that traverse the bottleneck links;
        bottle_collectives = []
        for k in left_collective_indexes:
            for n in range(Nks[k]):
                group_flows = get_group_flows(k, n, fcg_holder.matrix, cg_container)
                rates = []
                for e_idx in bottle_link_indexes:
                    for flow_idx in group_flows:
                        group_link_data = fl_s_holder.data_matrix[flow_idx, e_idx]
                        if group_link_data > 0:
                            if k not in bottle_collectives:
                                bottle_collectives.append(k)
                            rates.append(lambda_optimal * group_link_data)
                if len(rates) != 0:
                    big_R[k][n] = min(rates)

        # Remove the bottleneck collectives by setting that they are the allocated collectives
        allocated_collectives.extend(bottle_collectives)
        # Remove the bottleneck links by setting that they are the visited links
        visited_link_indexes.extend(bottle_link_indexes)

    flow_datas = np.array([flow.data_volume for flow in flow_container.item_objs])

    big_R_obj = compute_average_completion_time(
        big_R, fcg_holder, cg_container, flow_datas, K, Nks
    )
    solution = (big_R, big_R_obj)
    save_alloc_solutions(
        os.path.join(save_path, "optimized_flow_rates.json"),
        [solution],
    )

    end_time = time.time()
    logging.info("time cost: %s", end_time - start_time)
    logging.info("objective value: %s", big_R_obj)
    return big_R, end_time - start_time


def data_aware_allocation(
    flow_container: BaseContainer,
    link_container: BaseContainer,
    fl_s_holder: FlowLinkSendHolder,
    cg_container: CollectiveGroupContainer,
    fcg_holder: FlowCGHolder,
    opt_config: dict,
):
    """Allocating the bandwidth equally among group."""
    # Set the save path
    save_path = opt_config["model_path"]
    os.makedirs(save_path, exist_ok=True)

    start_time = time.time()

    capacities = np.array(
        [link.capacity for link in link_container.item_objs]
    )

    K = cg_container.K
    Nks = cg_container.Nks
    E = fl_s_holder.fl_holder.E

    big_R = [[0 for _ in range(Nks[k])] for k in range(K)]

    # Start from any group to search for the data-aware allocation
    for k in range(K):
        for n in range(Nks[k]):

            # Get all flow indexes of the group
            group_flow_idxes = get_group_flows(k, n, fcg_holder.matrix, cg_container)
            # Get the links that this group is using
            group_link_idxes = list()
            for e in range(E):
                if any(
                    [fl_s_holder.data_matrix[idx, e] > 0 for idx in group_flow_idxes]
                ):
                    group_link_idxes.append(e)

            # Visit these links to obtain the data-aware allocation
            link_allocations = list()
            for e_idx in group_link_idxes:
                link_capacity = capacities[e_idx]
                # Get all other groups that are using this link
                coll_groups, coll_groups_idxes = get_link_groups(
                    fl_s_holder.fl_holder.matrix,
                    fcg_holder.matrix,
                    e_idx,
                    cg_container.collective_ids,
                    cg_container.group_ids,
                )
                # Check if the group passing the link has been allocated
                allocated = [
                    (big_R[coll_idx][group_idx], i)
                    for i, (coll_idx, group_idx) in enumerate(coll_groups_idxes)
                    if big_R[coll_idx][group_idx] != 0
                ]
                # Remove the allocated bandwidth from the link capacity
                link_capacity -= sum([a for a, _ in allocated])
                # Remove the allocated groups from the list
                to_allocate_groups = [
                    (coll_idx, group_idx)
                    for j, (coll_idx, group_idx) in enumerate(coll_groups_idxes)
                    if j not in [i for _, i in allocated]
                ]
                # Get the data of the group on the link
                allocations = data_aware_link_allocation(
                    to_allocate_groups,
                    e_idx,
                    fcg_holder,
                    cg_container,
                    fl_s_holder,
                    link_capacity,
                )
                # Get the alloc of this group
                loc = to_allocate_groups.index((k, n))
                link_allocations.append(allocations[loc])

            # Get the minimum allocation from all links as the capacity of this group
            min_alloc = min(link_allocations)
            big_R[k][n] = min_alloc

    flow_datas = np.array([flow.data_volume for flow in flow_container.item_objs])

    big_R_obj = compute_average_completion_time(
        big_R, fcg_holder, cg_container, flow_datas, K, Nks
    )
    solution = (big_R, big_R_obj)
    save_alloc_solutions(
        os.path.join(save_path, "optimized_flow_rates.json"),
        [solution],
    )

    end_time = time.time()
    logging.info("time cost: %s", end_time - start_time)
    logging.info("objective value: %s", big_R_obj)

    return big_R, end_time - start_time
