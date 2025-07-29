"""
Implemented utility functions for optimization.
"""

import numpy as np

from generic import CollectiveGroupContainer


def get_group_flows(
    collective_idx: int,
    group_idx: int,
    fcg_matrix: np.ndarray,
    cg_container: CollectiveGroupContainer,
):
    """Get the flow indexes that belong to the specific group."""
    # 1. Get flows of the 'collective_idx' collective
    coll_id = cg_container.coll_id(collective_idx)
    group_id = cg_container.group_id(collective_idx, group_idx)
    is_collective = fcg_matrix[:, 0] == coll_id
    is_group = fcg_matrix[:, 1] == group_id

    flow_indexes = np.where(is_collective & is_group)[0]
    return flow_indexes


def v_kne(k, n, e, flow_links, fcg_matrix, cg_container):
    """The v^{k,n}_e defined in Lemma 4 of the paper."""
    return sum(
        flow_links[flow, e] for flow in get_group_flows(k, n, fcg_matrix, cg_container)
    )


def compute_average_completion_time(
    big_R, fcg_holder, cg_container, flow_datas, K, Nks
):
    """
    Computing the average completion time of the collectives.
    """
    completion_times = list()
    # We visit each group
    for k in range(K):
        k_completion_times = list()
        for n in range(Nks[k]):
            group_flows = get_group_flows(k, n, fcg_holder.matrix, cg_container)

            group_flow_datas = flow_datas[group_flows]
            group_rate = big_R[k][n]
            k_completion_times.append(sum(group_flow_datas) / group_rate)

        completion_times.append(k_completion_times)

    # Get the maximum value in each sub-list of the completion_times
    max_completion_times = [
        max(k_completion_times) for k_completion_times in completion_times
    ]
    return np.average(max_completion_times)
