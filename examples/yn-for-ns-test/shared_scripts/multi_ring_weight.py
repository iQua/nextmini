"""
Concurrent + Non-Concurrent
# use weight_collective_allocation() to get optimal weight for every collective
# put b_k into flow_chunk_optimization to process non-concurrent scheduling
# objective: average completion time
"""

import time
import math
import logging
from collections import defaultdict
from typing import Dict, Tuple, List
import cvxpy as cp


def get_flows_with_same_links(
    flow_info: Dict[int, dict],
    *,
    same_collective_only: bool = True,
) -> Dict[int, List[int]]:
    """return {fid: [fid_j, ...]}，flows that share the link in the same collective"""
    edge_record: Dict[int, List[int]] = defaultdict(list)
    items = list(flow_info.items())
    for i in range(len(items)):
        fid_i, f_i = items[i]
        for j in range(i + 1, len(items)):
            fid_j, f_j = items[j]
            if same_collective_only and f_i["collective"] != f_j["collective"]:
                continue
            if f_i['group'] == f_j['group']:
                continue
            if f_i["links"] & f_j["links"]:
                edge_record[fid_i].append(fid_j)
    return edge_record


def weight_collective_allocation(
    flow_info: Dict[int, dict],
    link_cap: Dict[str, float],
    flows_for_each_link: Dict[int, Dict[int, List[int]]],
) -> Dict[int, float]:
    """
    return {fid: bandwidth_MBps}
    """
    # Calculate collective's total data size
    S = defaultdict(float)     # collective -> MB
    for fid, f in flow_info.items():
        S[f["collective"]] += f["data_size"]
    total_S = sum(S.values())
    w = {k: S[k] / total_S for k in S}      # weight normalization

    # Construct link → {collective}
    link2collectives = {}
    for link_id, grp_dict in flows_for_each_link.items():
        col_set = set(flow_info[fid]["collective"] for g in grp_dict.values() for fid in g)
        link2collectives[link_id] = col_set

    # Calculate bottleneck link capacity for each flow
    flow_rate = {}

    for fid, f in flow_info.items():
        k = f["collective"]
        path_rates = []
        for e in f["links"]:
            cap_MBps = link_cap[str(e)] / 1024 / 1024 / 8
            col_set  = link2collectives[e]
            if len(col_set) == 1:                 # one flow occupy the link
                path_rates.append(cap_MBps)
            else:                                 # allocate by weight ratio
                share = cap_MBps * w[k] / sum(w[c] for c in col_set)
                path_rates.append(share)
        flow_rate[fid] = min(path_rates)
    return flow_rate



def flow_chunk_optimization4(
    flow_info: Dict[int, dict],
    link_cap: Dict[str, float],
    dependency_order: Dict[Tuple[int, int], list],
    fid_to_order_dict: Dict[int, Tuple[int]],
    flows_for_each_link: Dict[int, Dict[int, List[int]]],
    mip_time_limit: int = 10,
):
    """
    one by one + weight
    """
    start_time = time.time()
    # per-flow bandwidth by weight
    flow_rates = weight_collective_allocation(
        flow_info, link_cap, flows_for_each_link
    )

    print("\n========= Flow bandwidth (MB/s) =========")
    for fid in sorted(flow_rates, key=int):
        print(f"flow {fid}: {flow_rates[fid]:.3f}")

    # Duration
    K   = max(f["collective"] for f in flow_info.values())
    dur = {fid: info["data_size"] / max(flow_rates[fid], 1e-12)
           for fid, info in flow_info.items()}


    # Same as above
    s = {fid: cp.Variable(nonneg=True, name=f"s_{fid}") for fid in flow_info}
    T = {k: cp.Variable(nonneg=True, name=f"T_{k}") for k in range(1, K + 1)}
    constraints = []

    # collective completion time
    for fid, f in flow_info.items():
        constraints.append(T[f["collective"]] >= s[fid] + dur[fid])

    # intra-collective flow dependency
    for fid, f in flow_info.items():
        order = fid_to_order_dict[fid][0]
        if order > 1:
            k, n = f["collective"], f["group"]
            prev_fid = dependency_order[(k, n)][order - 2]
            constraints.append(s[fid] >= s[prev_fid] + dur[prev_fid])

    # intra-collective: no two flows can share the same link
    edge_record = get_flows_with_same_links(flow_info, same_collective_only=True)
    bigM = sum(dur.values()) + max(dur.values()) + 1
    for fid, others in edge_record.items():
        for other in others:
            b_var = cp.Variable(boolean=True, name=f"bin_{fid}_{other}")
            constraints += [
                s[fid] + dur[fid] <= s[other] + bigM * (1 - b_var),
                s[other] + dur[other] <= s[fid] + bigM * b_var,
            ]

    # Objective
    objective = cp.Minimize(cp.sum(list(T.values())))
    prob = cp.Problem(objective, constraints)
    prob.solve(
        solver=cp.MOSEK,
        mosek_params={
            "MSK_IPAR_LOG": 3,
            "MSK_DPAR_MIO_MAX_TIME": mip_time_limit,
            "MSK_IPAR_NUM_THREADS": 4,
        },
    )
    end_time = time.time()
    # Schedule results
    if prob.status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE):
        print("\n========= Schedule =========")
        for fid, var in s.items():
            print(
                f"flow {fid:>3}: start={var.value:6.1f}, "
                f"finish={var.value + dur[fid]:6.1f},  dur={dur[fid]:4}"
            )
    else:
        logging.warning(f"Solver status = {prob.status}")

    avgT = prob.value / K if prob.value is not None else None

    # Get natural and artificial dependencies for nextmini
    dependencies_map = defaultdict(list)

    # Get natural dependencies
    for fid, f in flow_info.items():
        order = fid_to_order_dict[fid][0]
        if order > 1:
            k, n = f["collective"], f["group"]
            prev_fid = dependency_order[(k, n)][order - 2]
            dependencies_map[fid].append(prev_fid)

    # Get artificial dependencies from non-concurrency constraints
    if prob.status in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE):
        for fid, others in edge_record.items():
            for other in others:
                s_fid_val = s[fid].value
                s_other_val = s[other].value
                dur_fid = dur[fid]
                dur_other = dur[other]

                if s_fid_val is None or s_other_val is None:
                    continue

                # Artificial ordering from scheduling result
                if s_fid_val + dur_fid <= s_other_val + 1e-3:
                    dependencies_map[other].append(fid)
                elif s_other_val + dur_other <= s_fid_val + 1e-3:
                    dependencies_map[fid].append(other)

    # Remove duplicates and sort the dependency list
    dependencies_map = {fid: sorted(set(dep_list)) for fid, dep_list in dependencies_map.items()}

    time_cost = end_time - start_time
    logging.info(f"time cost: {time_cost}")
    logging.info(f"objective value: {avgT}")

    return {"avg_completion_time": avgT, "time_cost": time_cost, "flow_rate": flow_rates, "flow_dependencies":dependencies_map}
