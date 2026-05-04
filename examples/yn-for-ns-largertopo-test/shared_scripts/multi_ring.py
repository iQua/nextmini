"""
Concurrent + Non-Concurrent
# use dynamic_collective_allocation() to get optimal b_k for every collective
# print b_k
# put b_k into flow_chunk_optimization to process non-concurrent scheduling
# objective: average completion time
"""

import time
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


#  dynamic bandwidth allocation (per-collective)

def dynamic_collective_allocation(
    flow_info: Dict[int, dict],
    link_cap: Dict[str, float],
    flows_for_each_link: Dict[int, Dict[int, List[int]]],
) -> Dict[int, float]:
    """
    get the optimal bandwidth b_k for each collective
    return dict {k: b_k}
    """
    t0 = time.time()
    logging.info("**** Dynamic collective allocation ****")

    K = max(f["collective"] for f in flow_info.values())
    collectives = range(1, K + 1)

    # S_k = sum of flow size of kth collective (MB)
    S = {k: 0.0 for k in collectives}
    for f in flow_info.values():
        S[f["collective"]] += f["data_size"]

    # link: all the collectives that pass
    link_cols = defaultdict(set)  # link_id -> {k}
    for link_id, grp_dict in flows_for_each_link.items():
        for grp, flow_ids in grp_dict.items():
            for fid in flow_ids:
                link_cols[link_id].add(flow_info[fid]["collective"])

    # Variables
    b = {k: cp.Variable(pos=True, name=f"bw_{k}") for k in collectives}
    eps = 1e-6
    constraints = [b[k] >= eps for k in collectives]

    # Link Capacity
    for link_id, kset in link_cols.items():
        cap_MBps = link_cap[str(link_id)] / 1024 / 1024 / 8
        constraints.append(cp.sum([b[k] for k in kset]) <= cap_MBps)

    # ∑ S_k / b_k
    objective = cp.Minimize(cp.sum([S[k] * cp.inv_pos(b[k]) for k in collectives]))

    prob = cp.Problem(objective, constraints)
    prob.solve(solver=cp.MOSEK, mosek_params={"MSK_IPAR_LOG": 3})

    if prob.status not in (cp.OPTIMAL, cp.OPTIMAL_INACCURATE):
        raise RuntimeError(f"Dynamic allocation failed: {prob.status}")

    logging.info(
        "Dynamic alloc done. AvgT=%.3fs   (%.2fs)",
        prob.value / K,
        time.time() - t0,
    )
    ans = {}
    for k in collectives:
        val = b[k].value
        if val is None:
            ans[k] = eps
        else:
            ans[k] = float(val.item())
    return ans


# =========================================================================
#  final scheduling with fixed durations
# =========================================================================

def flow_chunk_optimization3(
    flow_info: Dict[int, dict],
    link_cap: Dict[str, float],
    dependency_order: Dict[Tuple[int, int], list],
    fid_to_order_dict: Dict[int, Tuple[int]],
    flows_for_each_link: Dict[int, Dict[int, List[int]]],
    mip_time_limit: int = 10,
):
    """
    one by one + dynamic
    """
    start_time = time.time()
    # Allocate bandwidth by dynamicAlloc
    bw_dict = dynamic_collective_allocation(
        flow_info, link_cap, flows_for_each_link
    )

    # Print each flow's allocated bandwidth
    print("\n========= Allocated bandwidth per collective =========")
    for k in sorted(bw_dict):
        print(f"Collective {k}: {bw_dict[k]:.3f} MB/s")

    # Calculate duration
    K = max(f["collective"] for f in flow_info.values())
    dur = {fid: info["data_size"]/bw_dict[info["collective"]] for fid, info in flow_info.items()}

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
    time_cost = end_time - start_time

    # Get natural and artificial dependencies for nextmini
    dependencies_map = defaultdict(list)
    flow_rates = {}

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

    # Get flow rates from bandwidth dict
    for fid in flow_info:
        flow_rates[fid] = bw_dict[flow_info[fid]["collective"]]

    logging.info(f"time cost: {time_cost}")
    logging.info(f"objective value: {avgT}")
    return {"avg_completion_time": avgT, "time_cost": time_cost, "flow_rate": flow_rates, "flow_dependencies":dependencies_map}
