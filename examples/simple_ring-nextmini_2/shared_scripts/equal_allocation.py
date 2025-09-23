import io
import time
import logging
from collections import defaultdict

def equal_share_full_outof_dependency(flow_info, link_cap, flows_for_each_link):
    """
    Baseline: on every link, split capacity equally among *all* flows using it.
    """
    t0 = time.time()
    logging.info("**** Start Equal-Share Baseline ****")

    # Equally allocate the bandwidth
    per_link_share = {}
    for link_id, groups_dict in flows_for_each_link.items():
        # flat_flows = [fid for group_id in groups_dict.keys() for fid in groups_dict[group_id]]
        flat_flows = [fid for fids in groups_dict.values() for fid in fids]
        cap_MBps   = link_cap[str(link_id)] / 1024 / 1024 / 8
        per_link_share[link_id] = cap_MBps / max(len(flat_flows), 1)

    # Get effective sending rate
    b = {}
    for fid, f in flow_info.items():
        shares = [per_link_share[link_id] for link_id in f["links"]]
        b[fid] = min(shares) if shares else float("inf")

    # Print the allocated sending rate of each flow
    print("\n===== Flow Bandwidth Allocation (Equal-Share) =====")
    for fid in sorted(b):
        print(f"Flow {fid:>4}: {b[fid]:6.2f} MB/s")

    # Calculate completion time
    dur  = {fid: flow_info[fid]["data_size"] / b[fid] for fid in flow_info}
    K    = max(f["collective"] for f in flow_info.values())
    makespan = {k: 0.0 for k in range(1, K+1)}
    group_finish = defaultdict(float)

    for fid, f in flow_info.items():
        k, n = f["collective"], f["group"]
        group_finish[(k, n)] += dur[fid]

    for (k, n), t in group_finish.items():
        makespan[k] = max(makespan[k], t)

    avg_completion = sum(makespan.values()) / K
    time_cost = time.time() - t0

    # Print the completion time of each flow
    print("\n===== Flow Completion Time (Equal-Share) =====")
    for fid in sorted(dur):
        print(f"Flow {fid:>4}: {dur[fid]:6.2f} s")

    logging.info(f"time cost: {time_cost}")
    logging.info(f"objective value: {avg_completion}")
    return {"avg_completion_time": avg_completion, "time_cost": time_cost, "flow_rate":b}




def equal_share_by_group_baseline(
    flow_info: dict,
    link_cap: dict,
    flows_for_each_link: dict,
):
    """
    Baseline-2：Allocate bandwidth equally among flow groups
    Flows from the same group will share the same sending rate
    """
    t0 = time.time()
    logging.info("**** Start Equal-Share-by-Group Baseline ****")

    # Calculate the number of groups on each link
    per_link_group_share = {}            # link -> MB/s per group
    for link_id, groups_dict in flows_for_each_link.items():
        n_groups = len(groups_dict)      # each key = one group
        cap_MBps = link_cap[str(link_id)] / 1024 / 1024 / 8
        per_link_group_share[link_id] = cap_MBps / max(n_groups, 1)

    # Record each flow group's allocated sending rate on each link, and calculate the effective bandwidth for each flow
    group_bandwidth = defaultdict(lambda: float("inf"))  # (collective, group) -> MB/s
    flow_bandwidth  = {}                                 # fid -> MB/s

    for fid, f in flow_info.items():
        k, n = f["collective"], f["group"]
        shares = [per_link_group_share[l] for l in f["links"]]
        bw     = min(shares) if shares else float("inf")

        # Get effective bandwidth for each flow
        group_bandwidth[(k, n)] = min(group_bandwidth[(k, n)], bw)
        flow_bandwidth[fid] = group_bandwidth[(k, n)]

    # Calculate the duration of each flow
    dur = {}
    for fid, f in flow_info.items():
        k, n = f["collective"], f["group"]
        bw   = group_bandwidth[(k, n)]
        dur[fid] = f["data_size"] / bw

    # Calculate the total completion time of each flow group，and get the latest completion time of each collective
    group_finish = defaultdict(float)
    for fid, f in flow_info.items():
        k, n = f["collective"], f["group"]
        group_finish[(k, n)] += dur[fid]

    K = max(f["collective"] for f in flow_info.values())
    makespan = {k: 0.0 for k in range(1, K+1)}
    for (k, n), t_g in group_finish.items():
        makespan[k] = max(makespan[k], t_g)

    avg_completion = sum(makespan.values()) / K
    time_cost = time.time() - t0

    print("\n===== Flow Bandwidth Allocation (Equal-Share by Group) =====")
    for fid in sorted(flow_bandwidth):
        print(f"Flow {fid:>4}: {flow_bandwidth[fid]:6.2f} MB/s  "
              f"dur {dur[fid]:6.2f}s")

    print("\nGroup-level bottleneck bandwidth:")
    for (k, n), bw in group_bandwidth.items():
        print(f"Collective {k}, Group {n}: {bw:.2f} MB/s")

    logging.info(f"time cost: {time_cost}")
    logging.info(f"objective value: {avg_completion}")
    return {"avg_completion_time": avg_completion, "time_cost": time_cost, "flow_rate": flow_bandwidth}





def equal_share_per_link_flow(
    flow_info: dict,
    link_cap: dict,
    flows_for_each_link: dict,
):
    """
    Baseline-3：Allocate bandwidth equally to each individual flow
    Each individua flow will have its own sending rate
    """
    t0 = time.time()
    logging.info("*** Start Equal-Share by Group per Link ***")

    # Calculate the number of (collective, group) pairs on each link
    per_link_share = {}
    for link_id, groups_dict in flows_for_each_link.items():
        # groups_dict: {group_id: [flow_id, ...], ...}
        # Need collective -> (k, n)
        distinct_pairs = set()
        for group, fids in groups_dict.items():
            fid0 = fids[0]
            k = flow_info[fid0]["collective"]
            distinct_pairs.add((k, group))
        cap_MBps = link_cap[str(link_id)] / 1024 / 1024 / 8
        per_link_share[link_id] = cap_MBps / max(len(distinct_pairs), 1)

    # Get effective sending rate for each flow
    flow_bw = {}
    for fid, f in flow_info.items():
        shares = [per_link_share[l] for l in f["links"]]
        flow_bw[fid] = min(shares) if shares else float("inf")

    print("\n===== Flow Bandwidth Allocation (Per-Link, Group-Aware) =====")
    for fid in sorted(flow_bw):
        print(f"Flow {fid:>4}: {flow_bw[fid]:6.2f} MB/s")

    # Calculate the duration of each flow
    flow_dur = {fid: flow_info[fid]["data_size"] / flow_bw[fid] for fid in flow_info}

    # Calculate the total completion time of each flow group，and get the latest completion time of each collective
    group_finish = defaultdict(float)
    for fid, f in flow_info.items():
        k, n = f["collective"], f["group"]
        group_finish[(k, n)] += flow_dur[fid]

    K = max(f["collective"] for f in flow_info.values())
    makespan = {k: 0.0 for k in range(1, K + 1)}
    for (k, n), t_g in group_finish.items():
        makespan[k] = max(makespan[k], t_g)

    avg_completion = sum(makespan.values()) / K
    time_cost = time.time() - t0

    logging.info(f"time cost: {time_cost}")
    logging.info(f"objective value: {avg_completion}")
    return {"avg_completion_time": avg_completion, "time_cost": time_cost, "flow_rate": flow_bw}
