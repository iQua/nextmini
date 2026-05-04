###### Objective Convergence #####
import logging, time, random
import cvxpy as cp
from typing import Dict, Tuple, List
import numpy as np
random.seed(42)
np.random.seed(42)

# Assigns initial weights inversely based each group's bottleneck link
def bottleneck_initial_weights(
    flow_info: Dict[str, dict],             # flow meta, each value has keys: collective, group, data_size, links
    link_cap: Dict[str, float],             # link capacity  (Byte/s or bit/s – keep consistent)
    link2flows: dict,                       # {link: {group: [fid,...]}}
    fid_group: Dict[str, Tuple[int, int]],  # "flow_id": (collective_id, group_id)
    groups: List[Tuple[int, int]],          # [(collective_id, group_id), ...]
)-> Dict[Tuple[int, int], float]:

    # Create a mapping {"link_id": {(collective, group), (collective, group)}}
    link2groups = {
        e: set(fid_group[fid]
            for fids in link2flows[e].values()
                for fid in fids)
        for e in link2flows
    }

    # Map each group (collective, group) to its bottleneck link rate
    group2bottleneck = {}
    for (k, n) in groups:
        fids = [fid for fid, gn in fid_group.items() if gn == (k, n)]
        if not fids:
            group2bottleneck[(k, n)] = 1e-12  # avoid division by zero
            continue
        paths = [flow_info[fid]["links"] for fid in fids]
        all_edges = set(e for path in paths for e in path)
        bottleneck = min(
            link_cap[str(e)] / max(len(link2groups[e]), 1) # include 1 for safety
            for e in all_edges
        )
        #logging.info(f"Group {(k, n)}: Bottleneck capacity = {bottleneck/1024/1024/8:.2f} MBps")
        group2bottleneck[(k, n)] = max(bottleneck, 1e-12)

    group2inv_bottleneck = {group: 1 / rate for group, rate in group2bottleneck.items()}
    cumulative_inv_bottlenecks = sum(group2inv_bottleneck.values())

    # Normalize to make the weights sum to 1
    return {group: inv_bottleneck/cumulative_inv_bottlenecks for group, inv_bottleneck in group2inv_bottleneck.items()}


#  each group (collective k, group n) has a weight w_{k,n}  (Σw = 1)
#  outer loop: freeze old weights, linearise F_i / b_i, solve convex sub‑problem
#  convergence when |obj^{t} − obj^{t−1}| < tol_obj  (true objective)
def weight_allocation_optimization(
    flow_info: Dict[str, dict],            # flow meta, each value has keys: collective, group, data_size, links
    link_cap: Dict[str, float],            # link capacity  (Byte/s or bit/s – keep consistent)
    fid_to_order: dict,
    link2flows: dict,                      # {link: {group: [fid,…]}}
    max_iter: int = 50,
    tol_obj: float = 1e-3,
    verbose: bool = False,
) -> Tuple[dict, dict, float, float]:
    """Return (weights‑dict, avg_completion_time_seconds) using Δ‑objective convergence."""

    # if verbose and not logging.getLogger().handlers:
    #     logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    start_time = time.time()
    collectives = sorted({v["collective"] for v in flow_info.values()})
    groups      = sorted({(v["collective"], v["group"]) for v in flow_info.values()})
    # print(flow_info.values())

    # link  ->  flattened flow list
    link2fids = {e: [fid for fids in link2flows[e].values() for fid in fids]
                 for e in link2flows}
    fid_group = {fid: (v["collective"], v["group"]) for fid, v in flow_info.items()}

    # true average completion time
    def true_avg(wd: Dict[tuple, float]) -> tuple[float, dict]:
        flow_rates = {}
        # sum_w_link = {e: sum(wd[fid_group[fid]] for fid in link2fids[e])
        #                for e in link2fids}
        sum_w_link = {
            e: sum(wd[g]                           # g: (collective, group)
                   for g in {fid_group[f]          # go to group
                              for f in link2fids[e]} )   # remove the redundancy
            for e in link2fids
        }
        # print(f"link to fids: {link2fids}")
        # print(f"sum_w_link: {sum_w_link}")
        # print(f"link_cap :{link_cap}")

        flow_lat = {}
        for fid, info in flow_info.items():
            k, n = fid_group[fid]
            path = info["links"]

            bottleneck_bps = min(link_cap[str(e)] * wd[(k, n)] / max(sum_w_link[e], 1e-12)
                                 for e in path)

            flow_lat[fid] = info["data_size"] * 1024 * 1024 * 8 / bottleneck_bps  # time
            flow_rates[fid] = bottleneck_bps/1024/1024/8 # minimum bandwidth allocated


        # print(flow_rates)
        grp_lat = {(k, n): sum(flow_lat[fid] for fid in flow_info if fid_group[fid] == (k, n))
                    for (k, n) in groups}
        col_lat = {k: max(grp_lat[(k, n)] for n in {g for (kk, g) in groups if kk == k})
                    for k in collectives}
        true_avg = sum(col_lat.values()) / len(collectives)
        return true_avg, flow_rates

    # assign initial weights by some heuristic
    w_val = bottleneck_initial_weights(flow_info, link_cap, link2flows, fid_group, groups)

    # Original initial weights (±5%)
    # w_val = {gn: 1 + 0.05 * random.uniform(-1, 1) for gn in groups}

    S = sum(w_val.values())
    w_val = {g: v / S for g, v in w_val.items()}

    prev_real, flow_rates = true_avg(w_val)
    #if verbose:
     #   logging.info(f"Initial real_avg: {prev_real:.2f} weight: {w_val}")

    for it in range(max_iter):
        # constants with current weights
        # sum_w = {e: sum(w_val[fid_group[fid]] for fid in link2fids[e]) for e in link2fids}
        sum_w = {
            e: sum(w_val[g]
                   for g in {fid_group[f] for f in link2fids[e]})
            for e in link2fids
        }

        # convex sub‑problem
        # Normalization
        w = {gn: cp.Variable(nonneg=True) for gn in groups}
        constraints = [cp.sum(cp.hstack(list(w.values()))) == 1]

        # Integer weights
        # w = {gn: cp.Variable(integer=True, name=f"w_{gn}") for gn in groups}
        # constraints = []
        # for gn in groups:
        #     constraints.append(w[gn] >= 1)

        T = {(k, n): cp.Variable(nonneg=True) for (k, n) in groups}
        U = {k: cp.Variable(nonneg=True) for k in collectives}

        for (k, n) in groups: # in a group, find the slowest time
            fid_sub = [fid for fid in flow_info if fid_group[fid] == (k, n)]
            if fid_sub:
                term_list = []
                for fid in fid_sub:
                    path = flow_info[fid]["links"]
                    # bottleneck link capacity under old weights
                    bottleneck_link = min(path,
                        key = lambda e: link_cap[str(e)] * w_val[(k, n)] / max(sum_w[e], 1e-12))

                    cap_e = link_cap[str(bottleneck_link)]
                    # Flow size/bandwidth = coef * w[(k, n)] = time
                    coef  = flow_info[fid]["data_size"] * 1024 * 1024 * 8 * sum_w[bottleneck_link] / cap_e
                    term_list.append(coef * cp.inv_pos(w[(k, n)]))
                constraints += [T[(k, n)] >= cp.sum(cp.hstack(term_list))]
            else:
                constraints += [T[(k, n)] >= 1e-12]
        # print(groups)
        for k in collectives:
            for n in {g for (kk, g) in groups if kk == k}:
                constraints += [U[k] >= T[(k, n)]]

        approx_obj = cp.sum(cp.hstack(list(U.values()))) / len(collectives)
        objective = cp.Minimize(approx_obj)
        prob = cp.Problem(objective, constraints)
        prob.solve(solver=cp.MOSEK)

        # update weights & compute true objective
        w_val = {gn: float(w[gn].value) for gn in groups}
        real_avg, flow_rates = true_avg(w_val)
        if verbose:
            logging.info(f"Iter {it}, approx_obj: {approx_obj.value:.2f}, real_avg:{real_avg:.2f}")
                # , weights: {w_val}")

        if abs(real_avg - prev_real) < tol_obj:
            break
        prev_real = real_avg
    end_time = time.time()
    time_cost = end_time - start_time
    #logging.info(f"Converged in {it + 1} iterations, real_avg: {prev_real}")
    weights_out = {f"{k}_{n}": v for (k, n), v in w_val.items()}
    #logging.info(f"Solver: {prob.solver_stats.solver_name}, Solver status: {prob.status}, weights: {weights_out}, objective value: {prev_real}")
    logging.info(f"objective value: {prev_real}")
    logging.info(f"time cost: {time_cost}")

    return weights_out, flow_rates, time_cost, prev_real











# ###### Objective Convergence #####
# import logging, time, random
# import cvxpy as cp
# from typing import Dict, Tuple
# import numpy as np
# random.seed(42)
# np.random.seed(42)

# #  each group (collective k, group n) has an integer weight w_{k,n}  (w >= 1)
# #  outer loop: freeze old weights, linearise F_i / b_i, solve mixed-integer convex sub‑problem
# #  convergence when |obj^{t} − obj^{t−1}| < tol_obj  (true objective)

# def weight_allocation_optimization(
#     flow_info: Dict[str, dict],            # flow meta
#     link_cap: Dict[str, float],            # link capacity
#     fid_to_order: dict,
#     link2flows: dict,                      # {link: {group: [fid,…]}}
#     max_iter: int = 50,
#     tol_obj: float = 1e-3,
#     verbose: bool = False,
# ) -> Tuple[dict, float]:
#     """Return (integer_weights‑dict, avg_completion_time_seconds)."""

#     start_time = time.time()
#     collectives = sorted({v["collective"] for v in flow_info.values()})
#     groups      = sorted({(v["collective"], v["group"]) for v in flow_info.values()})

#     # link  ->  list of fids
#     link2fids = {e: [fid for fids in link2flows[e].values() for fid in fids]
#                  for e in link2flows}
#     fid_group = {fid: (v["collective"], v["group"]) for fid, v in flow_info.items()}

#     def true_avg(wd: Dict[tuple, float]) -> float:
#         sum_w_link = {e: sum(wd[fid_group[fid]] for fid in link2fids[e])
#                        for e in link2fids}
#         flow_lat = {}
#         for fid, info in flow_info.items():
#             k, n = fid_group[fid]
#             path = info["links"]
#             bottleneck_bps = min(
#                 link_cap[str(e)] * wd[(k, n)] / max(sum_w_link[e], 1e-12)
#                 for e in path)
#             flow_lat[fid] = info["data_size"] * 1024 * 1024 * 8 / bottleneck_bps
#         grp_lat = {(k, n): sum(flow_lat[fid] for fid in flow_info if fid_group[fid] == (k, n))
#                     for (k, n) in groups}
#         col_lat = {k: max(grp_lat[(k, n)] for n in {g for (kk, g) in groups if kk == k})
#                     for k in collectives}
#         return sum(col_lat.values()) / len(collectives)

#     # initial integer weights >=1 (no normalization)
#     w_val = {gn: 1 for gn in groups}
#     prev_real = true_avg(w_val)
#     if verbose:
#         logging.info(f"Iter -1  real_avg: {prev_real:.2f}  weights: {w_val} (initial)")

#     for it in range(max_iter):
#         sum_w = {e: sum(w_val[fid_group[fid]] for fid in link2fids[e]) for e in link2fids}

#         # mixed-integer convex sub‑problem
#         w = {gn: cp.Variable(integer=True, name=f"w_{gn}") for gn in groups}
#         constraints = [w[gn] >= 1 for gn in groups]
#         # keep integer weights scaled: sum == number of groups (no normalization to 1)
#         constraints.append(cp.sum(cp.hstack(list(w.values()))) == 1500)
#         T = {(k, n): cp.Variable(nonneg=True) for (k, n) in groups}
#         U = {k: cp.Variable(nonneg=True) for k in collectives}

#         for (k, n) in groups:
#             fid_sub = [fid for fid in flow_info if fid_group[fid] == (k, n)]
#             if fid_sub:
#                 term_list = []
#                 for fid in fid_sub:
#                     path = flow_info[fid]["links"]
#                     bottleneck_link = min(path,
#                         key=lambda e: link_cap[str(e)] * w_val[(k, n)] / max(sum_w[e], 1e-12))
#                     cap_e = link_cap[str(bottleneck_link)]
#                     coef = (flow_info[fid]["data_size"] * 1024 * 1024 * 8
#                             * sum_w[bottleneck_link] / cap_e)
#                     term_list.append(coef * cp.inv_pos(w[(k, n)]))
#                 constraints.append(T[(k, n)] >= cp.sum(cp.hstack(term_list)))
#             else:
#                 constraints.append(T[(k, n)] >= 1e-12)

#         for k in collectives:
#             for n in {g for (kk, g) in groups if kk == k}:
#                 constraints.append(U[k] >= T[(k, n)])

#         approx_obj = cp.sum(cp.hstack(list(U.values()))) / len(collectives)
#         prob = cp.Problem(cp.Minimize(approx_obj), constraints)
#         prob.solve(solver=cp.ECOS_BB)

#         # update integer weights & compute true objective
#         w_val = {gn: int(round(w[gn].value)) for gn in groups}
#         real_avg = true_avg(w_val)
#         if verbose:
#             logging.info(
#                 f"Iter {it:2d}  approx_obj: {approx_obj.value:.2f}  real_avg: {real_avg:.2f}  weights: {w_val}"
#             )

#         if abs(real_avg - prev_real) < tol_obj:
#             break
#         prev_real = real_avg

#     logging.info(
#         f"Converged in {it+1} iterations  real_avg: {prev_real:.2f}  time: {time.time()-start_time:.2f}s"
#     )
#     weights_out = {f"{k}_{n}": v for (k, n), v in w_val.items()}
#     return weights_out, prev_real
