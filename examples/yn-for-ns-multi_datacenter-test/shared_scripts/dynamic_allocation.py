import os
import math
import logging
from typing import List
import numpy as np
import cvxpy as cp
import operator
from collections import defaultdict
import time
#print(cp.installed_solvers())

def dynamic_allocation_optimization(flow_info, link_cap, fid_to_order_dict, flows_for_each_link):
    start_time = time.time()
    logging.info('**** Start Dynamic Allocation Optimization ****')
    last_key = next(reversed(flow_info))
    K = flow_info[last_key]['collective']

    # Create Variables F(k,n,o), b(k,n,o)
    F = {}
    b = {}
    collectives = set(flow['collective'] for flow in flow_info.values())
    groups = set(flow['group'] for flow in flow_info.values())
    helper_groups = {n: cp.Variable(nonneg=True, name=f"n_{n}") for n in groups}
    helper_collective = {k: cp.Variable(nonneg=True, name=f"k_{k}") for k in collectives}
    constraints = []
    cur_n = 1
    for flow_id, flow in flow_info.items():
        k = flow['collective']
        n = flow['group']
        order = fid_to_order_dict[flow_id][0]
        F[(flow_id, k, n, order)] = flow['data_size']
        b[(flow_id, k, n, order)] = cp.Variable(pos = True, name = f"b_fid_{flow_id}_k{k}_n{n}_o{order}")
        constraints.append(b[(flow_id, k, n, order)] >= 1e-6)

    # Helper Constraints
    # for every flow group n：∑[F_i / b_i] <= helper[n]
    collective_terms = {}
    for k in collectives:
        for n in groups:
            # collect all the flows in group n
            group_terms = []
            for flow_id, flow in flow_info.items():
                if flow['group'] == n and flow['collective'] == k:
                    order = fid_to_order_dict[flow_id][0]
                    #  cp.inv_pos(b): 1/b
                    group_terms.append( F[(flow_id, k, n, order)] * cp.inv_pos(b[(flow_id, k, n, order)]) )
            # Add Constraint：∑[F_i / b_i] <= helper[n]
            nth_group_sum = cp.sum(group_terms) # sum of the time of Kth collective, Nth group
            # constraints.append( group_sum <= helper_groups[n] )
            # print(f"k is {k}, n is {n}, group_sum is {nth_group_sum}")
            collective_terms.setdefault(k, []).append(nth_group_sum)
            # print(f"now: {collective_terms}")
        # constraints.append( cp.sum(collective_terms[k]) <= helper_collective[k] )

    # Link Capacity Constraints
    # For each link, create an auxiliary variable for each group on that link.
    for link_id, groups_dict in flows_for_each_link.items():
        link_capacity = link_cap[str(link_id)] / 1024 / 1024 / 8
        effective_usage_vars = []
        for group, flow_ids in groups_dict.items():
            # Create an auxiliary variable for this (link, group)
            E = cp.Variable(pos=True, name=f"E_link{link_id}_group{group}")
            # For each flow in the same group, force E to be at least its allocated bandwidth.
            for flow_id in flow_ids:
                k = flow_info[flow_id]['collective']
                n = flow_info[flow_id]['group']
                order = fid_to_order_dict[flow_id][0]
                constraints.append(E >= b[(flow_id, k, n, order)])
            effective_usage_vars.append(E)
        # The sum of the effective usages from all groups on this link must be within the link capacity.
        constraints.append(cp.sum(effective_usage_vars) <= link_capacity)


    # # Link Capacity Constraints
    # for link_id, flows in flows_for_each_link.items():
    #     link_capacity = link_cap[str(link_id)]/1024/1024/8
    #     sum = 0
    #     for flow_id in flows:
    #         k = flow_info[flow_id]['collective']
    #         n = flow_info[flow_id]['group']
    #         order = fid_to_order_dict[flow_id][0]
    #         sum += b[(k, n, order)]
    #     constraints.append(sum <= link_capacity)

    # Create the objective function
    # objective = cp.Minimize(cp.sum([helper_collective[key] for key in helper_collective]))
    # print(collective_terms)
    # objective = cp.Minimize(cp.sum([cp.max(collective_terms[k]) for k in collective_terms]))
    objective = cp.Minimize(cp.sum([cp.max(cp.hstack(collective_terms[k])) for k in collective_terms]))

    # Solve
    prob = cp.Problem(objective, constraints)
    logging.info("-----> Building MILP for chunk-based scheduling done, start solving...")
    #solver_name = opt_config.get("solver", "HIGHS")  # can change to "CBC"or "GLPK_MI"(?
    # prob.solve(solver=solver_name)
    # prob.solve(qcp=True, solver=cp.SCS, verbose=True)
    prob.solve(qcp=True, solver=cp.MOSEK, verbose=True)
    # print("Solver used:", prob.solver_stats.solver_name)
    if prob.status == cp.OPTIMAL:
        print("\n========= Var Values =========")
    end_time = time.time()
    time_cost = end_time - start_time
    for (flow_id, k, n, o), var in b.items():
        print(f"b(flow_id={flow_id}, k={k}, n={n}, order={o}), bandwidth allocated: {var.value:.1f}, total time: {F[(flow_id, k, n, o)]/var.value}")

    # After solving the problem, print the computed time for each collective.

    for k in collective_terms:
        # Combine the list of expressions into one single expression.
        term_expr = cp.max(cp.hstack(collective_terms[k]))
        print(f"Collective {k} max time: {term_expr.value}")

    objective_value = prob.value
    all_vars = prob.variables() # 10 links (E: links in use) * 6 groups + 72 flows = 132 vars for Napnet_1-RAR
    logging.info(f"\n[CVXPY] Number of variables = {len(all_vars)}")

    logging.info(f"Solver: {prob.solver_stats.solver_name}, Solver status: {prob.status}, objective value: {objective_value/K}, time cost: {time_cost}")
    flow_rates = {
        flow_id: b[(flow_id, flow_info[flow_id]['collective'], flow_info[flow_id]['group'], fid_to_order_dict[flow_id][0])].value
        for flow_id in flow_info
    }
    return {"avg_completion_time":objective_value, "time_cost":time_cost, "flow_rate":flow_rates}
