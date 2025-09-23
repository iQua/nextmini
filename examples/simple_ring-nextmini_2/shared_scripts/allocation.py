"""
Implementation of the allocation optimization, referred to as
LP Optimization problem (OR_l), derived from Theorem 2 of the paper.
"""

import os
import logging
from typing import List
import numpy as np
import cvxpy as cp


from generic import (
    BaseContainer,
    FlowLinkSendHolder,
    CollectiveGroupContainer,
    FlowCGHolder,
)
from utils import save_alloc_solutions
from opt_utils import get_group_flows, compute_average_completion_time


def get_link_groups(
    e,
    fl_s_holder: FlowLinkSendHolder,
    fcg_holder: FlowCGHolder,
    cg_container: CollectiveGroupContainer,
    K,
    Nks,
):
    """Get the groups that pass the link."""
    return [
        (k, n)
        for k in range(K)
        for n in range(Nks[k])
        if fl_s_holder.fl_holder.matrix[
            get_group_flows(k, n, fcg_holder.matrix, cg_container)
        ][:, e].any()
    ]


def optimize_flow_rates(
    flow_container: BaseContainer,
    big_tau: List[List[int]],
    link_container: BaseContainer,
    fl_s_holder: FlowLinkSendHolder,
    cg_container: CollectiveGroupContainer,
    fcg_holder: FlowCGHolder,
    opt_parameters: dict,
):
    """
    Optimizing the flow rates of groups to minimize the average completion time of collective.

    See (OR_l) in subsection 3-C of the paper.
    """
    # Set the save path
    save_path = os.path.join(opt_parameters["model_path"], "AllocationOptimization")
    os.makedirs(save_path, exist_ok=True)

    # Get the basic numbers
    K = cg_container.K
    Nks = cg_container.Nks
    N = cg_container.N
    F = fl_s_holder.fl_holder.F
    E = fl_s_holder.fl_holder.E

    logging.info("%s %s %s", "*" * 15, "Optimizing Group Flow Rates", "*" * 15)

    logging.info(
        "-----> %s",
        "Extract Optimization Parameters",
    )

    logging.info("%s Start addressing the LP Optimization (OR_l)", "*" * 15)

    # Note: Here we do not use the pulp to address the optimization problem, instead we implement the our own solver which reaches the same solution as that discussed in the (OR_l) in subsection3-C of the paper.
    # In the first step, we need to compute the initial flow rates of groups
    # Step 1. Compute the initial flow rates of groups
    logging.info("-----> Step1. Computing initial flow rates:")
    flow_datas = np.array([flow.data_volume for flow in flow_container.item_objs])
    op_big_R = list()
    # We visit each group
    for k in range(K):
        big_R_k = list()
        for n in range(Nks[k]):
            group_flows = get_group_flows(k, n, fcg_holder.matrix, cg_container)

            group_flow_datas = flow_datas[group_flows]
            group_tau = big_tau[k][n]
            big_R_k.append(sum(group_flow_datas) / group_tau)

        op_big_R.append(big_R_k)

    logging.info("-----> Step2. Visiting all links to adjust the flow rates:")
    adjusted_big_R = op_big_R.copy()
    for e in range(E):
        # all groups that pass the link
        link_groups = get_link_groups(e, fl_s_holder, fcg_holder, cg_container, K, Nks)
        # We ensure that they do not exceed the link capacity, based on
        # the Eq. 15 of the paper.
        lg_rates = np.array([op_big_R[k][n] for k, n in link_groups])
        capacity = link_container.item_obj(e).capacity
        if sum(lg_rates) > capacity:
            # We adjust them so that they equal the link capacity while maintaining the ratio
            lg_rates = lg_rates * capacity / sum(lg_rates)
        for idx, (k, n) in enumerate(link_groups):
            adjusted_big_R[k][n] = lg_rates[idx]

    # Step 3. Based on the Theorem 2, the OR_l, Eq. 21 and Eq. 22 of the paper
    # We start from the lowest flow rate and slightly adds the lambda to it until the constraint is satisfied
    logging.info("-----> Step3. Extending the flow rates with lambda:")
    small_lambda = opt_parameters["small_lambda"]
    jump_range = opt_parameters["jump_range"]
    total_lambda = small_lambda * jump_range

    # We jump through each group rate on both size with step size of small_lambda
    # Step 3.1, we flat the kn groups to be a list
    extened_rates = np.column_stack(
        [
            np.arange(
                adjusted_big_R[k][n] - total_lambda,
                adjusted_big_R[k][n] + total_lambda,
                small_lambda,
            )
            for k in range(K)
            for n in range(Nks[k])
        ]
    )
    n_cols = extened_rates.shape[1]

    cases = [extened_rates[:, i].tolist() for i in range(n_cols)]
    available_cases = np.array(np.meshgrid(*cases)).T.reshape(-1, n_cols)

    # Step 4. Based on Eq. 21 and Eq. 22, we filter out those that exceed the link capacity
    logging.info("-----> Step 4. Extracting the feasible ones and computing:")
    solutions = list()
    for case_i in available_cases:
        big_kn_R = np.array_split(case_i, Nks[:-1])

        is_valid = True
        for e in range(E):
            link_groups = get_link_groups(
                e, fl_s_holder, fcg_holder, cg_container, K, Nks
            )
            # We ensure that they do not exceed the link capacity, based on
            # the Eq. 15 of the paper.
            lg_rates = np.array([big_kn_R[k][n] for k, n in link_groups])
            capacity = link_container.item_obj(e).capacity
            if sum(lg_rates) > capacity:
                is_valid = False
                break
        if is_valid:
            sol_time = compute_average_completion_time(
                big_kn_R, fcg_holder, cg_container, flow_datas, K, Nks
            )
            solutions.append((big_kn_R, sol_time))

    # Step 6. Extracting the optimal solution, i.e., the one with the minimal
    # average completion time
    # Get both the time and the rates
    logging.info(
        "%s Solved the Optimization (OR) with our own solver.",
        "*" * 15,
    )
    optimal_solution = min(solutions, key=lambda x: x[1])
    logging.info("-----> Optimal flows: %s.", optimal_solution[0])
    logging.info("-----> Optimal objective: %s.", optimal_solution[1])
    # Save the solutions:
    # Sort the solutions based on the completion time from low to high
    solutions.sort(key=lambda x: x[1])
    save_alloc_solutions(
        os.path.join(save_path, "optimized_flow_rates.json"),
        solutions,
    )

    return optimal_solution, op_big_R, adjusted_big_R


def optimize_lp_flow_rates(
    flow_container: BaseContainer,
    big_tau: List[List[int]],
    link_container: BaseContainer,
    fl_s_holder: FlowLinkSendHolder,
    cg_container: CollectiveGroupContainer,
    fcg_holder: FlowCGHolder,
    opt_parameters: dict,
):
    """
    Optimizing the flow rates of groups to minimize the average completion time of collective based on the LP of the pulp.

    See (OR_l) in subsection 3-C of the paper.
    """
    # Set the save path
    save_path = os.path.join(opt_parameters["model_path"], "AllocationOptimization")
    os.makedirs(save_path, exist_ok=True)

    # Get the basic numbers
    K = cg_container.K
    Nks = cg_container.Nks
    N = cg_container.N
    F = fl_s_holder.fl_holder.F
    E = fl_s_holder.fl_holder.E

    logging.info("%s %s %s", "*" * 15, "Optimizing Group Flow Rates", "*" * 15)

    logging.info(
        "-----> %s",
        "Extract Optimization Parameters",
    )

    logging.info("%s Start addressing the LP Optimization (OR_l)", "*" * 15)

    # Note: Here we do not use the pulp to address the optimization problem, instead we implement the our own solver which reaches the same solution as that discussed in the (OR_l) in subsection3-C of the paper.
    # In the first step, we need to compute the initial flow rates of groups
    # Step 1. Compute the initial flow rates of groups
    logging.info("-----> Step1. Computing initial flow rates:")
    flow_datas = np.array([flow.data_volume for flow in flow_container.item_objs])
    op_big_R = list()
    # We visit each group
    for k in range(K):
        op_big_R_k = list()
        for n in range(Nks[k]):
            group_flows = get_group_flows(k, n, fcg_holder.matrix, cg_container)

            group_flow_datas = flow_datas[group_flows]
            group_tau = big_tau[k][n]
            op_big_R_k.append(sum(group_flow_datas) / group_tau)

        op_big_R.append(op_big_R_k)

    logging.info("-----> Step2. Building the LP for flow rates:")
    small_lambda = opt_parameters["small_lambda"]
    jump_range = opt_parameters["jump_range"]
    total_lambda = float(small_lambda * jump_range)

    # Define decision variables based on Eq 12 and 14 of the paper
    k_groups_data = list()
    for k in range(K):
        k_groups_data.append(
            np.array(
                [
                    sum(
                        flow_datas[
                            get_group_flows(k, n, fcg_holder.matrix, cg_container)
                        ]
                    )
                    for n in range(Nks[k])
                ]
            )
        )

    r_variables = [cp.Variable(Nks[k], f"C-{k}") for k in range(K)]
    r_lower = [np.array(op_big_R[k]) - total_lambda for k in range(K)]
    r_upper = [np.array(op_big_R[k]) + total_lambda for k in range(K)]

    logging.info(
        "-----> Defined vars (Theorem 2). R: #%s,",
        sum(Nks),
    )

    # Objective function, Eq. 12 of the paper
    objective = cp.Minimize(
        cp.sum(
            [cp.max(cp.inv_pos(r_variables[k]) * k_groups_data[k]) for k in range(K)]
        )
        * (1.0 / K)
    )

    logging.info(
        "-----> Set Objective.",
    )
    constraints = list()
    # Add constraints, Eq 9 of the paper
    # r_variables[k] <= np.ones_like(r_variables[k]),
    # r_variables[k] >= np.zeros_like(r_variables[k])
    for k in range(K):
        constraints.append(r_variables[k] >= 0)
        # constraints.append(r_variables[k] >= r_lower[k])
        constraints.append(
            r_variables[k] <= r_upper[k],
        )
    logging.info(
        "-----> Defined the range constraints (Eq. 12/13): #%s",
        N,
    )

    for e in range(E):
        # The groups that pass the link e
        link_groups = get_link_groups(e, fl_s_holder, fcg_holder, cg_container, K, Nks)
        total_occupy = cp.sum([r_variables[k][n] for k, n in link_groups])
        link_capacity = link_container.item_obj(e).capacity
        constraints.append(total_occupy <= link_capacity)

    logging.info(
        "-----> Defined the link capacity constrains: #%s",
        E,
    )

    # Define LP problem based on Theorem 1 of the paper
    prob = cp.Problem(objective, constraints)

    # Solve problem
    logging.info(
        "%s Start solving the LP Optimization (OR) with cvxpy",
        "*" * 15,
    )
    # The problem data is written to an .lp file
    # prob.writeLP(os.path.join(save_path, "OptimizationModel.lp"))
    prob.solve(solver=cp.MOSEK)
    print("Solver used:", prob.solver_stats.solver_name)
    logging.info(
        "%s Solved the LP Optimization (OR) with cvxpy",
        "*" * 15,
    )
    optimized_big_R = list()
    for k in range(K):
        optimized_big_R.append(r_variables[k].value)

    optimal_obj = compute_average_completion_time(
        optimized_big_R, fcg_holder, cg_container, flow_datas, K, Nks
    )
    optimal_solution = (optimized_big_R, optimal_obj)
    save_alloc_solutions(
        os.path.join(save_path, "optimized_flow_rates.json"),
        [optimal_solution],
    )
    logging.info("-----> Optimal flows: %s.", optimal_solution[0])
    logging.info("-----> Optimal objective: %s.", optimal_solution[1])

    # As the ablation study, we also save the flow rates and obj of the OP
    op_big_R_obj = compute_average_completion_time(
        op_big_R, fcg_holder, cg_container, flow_datas, K, Nks
    )
    ablation_solution = (op_big_R, op_big_R_obj)
    save_alloc_solutions(
        os.path.join(save_path, "ablation_flow_rates.json"),
        [ablation_solution],
    )

    return optimal_solution, op_big_R, None
