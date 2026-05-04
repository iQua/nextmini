"""
Implementation of the optimization of the Theorem 1 of the paper.
"""

import os
import logging
from typing import List
import math

import pulp

from generic import (
    BaseContainer,
    FlowLinkSendHolder,
    CollectiveGroupContainer,
    FlowCGHolder,
)
from utils import save_results, save_dict_variables, save_constraints
from opt_utils import v_kne


def knl_to_nested(variables, K: int, Nks: int, L: int):
    """Convert the list with knl to a nested list."""
    nested = list()
    for k in range(K):
        k_group = list()
        for n in range(Nks[k]):
            k_group.append(([variables[(k, n, l)].varValue for l in range(L)]))
        nested.append(k_group)
    return nested


def compute_link_throughput(
    K: int, Nks: List[int], E: int, fl_s_holder, fcg_holder, cg_container
):
    """Compute the throughput of the flow groups."""
    return {
        (k, n, e): v_kne(
            k,
            n,
            e,
            fl_s_holder.data_matrix,
            fcg_holder.matrix,
            cg_container,
        )
        for k in range(K)
        for n in range(Nks[k])
        for e in range(E)
    }


def optimize_completion_time(
    link_container: BaseContainer,
    fl_s_holder: FlowLinkSendHolder,
    cg_container: CollectiveGroupContainer,
    fcg_holder: FlowCGHolder,
    opt_parameters: dict,
):
    """
    Optimizing the completion times of the flow groups.

    See Theorem 1 of the paper.

    See the `perform_steller` of the steller.py for details of the arguments.

    :param opt_parameters: A dict containing the parameters required to perform
     the optimization. It has:
     - T: the upper bound of the time slot
     - is_segment: whether use the corollary 1 of the paper to create the time interval.

    Note that N = sum_{k}sum_{n} N^k.

    :return big_S: A list, each item is a tuple, containing the time ranges used to build the optimization problem.
    :return big_C: The optimized completion times of groups.
    :return lambda0:
    """

    # Set the save path
    save_path = os.path.join(opt_parameters["model_path"], "PriorityOptimization")
    os.makedirs(save_path, exist_ok=True)

    # Get the basic numbers
    K = cg_container.K
    Nks = cg_container.Nks
    N = cg_container.N
    F = fl_s_holder.fl_holder.F
    E = fl_s_holder.fl_holder.E

    logging.info("%s %s %s", "*" * 15, "Optimizing Completion Times", "*" * 15)
    if len(Nks) < 50:
        Nks_str = f"{Nks}" if len(Nks) < 30 else f"{Nks[:30]}..."
    logging.info(
        "-----> K: %s, Nks: %s, N: %s, E: %s ",
        K,
        Nks_str,
        N,
        E,
    )

    logging.info(
        "-----> %s",
        "Extract Optimization Parameters",
    )
    T = opt_parameters["T"]
    segment_base = opt_parameters["segment_base"]
    is_segment = opt_parameters["is_segment"]

    # Obtain the time intervals based on corollary 1 of the paper
    # Here T + 1 makes the last interval to be [T, T+1) that is slightly
    # larger than the T.
    if is_segment == "True" or is_segment == "true" or is_segment == True:

        num_intervals = int(math.log(T, segment_base) + 1)

        big_S = [(0, 1)]
        big_S.extend(
            [
                (segment_base ** (i - 1), segment_base ** (i))
                for i in range(1, num_intervals + 1)
            ]
        )
    else:
        big_S = [(t, t + 1) for t in range(T)]
    L = len(big_S)
    save_results(os.path.join(save_path, "big_S.json"), ["big_S"], [big_S])
    big_S_str = f"{big_S}" if len(big_S) < 10 else f"{big_S[:5]}->{big_S[-1]}..."
    logging.info(
        "-----> Build %s-based #%s Time Ranges: %s", segment_base, L, big_S_str
    )

    logging.info("%s Start building the LP Optimization (OP)", "*" * 15)

    # Compute the link throughput
    link_throughput = compute_link_throughput(
        K, Nks, E, fl_s_holder, fcg_holder, cg_container
    )

    # Define LP problem based on Theorem 1 of the paper
    prob = pulp.LpProblem("Completion Time Minimization Model", pulp.LpMinimize)

    # Define decision variables based on Eq 12 and 14 of the paper
    variables = [(k, n, l) for k in range(K) for n in range(Nks[k]) for l in range(L)]
    c_variables = pulp.LpVariable.dicts(
        "C",
        variables,
        lowBound=0,
        cat=pulp.const.LpBinary,
    )
    lambda0_variables = pulp.LpVariable.dicts(
        "Lambda0",
        variables,
        lowBound=0,
        cat=pulp.const.LpBinary,
    )
    lambda1_variables = pulp.LpVariable.dicts(
        "Lambda1",
        variables,
        lowBound=0,
        cat=pulp.const.LpBinary,
    )
    logging.info(
        "-----> Defined vars (Lemma 2). C: #%s, Lambda0: #%s, Lambda1: #%s",
        len(c_variables),
        len(lambda0_variables),
        len(lambda1_variables),
    )

    # Objective function, Eq. 12 of the paper
    prob += (
        pulp.lpDot(
            pulp.lpSum(
                [
                    lambda0_variables[(k, n, l)]
                    + (N ** big_S[l][0]) * lambda1_variables[(k, n, l)]
                    for k in range(K)
                    for n in range(Nks[k])
                    for l in range(L)
                ]
            ),
            1 / K,
        ),
        "Objective",
    )
    logging.info(
        "-----> Set Objective.",
    )

    # Add constraints, Eq 13 of the paper
    for k in range(K):
        for n in range(Nks[k]):
            for l in range(L):
                prob += (
                    c_variables[(k, n, l)] == lambda1_variables[(k, n, l)],
                    f"c_lambda0_constraint_[{k}, {n}, {l}]",
                )
                prob += (
                    lambda0_variables[(k, n, l)] + lambda1_variables[(k, n, l)] == 1,
                    f"c_lambda1_constraint_[{k}, {n}, {l}]",
                )
    logging.info(
        "-----> Defined c-lambda constraints. (Eq. 13): #%s",
        N * L,
    )

    # Add constraints, Eq 9 of the paper
    for k in range(K):
        for n in range(Nks[k]):
            prob += (
                pulp.lpSum([c_variables[(k, n, l)] for l in range(L)]) == 1,
                f"c_constraint_({k}, {n})",
            )
    logging.info(
        "-----> Defined single-completion-time constraints (Eq. 9): #%s",
        N,
    )

    # Add constraints, Eq 8 or Eq 10 of the paper
    # Using Eq 8 or Eq 10 depends on the is_segment
    # Here will be the number of EL constraints
    # Note that in the code, the l and u should start from 0 to access
    # the corresponding values from the vector or matrix
    # However, to maintain consistency with the paper, we allows the l and
    # u start from 1. Yet, in the real value access, we minus 1 from them to
    # access the correct value.

    for l in range(1, L + 1):
        for e in range(E):
            if sum(fl_s_holder.data_matrix[:, e]) != 0:
                prob += (
                    pulp.lpSum(
                        [
                            c_variables[(k, n, u - 1)] * link_throughput[(k, n, e)]
                            for k in range(K)
                            for n in range(Nks[k])
                            for u in range(1, l + 1)
                        ]
                    )
                    <= big_S[l - 1][1] * link_container.item_obj(e).capacity,
                    f"load_constraint_({l-1}, {e})",
                )
    logging.info(
        "-----> Defined load constraints. (Eq. 8/10): #%s",
        L * E,
    )

    # Solve problem
    logging.info(
        "%s Start solving the LP Optimization (OP) with pulp",
        "*" * 15,
    )
    # The problem data is written to an .lp file
    prob.writeLP(os.path.join(save_path, "OptimizationModel.lp"))
    prob.solve()
    logging.info(
        "%s Solved the LP Optimization (OP) with pulp",
        "*" * 15,
    )
    logging.info("-----> Solved with status: %s.", pulp.LpStatus[prob.status])
    logging.info("-----> Solved with objective: %s.", pulp.value(prob.objective))

    save_constraints(os.path.join(save_path, "constraints.json"), prob.constraints)
    logging.info("!----> Saved constraints.")

    save_dict_variables(os.path.join(save_path, "C_variables.json"), c_variables)
    save_dict_variables(
        os.path.join(save_path, "lambda0_variables.json"), lambda0_variables
    )
    save_dict_variables(
        os.path.join(save_path, "lambda1_variables.json"), lambda1_variables
    )
    logging.info("!----> Saved variables.")

    # Convert the flatten variables to a nested list that are
    # easy to be accessed by using k, n, l
    # Extract the optimized variables

    big_C = knl_to_nested(c_variables, K, Nks, L)
    lambda0 = knl_to_nested(lambda0_variables, K, Nks, L)
    lambda1 = knl_to_nested(lambda1_variables, K, Nks, L)

    save_results(
        os.path.join(save_path, "kn_C_variables.json"),
        [f"collective {k}" for k in range(1, K + 1)],
        big_C,
    )
    save_results(
        os.path.join(save_path, "kn_lambda0_variables.json"),
        [f"collective {k}" for k in range(1, K + 1)],
        lambda0,
    )
    save_results(
        os.path.join(save_path, "kn_lambda1_variables.json"),
        [f"collective {k}" for k in range(1, K + 1)],
        lambda1,
    )
    logging.info("!----> Saved formatted variables.")

    # Convert the big_C from R^+ to the specific completion time
    # We use the maximum value of big_C' each group to determine the range
    big_C_S = list()
    for k in range(K):
        big_C_S.append([big_C[k][n].index(max(big_C[k][n])) for n in range(Nks[k])])
    save_results(
        os.path.join(save_path, "kn_big_C_S.json"),
        [f"collective {k}" for k in range(1, K + 1)],
        big_C_S,
    )
    logging.info("!----> Saved optimized intervals.")
    # Compute the completion times of flow groups
    big_tau = list()
    for k in range(K):
        group_intervals = big_C_S[k]
        taus = [big_S[group_intervals[n]][1] for n in range(Nks[k])]
        big_tau.append(taus)

    save_results(
        os.path.join(save_path, "big_tau.json"),
        [f"collective {k}" for k in range(1, K + 1)],
        big_tau,
    )
    logging.info("!----> Saved optimized big tau.")
    # constraints = prob.constraints
    # print(f"The constraints are held in a {type(constraints)}")

    # for name in constraints.keys():
    #     value = constraints.get(name).value()
    #     slack = constraints.get(name).slack
    #     print(f"constraint {name} has value: {value:0.2e} and slack: {slack:0.2e}")

    return (
        big_S,
        (big_C, lambda0, lambda1),
        big_tau,
    )
