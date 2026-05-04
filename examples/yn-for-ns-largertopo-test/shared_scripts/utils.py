"""
Some utility functions for the stellar example.
"""

import json
from typing import List


def save_variables(save_path, variables):
    """Saving the optimization variables to a file."""
    data = {var.name: var.varValue for var in variables}
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def save_dict_variables(save_path, variables):
    """Saving the optimization variables to a file."""
    data = {value.name: value.varValue for key, value in variables.items()}
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def save_results(save_path, result_names, results):
    """Save the results to a file."""
    data = dict(zip(result_names, results))
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def save_constraints(save_path, constraints):
    """Save the constraints."""
    # print(constraints)
    data = {
        name: {
            "content": str(constraints.get(name)),
            "value": constraints.get(name).value(),
            "slack": constraints.get(name).slack,
        }
        for name in constraints.keys()
    }
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def save_alloc_solutions(save_path, solutions: List[tuple]):
    """Save the allocation solutions."""
    solutions = [str(sol) for sol in solutions]
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(solutions, f, indent=4)
