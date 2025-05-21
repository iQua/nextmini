import networkx as nx
from gurobipy import *

def solve_max_min_fairness_throughput(capacities, demands):
    # Create a new model
    model = Model("MaxMinFairnessMultiCommodityFlow")
    model.params.LogToConsole = 0

    # Get the number of nodes and commodities
    num_nodes = len(capacities)
    num_commodities = len(demands)

    # Create variables
    flows = {}
    for k in range(num_commodities):
        for i in range(num_nodes):
            for j in range(num_nodes):
                flows[(k, i, j)] = model.addVar(lb=0, vtype=GRB.CONTINUOUS, name=f"Flow_{k}_{i}_{j}")

    # Relaxation factors for each demand
    relaxation_factors = {}
    for k in range(num_commodities):
        relaxation_factors[k] = model.addVar(lb=0, ub=1, vtype=GRB.CONTINUOUS, name=f"RelaxationFactor_{k}")

    # Define the minimum throughput as the objective function
    min_throughput = model.addVar(lb=0, vtype=GRB.CONTINUOUS, name="MinThroughput")
    model.setObjective(min_throughput, GRB.MAXIMIZE)

    # Constraints: capacity constraints and flow conservation constraints
    for i in range(num_nodes):
        for j in range(num_nodes):
            # Capacity constraint for each link (i, j)
            model.addConstr(quicksum(flows[(k, i, j)] for k in range(num_commodities)) <= capacities[i][j])

    for k in range(num_commodities):
        # Flow conservation constraint for each commodity
        for i in range(num_nodes):
            model.addConstr(quicksum(flows[(k, j, i)] for j in range(num_nodes)) - quicksum(flows[(k, i, j)] for j in range(num_nodes)) == -demands[k][i]*relaxation_factors[k])

            # Update the minimum throughput variable as the minimum flow among commodities
            if demands[k][i] < 0:
                model.addConstr(min_throughput <= quicksum(flows[(k, j, i)] for j in range(num_nodes)))

    # Set Gurobi parameters

    # Solve the problem
    model.optimize()

    # Retrieve the optimal flow values
    optimal_flows = {}
    for k in range(num_commodities):
        for i in range(num_nodes):
            for j in range(num_nodes):
                optimal_flows[(k, i, j)] = flows[(k, i, j)].x

    # Retrieve the optimal relaxation factors
    optimal_relaxation_factors = {}
    for k in range(num_commodities):
        optimal_relaxation_factors[k] = relaxation_factors[k].x

    return min_throughput.x, optimal_flows, optimal_relaxation_factors


def solve_max_min_fairness_throughput_overlay(capacities, mapping, demands):
    # Create a new model
    model = Model("MaxMinFairnessMultiCommodityFlow")
    model.params.LogToConsole = 0

    # Get the number of nodes and commodities
    num_nodes = len(capacities)
    num_commodities = len(demands)

    # Create variables
    flows = {}
    for k in range(num_commodities):
        for i in range(num_nodes):
            for j in range(num_nodes):
                flows[(k, i, j)] = model.addVar(lb=0, vtype=GRB.CONTINUOUS, name=f"Flow_{k}_{i}_{j}")

    # Relaxation factors for each demand
    relaxation_factors = {}
    for k in range(num_commodities):
        relaxation_factors[k] = model.addVar(lb=0, ub=1, vtype=GRB.CONTINUOUS, name=f"RelaxationFactor_{k}")

    # Define the minimum throughput as the objective function
    min_throughput = model.addVar(lb=0, vtype=GRB.CONTINUOUS, name="MinThroughput")
    model.setObjective(min_throughput, GRB.MAXIMIZE)

    # Constraints: capacity constraints and flow conservation constraints
    # capacity constraints for each underlay link
    for i in range(num_nodes):
        for j in range(num_nodes):
            # find the associated overlay links
            overlay_links = mapping[i*num_nodes+j]
            overlay_links = overlay_links.reshape((num_nodes, num_nodes))
            fs = []
            for ii in range(num_nodes):
                for jj in range(num_nodes):
                    if overlay_links[ii][jj] > 0:
                        for k in range(num_commodities):
                            fs.append(flows[(k, ii, jj)])
                
            model.addConstr(quicksum(fs) <= capacities[i][j])
    # capacity constraints for self loop
    for i in range(num_nodes):
        model.addConstr(quicksum([flows[(k, i, i)] for k in range(num_commodities)]) <= capacities[i][j])

    # flow convservation constraints
    for k in range(num_commodities):
        # Flow conservation constraint for each commodity
        for i in range(num_nodes):
            model.addConstr(quicksum(flows[(k, j, i)] for j in range(num_nodes)) - quicksum(flows[(k, i, j)] for j in range(num_nodes)) == -demands[k][i]*relaxation_factors[k])

            # Update the minimum throughput variable as the minimum flow among commodities
            if demands[k][i] < 0:
                model.addConstr(min_throughput <= quicksum(flows[(k, j, i)] for j in range(num_nodes)))

    # Set Gurobi parameters

    # Solve the problem
    model.optimize()

    # Retrieve the optimal flow values
    optimal_flows = {}
    for k in range(num_commodities):
        for i in range(num_nodes):
            for j in range(num_nodes):
                optimal_flows[(k, i, j)] = flows[(k, i, j)].x

    # Retrieve the optimal relaxation factors
    optimal_relaxation_factors = {}
    for k in range(num_commodities):
        optimal_relaxation_factors[k] = relaxation_factors[k].x

    return min_throughput.x, optimal_flows, optimal_relaxation_factors