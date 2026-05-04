"""
New flow information setting, replacement of generic.py
"""

import os
import json
import argparse
import logging
from typing import List
from collections import defaultdict, deque
import numpy as np


def get_flow_info(info):
    """
    Summarize most of the flow information from the json file and store in a dict
    Input: Raw flow information from the json file
    Output: A dict of the summarized flow information
    Example Output -> {'1': {'collective': 1, 'group': 1, 'source': 1, 'dest': 10, 'data_size': 150.0, 'links': {1, 2, 3, 4}},
    '2': {'collective': 1, 'group': 2, 'source': 2, 'dest': 11, 'data_size': 50.0, 'links': {5, 6, 2, 7, 9}} ...}
    """
    flow_idx = [key for key in info if key.isdigit()]
    flow_info = {}
    link_cap = {}
    for flow in flow_idx:
        flow_info[flow] = {}
        flow_info.get(flow)['collective'] = info.get(flow)['collective_id']
        flow_info.get(flow)['group'] = info.get(flow)['group_id']
        flow_info.get(flow)['source'] = info.get(flow)['src']
        flow_info.get(flow)['dest'] = info.get(flow)['dst']
        flow_info.get(flow)['data_size'] = info.get(flow)['total']/1024/1024/8
        flow_info.get(flow)['links'] = set(info.get(flow)['links'])
        flow_info.get(flow)['dependencies'] = info.get(flow)['dependencies']
    return flow_info

def dfs(flow, visited, graph, order):
    """
    visited: set of visited nodes
    graph: dict of graph, ex: {fid = '2': depends_on = ['4'], fid = '3': ['5'], fid = '4': ['3'], fid = '5': []}
    order: list of the order of the dependencies
    Example Output -> ['5','3','4','2']
    """
    if flow in visited:
        return
    visited.add(flow)

    for dep in graph[flow]:
        dfs(str(dep), visited, graph, order)
    order.append(flow)


def get_dependency_order(flow_info):
    """
    Get each flow group's dependency order
    Example Output -> {(k = 1, n = 1): order_list = ['1'], (k = 1, n = 2): order_list = ['2','3'], (k = 2, n = 3): order_list = ['4']}
    """
    groups = defaultdict(dict)
    for flow_id, flow in flow_info.items():
        groups[(flow["collective"], flow["group"])][flow_id] = flow
    dependency_orders = {}
    # key: (collective, group) value: flows
    for group_key, flows in groups.items(): # e.g. (k=1, n=2), {'1': {'collective': 1, 'group': 1, 'source': 1, 'dest': 10, 'data_size': 150.0, 'links': {1, 2, 3, 4}, 'dependency_order': []}})
        # Construct graph
        graph = {flow_id: [] for flow_id in flows}
        for flow_id, flow in flows.items():
            # print(f"flow is:{flow}")
            dep = flow["dependencies"]
            if dep == []:
                graph[flow_id] = []
            else:
                # print(dep)
                # print(type(dep))
                graph[flow_id] = dep
        # print(f"graph right now is: {graph}")
        visited = set()
        order = []

        for flow in graph:
            if flow not in visited:
                dfs(flow, visited, graph, order)
        dependency_orders[group_key] = order
    return dependency_orders


def fid_to_order(dependency_orders):
    """
    Get each flow's dependency order within their own flow group
    Example Input -> {(k = 1, n = 1): ['1'], (k = 1, n = 2): ['2','3'], (k = 2, n = 3): ['4']}
    Example Output -> {fid = '1': order = 1, fid = '2': order = 2, fid = '3': order = 1, fid = '4': order = 1}
    """
    dep = {}
    # print(f"dependency orders: {dependency_orders}")
    for key, values in dependency_orders.items():
        for flow in values:
            dep.setdefault(flow, []).append(values.index(flow) + 1)
    return dep


def get_flows_for_each_link(flow_info):
    """
    Construct a dictionary mapping each link to the flows that share it, grouped by flow group IDs.
    Example Output -> {link = 1: {flow_group = 1: [fid = '1'], flow_group = 2: [fid = '2', fid = '3']}, link = 2: {flow_group = 3: [fid = '4', fid = '5']}}
    """
    link_dict = {}
    for flow_id, flow in flow_info.items():
        for link in flow['links']:
            # link_dict.setdefault(link, []).append(flow_id)
            link_dict.setdefault(link, {}).setdefault(flow['group'], []).append(flow_id)

    return link_dict





