"""
A session to run the idea(s) on new setting
"""
import sys
import os
import time
import json
import argparse
import logging
from typing import List
from collections import defaultdict, deque
import numpy as np
from equal_allocation import equal_share_by_group_baseline
from equal_allocation import equal_share_full_outof_dependency
from equal_allocation import equal_share_per_link_flow
from equal_allocation import data_aware_by_group
from new_setting import *
# from flow_chunk_optimization import flow_chunk_optimization
from dynamic_allocation import dynamic_allocation_optimization
from weight_allocation import weight_allocation_optimization
from multi_ring import flow_chunk_optimization3
#from mmf_weight_allocation import mmf_weight_allocation_optimization
#from mmf_functions import is_max_min_fair
# from weight_try import weight_allocation_optimization2


logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

def extract_information(config_path: str, filename: str):
    """Extracting the information from the configuration file."""
    # load the json to dict
    file_path = os.path.join(config_path, filename)
    with open(file_path, "r", encoding="utf-8") as f:
        info_data = json.load(f)
    return info_data

def _main():
    print("sys.argv =", sys.argv)
    """Run the experiment."""
    start_time = time.time()
    # Extract the parse arguments
    parser = argparse.ArgumentParser(description="Process some files.")
    # Define the -b and -c arguments
    parser.add_argument(
        "-r", "--results", type=str, required=True, help="Path to results"
    )
    parser.add_argument(
        "-c", "--config", type=str, required=True, help="Path to config file"
    )

    parser.add_argument(
        "-p",
        "--project",
        type=str,
        required=True,
        help="Path to config file for the optimization",
    )
    parser.add_argument(
        "-m",
        "--method",
        type=str,
        required=True,
        help="Method used to optimize the flow rates",
    )

    # Get the directory of the current script
    script_path = os.path.abspath(__file__)
    base_path = os.path.dirname(script_path)
    config_foldername = "configs"

    # Parse the arguments
    args = parser.parse_args()
    result_path = args.results
    proj_name = args.project
    config_name = args.config
    method_name = args.method

    # Extract the basic settings
    config_folder_path = base_path #os.path.join(base_path, config_foldername)
    optconfig_path = base_path # os.path.join(base_path, config_foldername) #, optconfig_name)
    info = extract_information(config_folder_path, config_name)

    flow_info = get_flow_info(info) # flow info in the order of each flow

    link_cap = info.get("link_capacities") # link capacities
    dependency_order = get_dependency_order(flow_info) # dep orders, e.g. {(1, 1): ['1'], (1, 2): ['3', '2'], (2, 3): ['4']}
    fid_to_order_dict = fid_to_order(dependency_order)
    flows_for_each_link = get_flows_for_each_link(flow_info)

    project_path = os.path.join(result_path, proj_name, method_name)
    os.makedirs(project_path, exist_ok = True)  #./new/toyExample/flowChunk

    if method_name == "flowChunk":
        result = flow_chunk_optimization(flow_info, link_cap, dependency_order, fid_to_order_dict)

    elif method_name == "dynamicAlloc":
        result = dynamic_allocation_optimization(flow_info, link_cap, fid_to_order_dict, flows_for_each_link)

    elif method_name == "equalAlloc":
        result = equal_share_by_group_baseline(flow_info, link_cap, flows_for_each_link)

    elif method_name == "equalOutOfOrderAlloc":
        result = equal_share_full_outof_dependency(flow_info, link_cap, flows_for_each_link)

    elif method_name == "equalPerLink":
        result =equal_share_per_link_flow(flow_info, link_cap, flows_for_each_link)

    elif method_name == "dataAwareByGroup":
        result = data_aware_by_group(flow_info, link_cap, flows_for_each_link)

    elif method_name == "weightAlloc":
        weights_out, flow_rates, time_cost, objective = weight_allocation_optimization(flow_info, link_cap, fid_to_order_dict, flows_for_each_link, verbose=True)
        result = {"weights": weights_out, "flow_rate": flow_rates, "time_cost": time_cost, "avg_completion_time": objective}

    elif method_name in ("multiRing" or "multiRingWeight"):
        result = flow_chunk_optimization3(flow_info, link_cap, dependency_order, fid_to_order_dict, flows_for_each_link)
        # r_fe = fair_allocate(weights, flow_info, link_cap, flows_for_each_link)
    else:
        raise ValueError(f"Unknown method: {method_name}")

    #if method_name == "mmfWeightAlloc":
    #   weights_out, flow_rates, time_cost, objective = mmf_weight_allocation_optimization(flow_info, link_cap, fid_to_order_dict, flows_for_each_link, verbose=True)
    #    #is_fair = is_max_min_fair(flow_info, flow_rates, link_cap)
    #    #logging.info(f"Is the allocation max-min fair? {is_fair}")
    #    result = {"weights": weights_out, "flow_rate": flow_rates, "time_cost": time_cost, "avg_completion_time": objective}

    output_file = os.path.join(project_path, "result.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4)
    #if method_name == "weightAlloc" or "mmfWeightAlloc":
    #   logging.info(flow_rates)
    #end_time = time.time()
    #elapsed = end_time-start_time
    #logging.info("Execution time: %.6fseconds", elapsed)
    logging.info("%sOptimized %s saved at %s", "*" * 15, "result.json", project_path)
    logging.info("%s %s Done.", "*" * 15, proj_name)

if __name__ == "__main__":
    _main()
