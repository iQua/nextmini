import os
import sqlalchemy as sql
import numpy as np
from collections import defaultdict
from time import sleep

def measure_links(conn, prev_read, n_nodes):
    metrics = conn.execute(sql.text('''
        SELECT src_node_id, dst_node_id, total_bps, time_read
        FROM (
        SELECT src_node_id, dst_node_id, total_bps, time_read, ROW_NUMBER() OVER (PARTITION BY src_node_id, dst_node_id ORDER BY time_read DESC) AS row_num
        FROM (SELECT src_node_id, dst_node_id, SUM(bps) AS total_bps, time_read FROM "Metrics" GROUP BY src_node_id, dst_node_id, time_read) AS subquery1
        ) AS subquery
        WHERE row_num = 1;
    '''))
    ret = np.zeros((n_nodes, n_nodes))
    for metric in metrics:
        if not prev_read.get((metric[0], metric[1])) or prev_read[(metric[0], metric[1])] < metric[3]:
            ret[metric[0]-1][metric[1]-1] = metric[2]
            prev_read[(metric[0], metric[1])] = metric[3]
        else:
            ret[metric[0]-1][metric[1]-1] = 0
        
        print("{}, {}, {}".format(metric[0], metric[1], ret[metric[0]-1][metric[1]-1]))

    return ret, prev_read

def measure_flows(conn, prev_read, n_nodes):
    # Get all unique flow ids
    # Find all flow ids
    res = conn.execute(sql.text('''
        SELECT DISTINCT flow_id
        FROM "Metrics"
    '''))
    flow_ids = [arr[0] for arr in res]

    # Map flow -> dest id
    flow_to_dst = defaultdict(int)
    for flow_id in flow_ids:
        dst_flow_id = '.'.join(map(str, flow_id[4:])) # dest part only
        res = conn.execute(sql.text('''
            SELECT id
            FROM "Nodes"
            WHERE virtual_network_addr = '{}'
        '''.format(dst_flow_id,)))

        flow_to_dst[tuple(flow_id)] = [arr[0] for arr in res][0]
    
    # Map flow -> src id
    flow_to_src = defaultdict(int)
    for flow_id in flow_ids:
        src_flow_id = '.'.join(map(str, flow_id[:4])) # dest part only
        res = conn.execute(sql.text('''
            SELECT id
            FROM "Nodes"
            WHERE virtual_network_addr = '{}'
        '''.format(src_flow_id,)))

        flow_to_src[tuple(flow_id)] = [arr[0] for arr in res][0]

    # Get the latest metric for each flow
    metrics = conn.execute(sql.text('''
        SELECT flow_id, dst_node_id, total_bps, time_read
        FROM (
        SELECT flow_id, dst_node_id, total_bps, time_read, ROW_NUMBER() OVER (PARTITION BY flow_id, dst_node_id ORDER BY time_read DESC) AS row_num
        FROM (SELECT flow_id, dst_node_id, SUM(bps) AS total_bps, time_read FROM "Metrics" GROUP BY flow_id, dst_node_id, time_read) AS subquery1
        ) AS subquery
        WHERE row_num = 1
        ORDER BY TOTAL_BPS DESC;
    '''))
    ret = np.zeros((n_nodes, n_nodes))
    for metric in metrics:
        flow_id = metric[0]
        dst_node_id = flow_to_dst[tuple(flow_id)] -1
        src_node_id = flow_to_src[tuple(flow_id)] -1
        if flow_to_dst[tuple(flow_id)] == metric[1]:
            last_read = prev_read.get((src_node_id, dst_node_id), None)
            if not last_read or last_read < metric[3]:
                ret[src_node_id, dst_node_id] =  metric[2]
            else:
                ret[src_node_id, dst_node_id] =  0
            prev_read[(src_node_id, dst_node_id)] = metric[3]
    
    return ret, prev_read