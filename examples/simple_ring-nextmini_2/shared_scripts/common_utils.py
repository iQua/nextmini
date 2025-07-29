"""
Commonly used utilities for the stellar function.
"""

import numpy as np


def get_link_groups(fl_matrix, fcg_matrix, link_idx, collective_ids, group_ids):
    """Get all groups that pass the link"""
    # 2. Get the flows that are sent on this link
    link_flows = fl_matrix[:, link_idx]
    flow_indxes = np.where(link_flows == 1)[0]
    # 3. Get the groups for these flows
    # with shape [len(flow_indxes), 2]
    # [:,0]: collective id, [: 1]: group id
    coll_groups = fcg_matrix[flow_indxes, :2]
    coll_groups = np.unique(coll_groups, axis=0)
    # get their indexes
    coll_groups_idx = []
    for coll_id, group_id in coll_groups:
        col_idx = np.where(collective_ids == coll_id)[0][0]
        group_idx = np.where(group_ids[col_idx] == group_id)[0][0]
        coll_groups_idx.append((col_idx, group_idx))

    return coll_groups, coll_groups_idx
