"""
The generic items used to perform the stellar function.
"""

from typing import List, Dict
from dataclasses import dataclass

import numpy as np
from transformers.utils import ModelOutput as FieldFrozenContainer


def create_indicator_matrix(
    row_ids: List[str], col_ids: List[str], mapper: Dict[str, str]
) -> np.ndarray:
    """Create a indicator matrix presenting the alignment of ids."""
    num_rows = len(row_ids)
    num_cols = len(col_ids)
    matrix = np.zeros((num_rows, num_cols))
    for row_idx in range(num_rows):
        row_id = row_ids[row_idx]
        cols = mapper[row_id]
        if isinstance(cols, list):
            for col_id in cols:
                col_idx = np.where(col_ids == col_id)[0][0]
                matrix[row_idx, col_idx] = 1
        else:
            col_id = cols
            col_idx = np.where(col_ids == col_id)[0][0]
        matrix[row_idx, col_idx] = 1

    return matrix


def create_indicator_mapper(
    row_ids: List[str], col_ids: List[str], matrix: np.ndarray
) -> Dict[str, str]:
    """Create a mapper presenting the alignment of items."""
    num_rows = len(row_ids)
    num_cols = len(col_ids)
    mapper = {row_id: [] for row_id in row_ids}
    reverse_mapper = {col_id: [] for col_id in col_ids}
    for row_idx in range(num_rows):
        for col_idx in range(num_cols):
            if matrix[row_idx, col_idx] == 1:
                row_id = row_ids[row_idx]
                col_id = col_ids[col_idx]
                mapper[row_id].append(col_id)
                reverse_mapper[col_id].append(row_id)
    return mapper


@dataclass
class BaseFlow(FieldFrozenContainer):
    """
    A base class for all flows.
    """

    # The unique identifier of the flow.
    id: str

    ## Items related to the source of the flow.
    # The volume of data in the flow.
    data_volume: int = None

    # The src and dst of the flow
    # The links connected src and dst
    src: str = None
    dst: str = None
    links: List[str] = None

    # Dependent flow ids
    dependent_flow_ids: List[str] = None

    # The owner of the flow.
    # As a flow is generated included in a group, this owner identity
    # presents the group that the flow belongs to.
    # For example, this is generated used in the coflow scenarios to present
    # which coflow the flow belongs to.
    collective_id: str = None
    group_id: str = None

    def __str__(self):
        """
        Return the string representation of the flow.
        """
        return f"[Collective {self.collective_id}-Group {self.group_id}-Flow {self.id}] contains {self.data_volume} data."

    def get_unique_id(self):
        """
        Return the unique identifier of the flow.
        """
        return f"{self.collective_id}-{self.group_id}-{self.id}"

    def update_dependent_flow_ids(self):
        """
        The dependent flow only appear within the same collective and group.
        """
        self.dependent_flow_ids = [
            f"{self.collective_id}-{self.group_id}-{flow_id}"
            for flow_id in self.dependent_flow_ids
        ]

    def get_dependency_order(self):
        """
        Get the order of the flow in the dependent flows.
        The order starts from 0.
        """
        return len(self.dependent_flow_ids)


@dataclass
class BaseLink(FieldFrozenContainer):
    """
    A base class for all flows.
    """

    # The unique identifier of the flow.
    id: str

    # The capacity of the link
    capacity: int = None


@dataclass
class CollectiveGroupContainer(FieldFrozenContainer):
    """
    A base container for collectives and groups.
    """

    # Include the collective ids (unique)
    collective_ids: np.ndarray
    # Include the group ids (unique) of each collective
    # They are sorted according to the collective ids
    group_ids: List[np.ndarray] = None

    # Maintain the original order of the collective ids
    sorted_indexes: np.ndarray = None
    # Maintain the original order of groups in each collective
    group_sorted_indexes: List[np.ndarray] = None

    K: int = None
    Nks: List[int] = None
    N: int = None

    def sort_collective_groups(self):
        """Sort the groups."""
        self.sorted_indexes = np.argsort(self.collective_ids)
        self.collective_ids = self.collective_ids[self.sorted_indexes]
        self.group_ids = [self.group_ids[idx] for idx in self.sorted_indexes]

        self.group_sorted_indexes = [np.argsort(groups) for groups in self.group_ids]
        self.group_ids = [
            groups[self.group_sorted_indexes[i]]
            for i, groups in enumerate(self.group_ids)
        ]

    def get_numbers(self):
        """Set the numbers of collectives and groups."""
        self.K = len(self.collective_ids)
        self.Nks = [len(groups) for groups in self.group_ids]
        self.N = sum(self.Nks)

    def coll_id(self, index):
        """Get the collective id."""
        return self.collective_ids[index]

    def group_id(self, coll_index, index):
        """Get the group id."""
        return self.group_ids[coll_index][index]


@dataclass
class BaseContainer(FieldFrozenContainer):
    """
    A contain to hold all items in a sorted order.

    This can be used as the container for the flow or the link
    """

    item_objs: List[FieldFrozenContainer] = None
    # The contained items that are sorted from low to high
    # based on the string id directly
    # Please always use the unique id for the flow
    item_ids: np.ndarray = None

    # The indexes for the sorted flows
    sorted_indexes: np.ndarray = None

    def sort_items(self):
        """Sort the flows."""
        self.item_ids = np.array(self.item_ids)
        self.sorted_indexes = np.argsort(self.item_ids)
        self.item_ids = self.item_ids[self.sorted_indexes]
        self.item_objs = [self.item_objs[idx] for idx in self.sorted_indexes]

    def check_validity(self):
        """Checking whether the flows are correctly contained."""
        assert isinstance(self.item_ids, np.ndarray)
        assert len(self.item_ids) == len(np.unique(self.item_ids))

    def item_obj(self, index):
        """Get the item obj."""
        return self.item_objs[index]


@dataclass
class FlowLinkHolder(FieldFrozenContainer):
    """
    A holder for containing flow links
    """

    # The identity of the flow.
    flow_ids: str = None
    # The identity of the link.
    link_ids: str = None

    # A mapper showing which flow is assigned to which link.
    # Flow identity -> Link identity
    f2l_mapper: Dict[str, List[str]] = None
    # Link identity -> Flow identity
    l2f_mapper: Dict[str, List[str]] = None
    # A matrix to show the assignment of the flows to the links.
    # With the shape of (num_flows, num_links).
    # where the num_flows here present the total number of flows
    # 1: Assigned, 0: Not assigned.
    matrix: np.ndarray = None

    F: int = None
    E: int = None

    def create_mapper2matrix(self):
        """Get the assignment matrix."""
        self.matrix = create_indicator_matrix(
            row_ids=self.flow_ids,
            col_ids=self.link_ids,
            mapper=self.f2l_mapper,
        )
        self.l2f_mapper = {f: l for l, fs in self.f2l_mapper.items() for f in fs}

    def create_matrix2mapper(self):
        """Create a mapper from the matrix."""
        self.l2f_mapper = create_indicator_mapper(
            row_ids=self.flow_ids,
            col_ids=self.link_ids,
            matrix=self.matrix,
        )
        self.f2l_mapper = {f: l for l, fs in self.l2f_mapper.items() for f in fs}

    def get_numbers(self):
        """Set the numbers of flows and links."""
        self.F = len(self.flow_ids)
        self.E = len(self.link_ids)


@dataclass
class FlowLinkSendHolder(FieldFrozenContainer):
    """
    A holder for containing the sending info of the flow and the link.
    """

    # A holder to include the flows and links
    fl_holder: FlowLinkHolder = None
    # A 2D matrix containing the link capacity that the flow passes
    capacity_matrix: np.ndarray = None
    # A 2D matrix containing how much data of the flow is sent through
    # the link
    data_matrix: np.ndarray = None


@dataclass
class FlowCGHolder(FieldFrozenContainer):
    """
    A class to represent the holder of a flow to a link.
    The holder is represented as a matrix whose row is the flow
    and the columns are collective, group, and flow sending order.
    """

    # The identity of the flow.
    flow_ids: str = None

    # A mapper showing which flow is assigned to which link.
    # Flow identity -> Link identity
    f2cgd_mapper: Dict[str, List[str]] = None

    # A matrix to show the relation between flows, collectives,
    # groups.
    # With shape [N, 3], where
    # [:, 0]: collective id, [:, 1]: group id, [:, 2]: send order,
    matrix: np.ndarray = None
