import networkx as nx
import pickle
import os

'''The topology looks like this:
      w1 - w2 
      |  X  |
      w3 - w4

The node id of the workers are 1, 2, 3, 4
'''
T = nx.Graph()
T.add_nodes_from([1,2,3,4])

#Fullmesh between the worker nodes, with a rate of 300Mbps
T.add_edge(1,2, rate=300_000_000)
T.add_edge(1,3, rate=300_000_000)
T.add_edge(1,4, rate=300_000_000)
T.add_edge(2,3, rate=300_000_000)
T.add_edge(2,4, rate=300_000_000)
T.add_edge(3,4, rate=300_000_000)

G= nx.Graph()
G.add_nodes_from([1,2,3,4])

#Fullmesh between the worker nodes
G.add_edge(1,2, path0=[1,2], path1=[1,2], path2=[1,2])
G.add_edge(1,3, path0=[1,3], path1=[1,3], path2=[1,3])
G.add_edge(1,4, path0=[1,4], path1=[1,4], path2=[1,4])
G.add_edge(2,3, path0=[2,3], path1=[2,3], path2=[2,3])
G.add_edge(2,4, path0=[2,4], path1=[2,4], path2=[2,4])
G.add_edge(3,4, path0=[3,4], path1=[3,4], path2=[3,4])


file_path = os.path.abspath(__file__)
os.chdir(os.path.dirname(file_path))
file_name = os.path.splitext(os.path.basename(file_path))[0] + ".pkl"
pickle.dump((T, G), open(file_name, "wb"))


