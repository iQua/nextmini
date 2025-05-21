import networkx as nx
import pickle
import os

'''The topology looks like this:
  w1          w2
    \        /
      R1 - R2 
      |  X  |
      R3 - R4
    /        \
  w3          w4

The node id of the workers are 1, 2, 3, 4
The node id of the relays are 5, 6, 7, 8
'''
T = nx.Graph()

#path0 uses sp, custom path1 and path2
G= nx.Graph()
G.add_nodes_from([1,2,3,4,5,6,7,8])

#adding edges between worker and the relay nodes
G.add_edge(1,5, path0=[1,5], path1=[1,5], path2=[1,5])
G.add_edge(2,6, path0=[2,6], path1=[2,6], path2=[2,6])
G.add_edge(3,7, path0=[3,7], path1=[3,7], path2=[3,7])
G.add_edge(4,8, path0=[4,8], path1=[4,8], path2=[4,8])

#Fullmesh between the relay nodes
G.add_edge(5,6, path0=[5,6], path1=[5,6], path2=[5,6])
G.add_edge(5,7, path0=[5,7], path1=[5,7], path2=[5,7])
G.add_edge(5,8, path0=[5,8], path1=[5,8], path2=[5,8])
G.add_edge(6,7, path0=[6,7], path1=[6,7], path2=[6,7])
G.add_edge(6,8, path0=[6,8], path1=[6,8], path2=[6,8])
G.add_edge(7,8, path0=[7,8], path1=[7,8], path2=[7,8])

file_path = os.path.abspath(__file__)
os.chdir(os.path.dirname(file_path))
file_name = os.path.splitext(os.path.basename(file_path))[0] + ".pkl"
pickle.dump((T, G), open(file_name, "wb"))


