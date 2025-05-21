import networkx as nx
import pickle
import os

G= nx.Graph()
G.add_nodes_from([1,2,3,4,5,6])
G.add_edge(1,2, path0 = [1,2], path1 = [1,3,2])
G.add_edge(1,3, path0 = [1,3], path1 = [1,4,3])
G.add_edge(1,4, path0 = [1,4], path1 = [1,3,4])
G.add_edge(1,5, path0 = [1,5], path1 = [1,6,5])
G.add_edge(1,6, path0 = [1,6], path1 = [1,2,6])
G.add_edge(2,3, path0 = [2,3], path1 = [2,4,3])
G.add_edge(2,4, path0 = [2,4], path1 = [2,5,4])
G.add_edge(2,5, path0 = [2,5], path1 = [2,6,5])
G.add_edge(2,6, path0 = [2,6], path1 = [2,5,6])
G.add_edge(3,4, path0 = [3,4], path1 = [3,5,4])
G.add_edge(3,5, path0 = [3,5], path1 = [3,6,5])
G.add_edge(3,6, path0 = [3,6], path1 = [3,5,6])
G.add_edge(4,5, path0 = [4,5], path1 = [4,6,5])
G.add_edge(4,6, path0 = [4,6], path1 = [4,5,6])
G.add_edge(5,6, path0 = [5,6], path1 = [5,4,6])

G1 = nx.Graph()
G1.add_nodes_from([1,2,3,4,5,6])
G1.add_edge(1,2, rate=100000000)
G1.add_edge(2,3, rate=100000000)
G1.add_edge(3,4, rate=100000000)
G1.add_edge(4,5, rate=100000000)
G1.add_edge(5,6, rate=100000000)
G1.add_edge(6,1, rate=100000000)
G1.add_edge(1,3, rate=100000000)
G1.add_edge(1,4, rate=100000000)
G1.add_edge(1,5, rate=100000000)
G1.add_edge(2,4, rate=100000000)
G1.add_edge(2,5, rate=100000000)
G1.add_edge(2,6, rate=100000000)
G1.add_edge(3,5, rate=100000000)
G1.add_edge(3,6, rate=100000000)
G1.add_edge(4,6, rate=100000000)

os.chdir(os.path.dirname(os.path.abspath(__file__)))
pickle.dump((G, G1), open("6n_2p_100M.pkl", "wb"))


