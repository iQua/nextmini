import networkx as nx
import pickle
import os

'''The topology looks like this:
            1 --- 2
             \   /
               3
             / | \ 
            7* |  8*
             \ | /
               4
             /   \
            5 --- 6 

Where 1-2-3, 4-5-6 represents two subnets 3, 4 are the gateways for the two subnets.
Initially, 3-4 is the only connection. To improve the bandwidth, we use a three path
configuration, with two additional nodes 7,8 purely for data forwarding. 
'''
T = nx.Graph()
T.add_nodes_from([1,2,3,4,5,6,7,8])

#Links between 1,2,3 and 4,5,6 are unlimited bandwidth. We want to limit the bandwdith between 3-7-8-4
T.add_edge(3,4, rate=100000000)
T.add_edge(3,7, rate=100000000)
T.add_edge(3,8, rate=100000000)
T.add_edge(4,7, rate=100000000)
T.add_edge(4,8, rate=100000000)

#path0 uses sp, custom path1 and path2
G= nx.Graph()
G.add_nodes_from([1,2,3,4,5,6,7,8])
G.add_edge(1,2, path0 = [1,2], path1 = [1,2], path2 = [1,2])
G.add_edge(1,3, path0 = [1,3], path1 = [1,3], path2 = [1,3])
G.add_edge(1,4, path0 = [1,3,4], path1 = [1,3,7,4], path2 = [1,3,8,4])
G.add_edge(1,5, path0 = [1,3,4,5], path1 = [1,3,7,4,5], path2 = [1,3,8,4,5])
G.add_edge(1,6, path0 = [1,3,4,6], path1 = [1,3,7,4,6], path2 = [1,3,8,4,6])
G.add_edge(2,3, path0 = [2,3], path1 = [2,3], path2 = [2,3])
G.add_edge(2,4, path0 = [2,3,4], path1 = [2,3,7,4], path2 = [2,3,8,4])
G.add_edge(2,5, path0 = [2,3,4,5], path1 = [2,3,7,4,5], path2 = [2,3,8,4,5])
G.add_edge(2,6, path0 = [2,3,4,6], path1 = [2,3,7,4,6], path2 = [2,3,8,4,6])
G.add_edge(3,4, path0 = [3,4], path1 = [3,7,4], path2 = [3,8,4])
G.add_edge(3,5, path0 = [3,4,5], path1 = [3,7,4,5], path2 = [3,8,4,5])
G.add_edge(3,6, path0 = [3,4,6], path1 = [3,7,4,6], path2 = [3,8,4,6])
G.add_edge(4,5, path0 = [4,5], path1 = [4,5], path2 = [4,5])
G.add_edge(4,6, path0 = [4,6], path1 = [4,6], path2 = [4,6])
G.add_edge(5,6, path0 = [5,6], path1 = [5,6], path2 = [5,6])


os.chdir(os.path.dirname(os.path.abspath(__file__)))
pickle.dump((T, G), open("6n_2p_100M_3para.pkl", "wb"))


