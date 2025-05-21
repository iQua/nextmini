from model import Model
import numpy as np
import networkx as nx
import torch
import os

def generate_LC_map(x, model_path, threshold):
    model = Model()
    model.load_from_checkpoint(model_path, map_location=torch.device('cpu'))
    model.eval()
    xx = model(x)
    n = int(np.sqrt(x.shape[1])) #Number of nodes
    xx = xx.reshape(xx.shape[0], n*n, n*n)
    #===Construct the mapping matrix of shape(underlay_edges, overlay_edges)===
    mapping = torch.zeros(xx.shape)

    # #Make sure that each column (overlay edges) sums to at least 1 (each overlay edge has one underlay edge at least)
    # indices = torch.topk(xx, 1, dim=1)[1]
    # mapping.scatter_(1, indices, 1)

    #Add strato correlation: overlay edges with the same source node should have the same underlay edge
    for i in range(n):
        indices = torch.tensor([j for j in range(i*n, (i+1)*n) if j != i*n +i] )
        underlay_edge = xx[:, :, indices].sum(dim=-1).argmax()
        mapping[:, underlay_edge, indices] = 1

    mapping = mapping.squeeze()
    #Pick the predicted edges in general
    # while True:
    #     indices = xx.view(-1) > threshold
    #     mapping.view(-1)[indices] = 1

    #     #check if the mappingis valid
    #     underlay = mapping.sum(dim=1).reshape(n, n).numpy()
    #     G = nx.from_numpy_array(underlay, create_using=nx.DiGraph) 
    #     isolated_nodes = list(nx.isolates(G)) # Find isolated vertices
    #     G.remove_nodes_from(isolated_nodes)# Remove isolated vertices
    #     if nx.is_strongly_connected(G):
    #         break
    #     threshold -= 0.1
    indices = xx.view(-1) > threshold
    mapping.view(-1)[indices] = 1 

    #Remove columns that would be self loops
    mapping[torch.stack([torch.eye(n).flatten()] * n**2) == 1] = 0
    #Remove the rows that would be self loops
    mapping[torch.stack([torch.eye(n).flatten()] * n**2).T == 1] = 0

    return mapping

if __name__ == "__main__":
    mapping = torch.load(os.path.dirname(os.path.abspath(__file__)) + "/mapping.pt")
    generate_LC_map(mapping, model_path=os.path.dirname(os.path.abspath(__file__)) + "/models/LC6NodesV57.ckpt", threshold=15.5)



