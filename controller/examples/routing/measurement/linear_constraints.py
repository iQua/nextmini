from model import Model
import numpy as np
import torch

def generate_LC_map(x, model_path):
    x = model(x)
    model = Model.load_from_checkpoint(model_path)
    n = int(np.sqrt(x.shape[1])) #Number of nodes
    xx = xx.reshape(xx.shape[0], n*n, n*n)
    #===Construct the mapping matrix of shape(underlay_edges, overlay_edges)===
    mapping = torch.zeros(xx.shape)

    #Make sure that each column (overlay edges) sums to at least 1 (each overlay edge has one underlay edge at least)
    indices = torch.topk(xx, 1, dim=1)[1]
    mapping.scatter_(1, indices, 1)

    #Pick the top k edges in general
    topk = 18
    indices = torch.topk(xx.view(xx.shape[0], n**4), topk, dim=-1)[1]
    mapping.view(xx.shape[0], n**4).scatter_(1, indices, 1)
    return mapping




