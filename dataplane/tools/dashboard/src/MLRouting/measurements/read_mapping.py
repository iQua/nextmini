import torch
a = torch.load("./mapping.pt")
a = a[0]
print(a.sum(dim=0).reshape((6,6)))
print(a.sum(dim=1).reshape((6,6)))
