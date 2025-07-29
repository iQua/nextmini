import argparse
import os
import random
import time

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
import torchvision.transforms as transforms
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler

import torch.distributed as dist

def set_random_seeds(random_seed=0):
    torch.manual_seed(random_seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    np.random.seed(random_seed)
    random.seed(random_seed)


def evaluate(model, device, test_loader):
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for data in test_loader:
            images, labels = data[0].to(device), data[1].to(device)
            outputs = model(images)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()
    accuracy = correct / total
    return accuracy


class LeNet5(nn.Module):
    def __init__(self):
        super(LeNet5, self).__init__()
        self.conv1 = nn.Conv2d(1, 6, 5)
        self.conv2 = nn.Conv2d(6, 16, 5)
        self.fc1 = nn.Linear(16 * 4 * 4, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x):
        x = nn.functional.tanh(self.conv1(x))
        x = nn.functional.avg_pool2d(x, 2)
        x = nn.functional.tanh(self.conv2(x))
        x = nn.functional.avg_pool2d(x, 2)
        x = x.view(-1, 16 * 4 * 4)
        x = nn.functional.tanh(self.fc1(x))
        x = nn.functional.tanh(self.fc2(x))
        x = self.fc3(x)
        return x


def main():
    num_epochs_default = 3
    batch_size_default = 1024
    learning_rate_default = 0.001
    random_seed_default = 0

    local_rank = int(os.environ["OMPI_COMM_WORLD_LOCAL_RANK"])
    world_rank = int(os.environ["OMPI_COMM_WORLD_RANK"])

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--num_epochs",
        type=int,
        help="Number of training epochs.",
        default=num_epochs_default,
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        help="Training batch size for one process.",
        default=batch_size_default,
    )
    parser.add_argument(
        "--learning_rate",
        type=float,
        help="Learning rate.",
        default=learning_rate_default,
    )
    parser.add_argument(
        "--random_seed",
        type=int,
        help="Random seed.",
        default=random_seed_default,
    )
    argv = parser.parse_args()

    num_epochs = argv.num_epochs
    batch_size = argv.batch_size
    learning_rate = argv.learning_rate
    random_seed = argv.random_seed

    set_random_seeds(random_seed=random_seed)

    torch.distributed.init_process_group(backend="gloo")

    model = LeNet5()
    device = torch.device("cpu")
    model = model.to(device)

    ddp_model = torch.nn.parallel.DistributedDataParallel(model)

    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
    )

    train_set = torchvision.datasets.MNIST(
        root="data", train=True, download=True, transform=transform
    )
    test_set = torchvision.datasets.MNIST(
        root="data", train=False, download=True, transform=transform
    )

    train_sampler = DistributedSampler(dataset=train_set)
    train_loader = DataLoader(
        dataset=train_set,
        batch_size=batch_size,
        sampler=train_sampler,
        num_workers=4,
    )
    test_loader = DataLoader(
        dataset=test_set, batch_size=128, shuffle=False, num_workers=4
    )

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(ddp_model.parameters(), lr=learning_rate)

    for epoch in range(num_epochs):
        print(
            "World Rank: {}, Epoch: {}, Training...".format(world_rank, epoch)
        )

        if local_rank == 0:
            accuracy = evaluate(
                model=ddp_model, device=device, test_loader=test_loader
            )

            print("-" * 75)
            print(
                "World Rank: {}, Epoch: {}, Accuracy: {}".format(
                    world_rank, epoch, accuracy
                )
            )
            print("-" * 75)

        ddp_model.train()

        total_bytes = 0
        total_sync_time = 0.0

        for data in train_loader:
            inputs, labels = data[0].to(device), data[1].to(device)
            optimizer.zero_grad()
            outputs = ddp_model(inputs)
            loss = criterion(outputs, labels)
            
            with ddp_model.no_sync():
                start_local = time.time()
                loss.backward()
                local_time = time.time() - start_local
            
            start_sync = time.time()
            for param in ddp_model.parameters():
                if param.grad is not None:
                    dist.all_reduce(param.grad)
                    
            sync_time = time.time() - start_sync

            bytes_this_iter = sum(
                p.grad.numel() * p.grad.element_size()
                for p in ddp_model.parameters() if p.grad is not None
            )
            mbps = bytes_this_iter / sync_time / 1e6 if sync_time > 0 else 0.0
            
            total_bytes += bytes_this_iter
            total_sync_time += sync_time

            optimizer.step()

    if local_rank == 0 and total_sync_time > 0:
        print("=" * 75)
        avg_mbps = total_bytes / total_sync_time / 1e6
        print(f"Average gradient sync throughput: {avg_mbps:.1f} MB/s over {num_epochs} epochs")
        print("=" * 75)
        
if __name__ == "__main__":
    main()
