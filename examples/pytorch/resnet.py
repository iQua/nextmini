import torch
from torch.utils.data.distributed import DistributedSampler
from torch.utils.data import DataLoader
import torch.nn as nn
import torch.optim as optim
import torch.distributed as dist
import time

import torchvision
import torchvision.transforms as transforms

import argparse
import os
import random
import numpy as np


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


def main():
    num_epochs_default = 3
    batch_size_default = 256
    learning_rate_default = 0.001
    random_seed_default = 0
    model_dir_default = "saved_models"
    model_filename_default = "resnet_distributed.pth"

    # Each process runs on 1 GPU device specified by the local_rank argument.
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--local_rank",
        type=int,
        help="Local rank. Necessary for using the torch.distributed.launch utility.",
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
        "--random_seed", type=int, help="Random seed.", default=random_seed_default
    )
    parser.add_argument(
        "--model_dir",
        type=str,
        help="Directory for saving models.",
        default=model_dir_default,
    )
    parser.add_argument(
        "--model_filename",
        type=str,
        help="Model filename.",
        default=model_filename_default,
    )
    parser.add_argument(
        "--type",
        type=str,
        default="resnet18",
        help="ResNet variant in torchvision.models (e.g., resnet18, resnet34, resnet50, ...)",
    )
    parser.add_argument(
        "--resume", action="store_true", help="Resume training from saved checkpoint."
    )
    argv = parser.parse_args()

    # Retrieve local rank from the MPI environment (CPU-only training)
    local_rank = int(os.environ["OMPI_COMM_WORLD_LOCAL_RANK"])
    world_rank = int(os.environ["OMPI_COMM_WORLD_RANK"])
    num_epochs = argv.num_epochs
    batch_size = argv.batch_size
    learning_rate = argv.learning_rate
    random_seed = argv.random_seed
    model_type = argv.type
    model_dir = argv.model_dir
    model_filename = argv.model_filename
    resume = argv.resume

    # Create directories outside the PyTorch program
    # Do not create directory here because it is not multiprocess safe
    """
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
    """

    model_filepath = os.path.join(model_dir, model_filename)

    # We need to use seeds to make sure that the models initialized in different processes are the same
    set_random_seeds(random_seed=random_seed)

    # Initializes the distributed backend which will take care of sychronizing nodes/GPUs
    # torch.distributed.init_process_group(backend="nccl")
    torch.distributed.init_process_group(backend="gloo")

    # Select the model variant
    if not hasattr(torchvision.models, model_type):
        raise ValueError(f"Unknown model variant {model_type} in torchvision.models")
    model_fn = getattr(torchvision.models, model_type)
    model = model_fn(pretrained=False)
    print(f"[Rank {local_rank}] Using model variant: {model_type}")

    device = torch.device("cpu")
    model = model.to(device)
    ddp_model = torch.nn.parallel.DistributedDataParallel(model)

    # Checkpointing disabled for this lightweight CPU run.

    # Prepare dataset and dataloader
    transform = transforms.Compose(
        [
            transforms.RandomCrop(32, padding=4),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
        ]
    )

    # Download the dataset only on rank 0, then let other ranks read the existing files
    if local_rank == 0:
        train_set = torchvision.datasets.CIFAR10(
            root="data", train=True, download=True, transform=transform
        )
        test_set = torchvision.datasets.CIFAR10(
            root="data", train=False, download=True, transform=transform
        )
        # Ensure download is finished before other ranks proceed
        dist.barrier()
    else:
        # Wait until rank 0 has finished downloading
        dist.barrier()
        train_set = torchvision.datasets.CIFAR10(
            root="data", train=True, download=False, transform=transform
        )
        test_set = torchvision.datasets.CIFAR10(
            root="data", train=False, download=False, transform=transform
        )

    # Restricts data loading to a subset of the dataset exclusive to the current process
    train_sampler = DistributedSampler(dataset=train_set)

    train_loader = DataLoader(
        dataset=train_set, batch_size=batch_size, sampler=train_sampler, num_workers=8
    )
    # Test loader does not have to follow distributed sampling strategy
    test_loader = DataLoader(
        dataset=test_set, batch_size=128, shuffle=False, num_workers=8
    )

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(
        ddp_model.parameters(), lr=learning_rate, momentum=0.9, weight_decay=1e-5
    )

    # Loop over the dataset multiple times
    for epoch in range(num_epochs):
        print("Local Rank: {}, Epoch: {}, Training ...".format(local_rank, epoch))

        if local_rank == 0:
            accuracy = evaluate(model=ddp_model, device=device, test_loader=test_loader)
            print("-" * 75)
            print(f"Epoch: {epoch}, Accuracy: {accuracy}")
            print("-" * 75)

        ddp_model.train()

        for data in train_loader:
            start_time = time.time()
            inputs, labels = data[0].to(device), data[1].to(device)
            optimizer.zero_grad()
            outputs = ddp_model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            end_time = time.time()
            print(f"Time taken: {end_time - start_time} seconds")
            break


if __name__ == "__main__":
    main()
