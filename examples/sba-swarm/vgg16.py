# train_vgg16_ddp.py
#
# CPU-only Distributed Data Parallel training script for VGG-16.
# Launch with MPI (same env vars as lenet5.py), for example:
#   mpirun -np 4 python vgg16.py --data_dir /path/to/imagenet_subset
#
# Required environment variables provided by mpirun:
#   OMPI_COMM_WORLD_LOCAL_RANK – local rank on the node
#   OMPI_COMM_WORLD_RANK       – global rank
#
# Uses the "gloo" backend so it works on machines without GPUs.

import argparse
import os
import random
import time

import numpy as np
import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
import torchvision
import torchvision.transforms as transforms


def set_random_seeds(seed: int = 0) -> None:
    """Seed all RNGs for reproducibility (matches lenet5.py)."""
    torch.manual_seed(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    np.random.seed(seed)
    random.seed(seed)


def main() -> None:
    # --------------------------- CLI --------------------------- #
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=None,
        help="Root directory holding ImageFolder-style data. "
        "If omitted, a synthetic FakeData dataset is used.",
    )
    parser.add_argument("--batch_size", type=int, default=32)
    parser.add_argument("--num_epochs", type=int, default=10)
    parser.add_argument("--learning_rate", type=float, default=0.01)
    parser.add_argument("--workers", type=int, default=0)  # Changed from 4 to 0
    parser.add_argument("--random_seed", type=int, default=0)
    args = parser.parse_args()

    # ---------------------- DDP initialisation ---------------------- #
    local_rank = int(os.environ["OMPI_COMM_WORLD_LOCAL_RANK"])
    world_rank = int(os.environ["OMPI_COMM_WORLD_RANK"])

    dist.init_process_group(backend="gloo")
    set_random_seeds(args.random_seed)

    # --------------------------- Dataset --------------------------- #
    transform = transforms.Compose(
        [
            transforms.Resize(224),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=(0.485, 0.456, 0.406),
                std=(0.229, 0.224, 0.225),
            ),
        ]
    )

    if args.data_dir:
        train_set = torchvision.datasets.ImageFolder(
            root=args.data_dir, transform=transform
        )
    else:
        # Fallback to a synthetic dataset so the script works out-of-the-box
        train_set = torchvision.datasets.FakeData(
            size=10_000,
            image_size=(3, 224, 224),
            num_classes=1_000,
            transform=transform,
        )

    train_sampler = DistributedSampler(train_set)
    train_loader = DataLoader(
        train_set,
        batch_size=args.batch_size,
        sampler=train_sampler,
        num_workers=args.workers,
    )

    # ---------------------------- Model ---------------------------- #
    model = torchvision.models.vgg16(weights=None)
    device = torch.device("cpu")
    model.to(device)
    ddp_model = DDP(model)

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(
        ddp_model.parameters(),
        lr=args.learning_rate,
        momentum=0.9,
        weight_decay=1e-4,
    )

    for epoch in range(args.num_epochs):
        print(f"World Rank: {world_rank}, Epoch: {epoch}")
        train_sampler.set_epoch(epoch)
        ddp_model.train()
        total_loss = 0.0

        for images, labels in train_loader:
            start_time = time.time()

            images, labels = images.to(device), labels.to(device)

            optimizer.zero_grad()
            outputs = ddp_model(images)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

            batch_time = time.time() - start_time
            print(f"Batch time: {batch_time:.2f} s")
            total_loss += loss.item() * images.size(0)
            break

        if world_rank == 0:
            mean_loss = total_loss / len(train_loader.dataset)
            print(f"[Epoch {epoch + 1}/{args.num_epochs}] Loss: {mean_loss:.4f}")

    dist.destroy_process_group()


if __name__ == "__main__":
    main()
