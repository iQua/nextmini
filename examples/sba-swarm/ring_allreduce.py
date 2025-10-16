"""
Ring All-Reduce implementation using MPI4Py

This implements the bandwidth-optimal ring all-reduce algorithm where each node
sends and receives data from its neighbors in a ring topology.

Algorithm:
1. Scatter-Reduce: Each node sends different chunks to neighbors in N-1 steps
2. All-Gather: Each node sends reduced chunks to complete the all-reduce in N-1 steps

Total: 2(N-1) steps for N nodes
Communication volume per node: 2(N-1)/N * data_size ≈ 2 * data_size (for large N)
"""

import argparse
import os
import time
import numpy as np
from mpi4py import MPI


def ring_allreduce(send_buf, comm):
    """
    Perform ring all-reduce on a numpy array.
    
    Args:
        send_buf: Input numpy array to reduce
        comm: MPI communicator
    
    Returns:
        Reduced array (sum across all ranks)
    """
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    if size == 1:
        return send_buf.copy()
    
    # Initialize result buffer
    result = send_buf.copy()
    
    # Divide data into chunks (one per rank)
    chunk_size = len(send_buf) // size
    remainder = len(send_buf) % size
    
    chunks = []
    offset = 0
    for i in range(size):
        # Distribute remainder across first chunks
        curr_chunk_size = chunk_size + (1 if i < remainder else 0)
        chunks.append((offset, offset + curr_chunk_size))
        offset += curr_chunk_size
    
    # Compute neighbors in the ring
    left_neighbor = (rank - 1 + size) % size
    right_neighbor = (rank + 1) % size
    
    print(f"[Rank {rank}] Ring topology: {left_neighbor} <- {rank} -> {right_neighbor}")
    
    # ============ PHASE 1: Scatter-Reduce (N-1 steps) ============
    # In each step, send chunk to right neighbor and reduce with chunk from left
    print(f"[Rank {rank}] Starting Scatter-Reduce phase...")
    
    for step in range(size - 1):
        # Determine which chunk to send and which to receive
        send_chunk_idx = (rank - step + size) % size
        recv_chunk_idx = (rank - step - 1 + size) % size
        
        send_start, send_end = chunks[send_chunk_idx]
        recv_start, recv_end = chunks[recv_chunk_idx]
        
        send_data = result[send_start:send_end].copy()
        recv_data = np.empty(recv_end - recv_start, dtype=send_buf.dtype)
        
        # Simultaneous send and receive
        req_send = comm.Isend(send_data, dest=right_neighbor, tag=step)
        req_recv = comm.Irecv(recv_data, source=left_neighbor, tag=step)
        
        req_send.Wait()
        req_recv.Wait()
        
        # Reduce received data into our buffer
        result[recv_start:recv_end] += recv_data
        
        print(f"[Rank {rank}] Step {step}: sent chunk {send_chunk_idx}, reduced chunk {recv_chunk_idx}")
    
    # ============ PHASE 2: All-Gather (N-1 steps) ============
    # In each step, send reduced chunk to right neighbor
    print(f"[Rank {rank}] Starting All-Gather phase...")
    
    for step in range(size - 1):
        # Determine which chunk to send (the one we just reduced in last phase)
        send_chunk_idx = (rank - step + 1 + size) % size
        recv_chunk_idx = (rank - step + size) % size
        
        send_start, send_end = chunks[send_chunk_idx]
        recv_start, recv_end = chunks[recv_chunk_idx]
        
        send_data = result[send_start:send_end].copy()
        recv_data = np.empty(recv_end - recv_start, dtype=send_buf.dtype)
        
        # Simultaneous send and receive
        req_send = comm.Isend(send_data, dest=right_neighbor, tag=size + step)
        req_recv = comm.Irecv(recv_data, source=left_neighbor, tag=size + step)
        
        req_send.Wait()
        req_recv.Wait()
        
        # Overwrite with the reduced chunk from neighbor
        result[recv_start:recv_end] = recv_data
        
        print(f"[Rank {rank}] Step {step}: sent chunk {send_chunk_idx}, gathered chunk {recv_chunk_idx}")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Ring All-Reduce with MPI")
    parser.add_argument("--size", type=int, default=1000000, 
                        help="Size of the array to reduce (elements)")
    parser.add_argument("--verify", action="store_true",
                        help="Verify result against MPI.Allreduce")
    args = parser.parse_args()
    
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    if rank == 0:
        print(f"Running Ring All-Reduce with {size} processes")
        print(f"Array size: {args.size} elements ({args.size * 8 / 1e6:.2f} MB per rank)")
        print("=" * 60)
    
    # Create test data (each rank has different values)
    np.random.seed(rank)
    send_buf = np.random.randn(args.size).astype(np.float64)
    
    # Measure ring all-reduce time
    comm.Barrier()
    start_time = time.time()
    
    result = ring_allreduce(send_buf, comm)
    
    comm.Barrier()
    end_time = time.time()
    
    elapsed = end_time - start_time
    data_size_mb = args.size * 8 / 1e6
    bandwidth_mbps = data_size_mb / elapsed if elapsed > 0 else 0
    
    if rank == 0:
        print("=" * 60)
        print(f"Ring All-Reduce completed in {elapsed:.4f} seconds")
        print(f"Effective bandwidth: {bandwidth_mbps:.2f} MB/s per rank")
        print(f"Total data transferred per rank: {2 * (size - 1) / size * data_size_mb:.2f} MB")
    
    # Verification (optional)
    if args.verify:
        expected = np.empty_like(send_buf)
        comm.Allreduce(send_buf, expected, op=MPI.SUM)
        
        max_diff = np.max(np.abs(result - expected))
        if rank == 0:
            print("=" * 60)
            if max_diff < 1e-10:
                print(f"✓ Verification PASSED (max diff: {max_diff:.2e})")
            else:
                print(f"✗ Verification FAILED (max diff: {max_diff:.2e})")
    
    # Additional statistics
    if rank == 0:
        print("=" * 60)
        print(f"Result statistics:")
        print(f"  Sum: {np.sum(result):.2f}")
        print(f"  Mean: {np.mean(result):.2f}")
        print(f"  Std: {np.std(result):.2f}")


if __name__ == "__main__":
    main()

