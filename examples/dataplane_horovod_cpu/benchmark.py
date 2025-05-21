import torch
import horovod.torch as hvd
import time

def benchmark_allreduce(tensor_size, data_type, num_iterations):
    # Initialize Horovod
    hvd.init()

    # Check if Horovod is using GPU or CPU
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    # Create a random tensor of the given size and data type on each process
    tensor = torch.rand(tensor_size, dtype=data_type).to(device)

    # Synchronize to ensure all processes have initialized the tensor
    hvd.join()

    # Perform the allreduce operation and measure the time taken
    start_time = time.time()
    for _ in range(num_iterations):
        hvd.allreduce(tensor)
    elapsed_time = time.time() - start_time

    # Compute the total number of bits transferred
    element_size_in_bits = torch.finfo(data_type).bits
    total_bits = tensor_size * element_size_in_bits

    total_bits_transferred = total_bits * hvd.size() * num_iterations

    # Calculate the network bandwidth in Mbits/s
    bandwidth_mbits = total_bits_transferred / (elapsed_time * 1e6)

    # Print the results
    print(f"Tensor Size: {tensor_size}, Data Type: {data_type}, Num Iterations: {num_iterations}")
    print(f"Total Time taken: {elapsed_time:.6f} seconds")
    print(f"Network Bandwidth: {bandwidth_mbits:.2f} Mbits/s")

if __name__ == "__main__":
    # Define parameters for benchmarking
    tensor_size = 10000000  # Adjust this according to your desired tensor size
    data_type = torch.float32  # Adjust the data type (e.g., torch.float32, torch.float64, torch.int32, etc.)
    num_iterations = 10   # Number of iterations for averaging

    # Run the benchmark
    benchmark_allreduce(tensor_size, data_type, num_iterations)





