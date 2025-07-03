# Multi-stage build for eBPF loopback application
FROM --platform=linux/amd64 ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && \
    apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libelf-dev \
    clang \
    llvm \
    gcc-multilib \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.83
ENV PATH="/root/.cargo/bin:${PATH}"

# Add necessary Rust components
RUN rustup component add rustfmt
RUN rustup toolchain install nightly
RUN rustup default nightly
RUN rustup component add rust-src
RUN cargo install bpf-linker

# Set working directory
WORKDIR /app

# Copy source code
COPY . .

# Build the application
RUN cargo build --release

# Runtime stage
FROM --platform=linux/amd64 ubuntu:22.04 AS runtime

# Install minimal runtime dependencies
RUN apt-get update && \
    apt-get install -y \
    libelf1 \
    python3 \
    curl \
    procps \
    iproute2 \
    net-tools \
    iptables \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the built binary from builder stage
COPY --from=builder /app/target/release/loopback-ebpf /usr/local/bin/loopback-ebpf

# Make it executable
RUN chmod +x /usr/local/bin/loopback-ebpf

# Set environment
ENV RUST_LOG=info

# Expose common ports for testing
EXPOSE 8080

# Default command
CMD ["loopback-ebpf"]
