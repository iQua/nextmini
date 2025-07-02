FROM rust:1.82-bookworm AS build

RUN apt-get update && \
    apt-get install -y \
    curl bash \
    openssh-client \
    openssh-server \
    git \
    vim \
    build-essential \
    net-tools \
    iputils-ping \
    iperf3 \
    openssl \
    clang \
    llvm \
    libelf-dev \
    zlib1g-dev \
    pkg-config \
    libbpf-dev \
    linux-libc-dev \
    && ln -sf /usr/include/x86_64-linux-gnu/asm /usr/include/asm

# Set the log level
ENV RUST_LOG=info

# Install rustfmt component
RUN rustup component add rustfmt

# Setup the ebpf directory
WORKDIR /var/ebpf

# Copy the source code
COPY Cargo.toml /var/ebpf/Cargo.toml
COPY build.rs /var/ebpf/build.rs
COPY src /var/ebpf/src
COPY ebpf /var/ebpf/ebpf

# Build all binaries from source
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y \
    curl bash \
    openssh-client \
    openssh-server \
    git \
    vim \
    net-tools \
    iputils-ping \
    iperf3 \
    openssl \
    htop \
    libelf1 \
    libbpf1

ENV RUST_LOG=info

COPY --from=build /var/ebpf/target/release/server /var/ebpf/server
COPY --from=build /var/ebpf/target/release/client /var/ebpf/client
COPY --from=build /var/ebpf/target/release/ebpf /var/ebpf/proxy

WORKDIR /var/ebpf 