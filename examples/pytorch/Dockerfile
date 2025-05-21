FROM ubuntu:24.04

USER root

RUN apt-get update && \
    apt-get install -y \
    curl\
    openssh-client \
    openssh-server \
    git\
    vim \
    build-essential \
    net-tools \
    iperf3 \
    inetutils-ping \
    openssl \
    openmpi-bin \
    libopenmpi-dev \
    gcc && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo "source $HOME/.cargo/env" >> $HOME/.bashrc

# Install uv using the official installer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

# Verify uv installation
RUN uv --version

# Create parent directory and copy messages
RUN mkdir -p /var
WORKDIR /var
COPY messages /var/messages

# Setup the dataplane directory
RUN mkdir -p /var/node
WORKDIR /var/node

# Copy the source code
COPY dataplane/Cargo.toml .
COPY dataplane/src src

# running the certificate generator first
RUN /bin/bash -c "cargo run --bin cert-gen"

# build the dataplane from source
RUN /bin/bash -c "cargo build --release"
