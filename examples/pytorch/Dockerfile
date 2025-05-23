FROM rust:alpine AS build

USER root

RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache \
    curl \
    bash \
    openssh-client \
    openssh-server \
    git \
    vim \
    build-base \
    net-tools \
    iperf3 \
    iputils-ping \
    openssl \
    openmpi \
    openmpi-dev \
    gcc

# Setup the dataplane directory
WORKDIR /var/nextmini

# Copy the source code
COPY messages /var/messages
COPY dataplane/Cargo.toml /var/nextmini
COPY dataplane/src /var/nextmini/src

# running the certificate generator first
RUN /bin/bash -c "cargo run --bin cert-gen"

# build the dataplane from source
RUN /bin/bash -c "cargo build --release"

FROM alpine:latest

RUN apk update && \
    apk upgrade --no-cache && \
    apk add curl bash \
    openssh-client \
    openssh-server \
    git \
    vim \
    build-base \
    net-tools \
    iputils-ping \
    iperf3 \
    openssl \
    openmpi \
    python3 \
    python3-dev \
    py3-pip

# Generate ssh host keys in /etc/ssh and start the sshd service
RUN ssh-keygen -A
RUN /usr/sbin/sshd

# Install uv using the official installer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

# Verify uv installation
RUN uv --version

# Install uv using the official installer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

ENV RUST_LOG=info
WORKDIR /var/nextmini

COPY --from=build /var/nextmini/target/release/nextmini /var/nextmini/nextmini
COPY --from=build /var/nextmini/server_cert.pem /var/nextmini/server_cert.pem
COPY --from=build /var/nextmini/server_key.pem /var/nextmini/server_key.pem
