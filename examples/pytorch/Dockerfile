FROM rust:alpine AS build

USER root

RUN apk update && \
    apk upgrade --no-cache && \
    apk add --no-cache \
    curl \
    bash \
    build-base \
    gcc

# Setup the dataplane directory
WORKDIR /var/nextmini

# Copy the source code
COPY Cargo.toml /var/nextmini/Cargo.toml
COPY controller /var/nextmini/controller
COPY messages /var/nextmini/messages
COPY cert-gen /var/nextmini/cert-gen
COPY python-api /var/nextmini/python-api
COPY mettle /var/nextmini/mettle
COPY dataplane/Cargo.toml /var/nextmini/dataplane/Cargo.toml
COPY dataplane/src /var/nextmini/dataplane/src

# running the certificate generator first
RUN /bin/bash -c "cargo run -p cert-gen"

# build the dataplane from source
RUN /bin/bash -c "cargo build -p nextmini --release"

FROM bitnami/minideb:latest

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
    libopenmpi-dev && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

# Generate ssh host keys in /etc/ssh and start the sshd service
RUN ssh-keygen -A

# Install uv using the official installer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

# Install Python
RUN uv python install

ENV RUST_LOG=info
WORKDIR /var/nextmini

COPY --from=build /var/nextmini/target/release/nextmini /var/nextmini/nextmini
COPY --from=build /var/nextmini/server_cert.pem /var/nextmini/server_cert.pem
COPY --from=build /var/nextmini/server_key.pem /var/nextmini/server_key.pem
