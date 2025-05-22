FROM rust:alpine

ENV DEBIAN_FRONTEND=noninteractive

RUN apk update && \
    apk upgrade --no-cache && \
    apk add curl bash \
    openssh-client \
    openssh-server \
    git\
    vim \
    build-base \
    net-tools \
    iputils-ping \
    iperf3 \
    openssl

# Set the log level
ENV RUST_LOG=info

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

# Run the certificate generator first
RUN cargo run --bin cert-gen

# Build the dataplane from source
RUN cargo build --release
