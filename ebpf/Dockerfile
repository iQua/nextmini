FROM rust:alpine AS build

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

# Setup the ebpf directory
WORKDIR /var/ebpf

# Copy the source code
COPY Cargo.toml /var/ebpf/Cargo.toml
COPY src /var/ebpf/src

# Build all binaries from source
RUN cargo build --release

FROM alpine:latest

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
    openssl \
    htop

ENV RUST_LOG=info

COPY --from=build /var/ebpf/target/release/server /var/ebpf/server
COPY --from=build /var/ebpf/target/release/client /var/ebpf/client
COPY --from=build /var/ebpf/target/release/ebpf /var/ebpf/proxy

WORKDIR /var/ebpf 