FROM rust:latest AS build

# Use USTC mirror for Debian apt
RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list.d/debian.sources

# Use USTC mirror for Cargo/crates.io
ENV CARGO_HTTP_MULTIPLEXING=false
RUN mkdir -p ~/.cargo && echo '[source.crates-io]' > ~/.cargo/config.toml && \
    echo 'replace-with = "ustc"' >> ~/.cargo/config.toml && \
    echo '[source.ustc]' >> ~/.cargo/config.toml && \
    echo 'registry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"' >> ~/.cargo/config.toml

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    bash \
    build-essential \
    libssl-dev \
    ca-certificates \
    pkg-config && \
    rm -rf /var/lib/apt/lists/*

# First, create directories and copy files
WORKDIR /var/nextmini
COPY messages /var/nextmini/messages
COPY controller /var/nextmini/server

WORKDIR /var/nextmini/server

# Build the Rust controller
RUN cargo build --release

FROM ubuntu:24.04

# Use USTC mirror for Ubuntu apt
RUN sed -i 's/archive.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list.d/ubuntu.sources && \
    sed -i 's/security.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list.d/ubuntu.sources

ENV TZ=Asia/Shanghai
ENV DEBIAN_FRONTEND=noninteractive
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    bash \
    openssh-client \
    openssh-server \
    git \
    vim \
    build-essential \
    net-tools \
    iputils-ping \
    iperf3 \
    openssl \
    ca-certificates \
    htop && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /var/nextmini
COPY --from=build /var/nextmini/server/target/release/controller /var/nextmini/controller

EXPOSE 3000
