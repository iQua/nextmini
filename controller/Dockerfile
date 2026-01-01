FROM rust:latest AS build

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

ENV TZ=America/Toronto
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
