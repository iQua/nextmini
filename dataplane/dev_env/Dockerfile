FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && \
    apt-get install -y \
    openssh-client \
    openssh-server \
    build-essential \
    cmake \
    pkg-config &&\
    apt-get clean

# Install utilities
RUN apt-get update && \
    apt-get install -y \
    curl\
    git\
    vim \
    net-tools &&\
    apt-get clean

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo "source $HOME/.cargo/env" >> $HOME/.bashrc
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /var/strato
