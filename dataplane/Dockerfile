FROM alpine:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apk add curl bash \
  openssh-client \
  openssh-server \
  git\
  vim \
  build-base \
  net-tools \
  iputils-ping \
  iperf3 \
  openssl

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo "source $HOME/.cargo/env" >> $HOME/.bashrc
ENV PATH="/root/.cargo/bin:${PATH}"

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
RUN /bin/bash -c "cargo run --bin cert-gen"

# Build the dataplane from source
RUN /bin/bash -c "cargo build --release"
