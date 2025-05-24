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

# Setup the dataplane directory
WORKDIR /var/nextmini

# Copy the source code
COPY Cargo.toml /var/nextmini/Cargo.toml
COPY controller /var/nextmini/controller
COPY messages /var/nextmini/messages
COPY cert-gen /var/nextmini/cert-gen
COPY dataplane/Cargo.toml /var/nextmini/dataplane/Cargo.toml
COPY dataplane/src /var/nextmini/dataplane/src

# Run the certificate generator first from the root directory
# This will generate the certificate files in /var/nextmini/
WORKDIR /var/nextmini
RUN cargo run -p cert-gen

# Build the dataplane from source
RUN cargo build -p nextmini --release

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
    openssl

ENV RUST_LOG=info

COPY --from=build /var/nextmini/target/release/nextmini /var/nextmini/nextmini
COPY --from=build /var/nextmini/server_cert.pem /var/nextmini/server_cert.pem
COPY --from=build /var/nextmini/server_key.pem /var/nextmini/server_key.pem

WORKDIR /var/nextmini
