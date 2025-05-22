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
COPY messages /var/messages
COPY dataplane/Cargo.toml /var/nextmini
COPY dataplane/src /var/nextmini/src

# Run the certificate generator first
RUN cargo run --bin cert-gen

# Build the dataplane from source
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
    openssl

ENV RUST_LOG=info
WORKDIR /var/nextmini

COPY --from=build /var/nextmini/target/release/nextmini /var/nextmini/nextmini
COPY --from=build /var/nextmini/server_cert.pem /var/nextmini/server_cert.pem
COPY --from=build /var/nextmini/server_key.pem /var/nextmini/server_key.pem
