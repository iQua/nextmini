FROM rust:alpine AS build

RUN apk update && \
    apk upgrade --no-cache && \
    apk add curl bash \
    build-base \
    openssl \
    ca-certificates

# First, create directories and copy files
WORKDIR /var/nextmini
COPY messages /var/nextmini/messages
COPY controller /var/nextmini/server

WORKDIR /var/nextmini/server

# Build the Rust controller
RUN cargo build --release

FROM alpine:latest

ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

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
    ca-certificates

WORKDIR /var/nextmini
COPY --from=build /var/nextmini/server/target/release/controller /var/nextmini/controller

EXPOSE 3000
