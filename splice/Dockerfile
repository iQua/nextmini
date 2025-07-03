FROM rust:1.85 as builder

WORKDIR /app
COPY Cargo.toml .
COPY src/ src/

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tcp-proxy-zerocopy /usr/local/bin/tcp-proxy

EXPOSE 8080

CMD ["tcp-proxy"]
