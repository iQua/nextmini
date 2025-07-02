FROM --platform=linux/amd64 ubuntu:22.04

# Install minimal runtime dependencies
RUN apt-get update && \
    apt-get install -y \
    libelf1 \
    python3 \
    curl \
    procps \
    iproute2 \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the pre-built binary from host
COPY target/release/aya /usr/local/bin/aya

# Make it executable
RUN chmod +x /usr/local/bin/aya

# Set environment
ENV RUST_LOG=info

# Expose common ports for testing
EXPOSE 8080

# Keep container running for testing
CMD ["tail", "-f", "/dev/null"]
