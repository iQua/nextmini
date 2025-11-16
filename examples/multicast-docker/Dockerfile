FROM python:3.13-slim

# Install Rust and build dependencies (Debian/glibc based)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        build-essential \
        libssl-dev \
        pkg-config \
        ca-certificates \
        libpq-dev \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Install uv early so helper scripts can create virtualenvs quickly.
RUN pip install --no-cache-dir uv

# Install Rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace

CMD ["sleep", "infinity"]
