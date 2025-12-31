FROM python:3.13-slim

# Minimal runtime + build deps for:
# - building nextmini_py (maturin + Rust toolchain)
# - running mFlow (numpy + cvxopt)
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
        gfortran \
        libblas-dev \
        liblapack-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust toolchain (needed for building nextmini_py).
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

# Python deps for the toy demo (LP solver + wheel builder + DB access).
RUN pip install --no-cache-dir maturin numpy cvxopt "psycopg[binary]"

WORKDIR /workspace
COPY . /workspace

# Build and install nextmini_py once at image build time.
RUN maturin build --release -m python-api/Cargo.toml -F python-extension -o /tmp/wheels \
    && pip install --no-cache-dir /tmp/wheels/nextmini_py-*.whl

ENV PYTHONPATH=/workspace

CMD ["sleep", "infinity"]

