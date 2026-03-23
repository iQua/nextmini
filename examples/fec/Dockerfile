FROM python:3.13-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends curl build-essential libssl-dev pkg-config ca-certificates libpq-dev libpq5 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir maturin
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /app
COPY . /app
RUN maturin build --release -m python-api/Cargo.toml -F python-extension

FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates iproute2 iputils-ping net-tools libpq5 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . /app
COPY --from=build /app/target/wheels/nextmini_py-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/nextmini_py-*.whl && rm -f /tmp/nextmini_py-*.whl
ENV PYTHONUNBUFFERED=1
CMD ["sleep", "infinity"]
