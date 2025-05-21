FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && \
    apt-get install -y \
    curl \
    openssh-client \
    openssh-server \
    git \
    vim \
    build-essential \
    net-tools \
    pkg-config \
    libssl-dev \
    libpq-dev \
    ca-certificates && \
    apt-get clean

ENV INIT_POSTGRES=true
# Install PostgreSQL
RUN apt-get update && apt-get install -y postgresql
# Configure PostgreSQL to listen on all interfaces
RUN echo "listen_addresses='*'" >> $(ls /etc/postgresql/*/main/postgresql.conf)
# Allow all hosts to connect to PostgreSQL
RUN echo "host all all 0.0.0.0/0 md5" >> $(ls /etc/postgresql/*/main/pg_hba.conf)

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# First, create directories and copy files
WORKDIR /var/strato
COPY messages /var/strato/messages
COPY controller /var/strato/server

WORKDIR /var/strato/server

COPY controller/entrypoint.sh /
RUN chmod +x /entrypoint.sh

# Build the Rust controller
RUN cargo build --release

EXPOSE 5432
EXPOSE 3000

ENTRYPOINT ["/entrypoint.sh"]
