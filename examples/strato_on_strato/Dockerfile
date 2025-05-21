FROM horovod/horovod-cpu

RUN apt-get update && \
    apt-get install -y \
    curl\
    openssh-client\
    openssh-server\
    git\
    vim \
    build-essential\
    net-tools \
    netcat \
    python3 \
    openssl &&\
    apt-get clean

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo "source $HOME/.cargo/env" >> $HOME/.bashrc
ENV PATH="/root/.cargo/bin:${PATH}"

# Create parent directory and copy messages
RUN mkdir -p /var
WORKDIR /var
COPY messages /var/messages

# Setup the dataplane directory
RUN mkdir -p /var/node
WORKDIR /var/node

# Copy dataplane source code
COPY dataplane/Cargo.toml .
COPY dataplane/src src

# Run the certificate generator first
RUN /bin/bash -c "cargo run --bin cert-gen"

# Build the dataplane
RUN /bin/bash -c "cargo build --release"

WORKDIR /horovod/examples
