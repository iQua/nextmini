FROM deepspeed/deepspeed:latest

RUN apt-get update && \
    apt-get install -y \
    build-essential\
    net-tools \
    netcat \
    iperf3 &&\
    apt-get clean

RUN pip install transformers \
    datasets \
    pytorch_lightning \
    lightning_horovod

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo "source $HOME/.cargo/env" >> $HOME/.bashrc
ENV PATH="/root/.cargo/bin:${PATH}"
RUN /root/.cargo/bin/cargo install tokio-console

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
