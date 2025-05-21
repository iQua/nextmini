# Install basic tools
apt-get update && \
apt-get install -y \
        build-essential\
        net-tools \
        netcat \
        iperf3 &&\
apt-get clean

# Install openmpi
apt-get update &&\
apt-get install -y \
        openmpi-bin \
        openmpi-doc \
        libopenmpi-dev &&\
apt-get clean

# Install horovod
HOROVOD_WITH_PYTORCH=1 pip install horovod[pytorch]

# Install python dependencies
pip install transformers \
        datasets \
        pytorch_lightning \
        lightning_horovod

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
echo "source $HOME/.cargo/env" >> $HOME/.bashrc
/root/.cargo/bin/cargo install tokio-console
/bin/bash -c "source /root/.bashrc"
cargo build --release



