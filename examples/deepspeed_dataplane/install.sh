apt-get update && \
apt-get install -y \
        build-essential\
        net-tools \
        netcat \
        iperf3 &&\
apt-get clean

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

echo "source $HOME/.cargo/env" >> $HOME/.bashrc

/root/.cargo/bin/cargo install tokio-console

/bin/bash -c "source /root/.bashrc"

cargo build --release

