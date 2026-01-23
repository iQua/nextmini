#!/bin/bash
reset

# Set environment to current script directory
export THIS_SHELL_SCRIPT_FULL_PATH=$(readlink -f "$0")
export COMPOSE_PATH=$(dirname "$THIS_SHELL_SCRIPT_FULL_PATH")
pushd $COMPOSE_PATH

InstallCargoOnce() {

    if [ ! $(command -v cargo) ]; then
        echo "Installing cargo & rust once..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        echo "source $HOME/.cargo/env" >> $HOME/.bashrc
    else
        echo "Cargo found!"
    fi

    export PATH="/${HOME}/.cargo/bin:${PATH}"

}

InstallCcCompilerToolchainOnce() {

    if [ ! $(command -v cmake) ]; then
        echo "Installing CC compiler toolchain once..."
        sudo apt-get update && \
        sudo apt-get install -y \
        openssh-client \
        openssh-server \
        build-essential \
        cmake \
        pkg-config && \
        sudo apt-get clean

        # Install utilities
        sudo apt-get update && \
        sudo apt-get install -y \
        curl\
        git\
        vim \
        net-tools && \
        sudo apt-get clean
    else
        echo "CC compiler toolchain found!"
    fi
}

ComposeDocs() {

    pushd docs
        sudo docker compose -f docker-compose.yml build --parallel
    popd
}

ComposeController() {

    pushd controller
        sudo docker compose -f docker-compose.yml build --parallel
    popd
}

ComposeDataplane() {

    pushd dataplane
        sudo docker compose -f docker-compose.yml build --parallel
    popd
}

ComposeAllResources() {

    InstallCcCompilerToolchainOnce

    InstallCargoOnce

    # Build everything for coverage
    cargo run -p cert-gen
    cargo build -p controller # debug
    cargo build -p nextmini   # debug
    cargo build -p controller --release
    cargo build -p nextmini   --release

    ComposeDocs
    ComposeController
    ComposeDataplane

    sudo docker run -itd nextmini_docs
    sudo docker run -itd nextmini_controller ./target/release/controller
    sudo docker run      nextmini_datapath   ./target/release/nextmini

}
ComposeAllResources
popd