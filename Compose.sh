#!/bin/bash
reset

# Set environment to current script directory
export THIS_SHELL_SCRIPT_FULL_PATH=$(readlink -f "$0")
export COMPOSE_PATH=$(dirname "$THIS_SHELL_SCRIPT_FULL_PATH")

pushd $COMPOSE_PATH

    # Build everything for coverage
    cargo run -p cert-gen
    cargo build -p controller # debug
    cargo build -p nextmini   # debug
    cargo build -p controller --release
    cargo build -p nextmini   --release

    pushd docs
        sudo docker compose -f docker-compose.yml build --parallel
    popd

    pushd controller
        sudo docker compose -f docker-compose.yml build --parallel
    popd

    pushd dataplane
        sudo docker compose -f docker-compose.yml build --parallel
    popd

    sudo docker run -itd nextmini_controller ./target/release/controller

    sudo docker run      nextmini_datapath   ./target/release/nextmini

popd