#!/bin/bash
reset

export THIS_SHELL_SCRIPT_PATH=$(readlink -f "$0")
export THIS_DIR=$(dirname "$THIS_SHELL_SCRIPT_PATH")

cd $THIS_DIR

sudo docker compose -f docker-compose.yml build --parallel

sudo docker run -tid -p 8000:8000 nextmini/docs 