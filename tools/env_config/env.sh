#!/bin/bash

IF_NAME="ens3"
# Get the local IPv4 address from the ens3 interface
LOCAL_STRATO_ADDR=$(ip addr show $IF_NAME | grep 'inet ' | awk '{print $2}' | cut -d/ -f1)
LOCAL_STRATO_PORT=6688
STRATO_NODE_ID=$1
# Backup the existing .bashrc file
cp ~/.bashrc ~/.bashrc.backup

# Add the environment variable to the end of the .bashrc file
echo "export LOCAL_STRATO_ADDR=$LOCAL_STRATO_ADDR" >> ~/.bashrc
echo "export LOCAL_STRATO_PORT=$LOCAL_STRATO_PORT" >> ~/.bashrc
echo "export STRATO_NODE_ID=$STRATO_NODE_ID" >> ~/.bashrc

# Reload .bashrc or inform the user to restart their shell
echo "Added strato variables to ~/.bashrc. Please restart your shell or source ~/.bashrc to apply changes."
