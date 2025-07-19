#!/usr/bin/env bash

# Start OpenVSwitch service
service openvswitch-switch start
ovs-vsctl set-manager ptcp:6640

# Start the OpenVSwitch controller listening on port 6633
ovs-testcontroller ptcp:6633 &

# Start SSH service
service ssh start || /usr/sbin/sshd

if [ $# -gt 0 ]
then
  if [ "$1" == "mn" ]
  then
    bash -c "$@"
  else
    mn "$@"
  fi
else
  bash
fi

# Kill the controller before stopping
pkill ovs-testcontroller
service openvswitch-switch stop
service ssh stop
