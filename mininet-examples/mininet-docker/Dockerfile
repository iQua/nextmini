FROM ubuntu:22.04

USER root
WORKDIR /root

COPY ENTRYPOINT.sh /

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo \
    curl \
    dnsutils \
    ifupdown \
    iproute2 \
    iptables \
    iputils-ping \
    mininet \
    net-tools \
    openvswitch-switch \
    openvswitch-testcontroller \
    python3-pip \
    python3-setuptools \
    tcpdump \
    vim \
    x11-xserver-utils \
    xterm \
    && rm -rf /var/lib/apt/lists/* \
    && touch /etc/network/interfaces \
    && chmod +x /ENTRYPOINT.sh \
    && ln -s /usr/bin/ovs-testcontroller /usr/bin/controller

EXPOSE 6633 6653 6640

ENTRYPOINT ["/ENTRYPOINT.sh"]
