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
    openvswitch-switch-dpdk \
    openvswitch-common \
    python3-pip \
    python3-setuptools \
    openssh-server \
    tcpdump \
    vim \
    wget \
    x11-xserver-utils \
    xterm \
    git \
    build-essential \
    autoconf \
    automake \
    libtool \
    pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && touch /etc/network/interfaces \
    && chmod +x /ENTRYPOINT.sh \
    && ln -s /usr/bin/ovs-testcontroller /usr/bin/controller \
    && git clone https://github.com/mininet/openflow.git \
    && cd openflow \
    && ./boot.sh \
    && ./configure \
    && make \
    && make install \
    && cd / && rm -rf openflow \
    && mkdir -p /var/run/sshd \
    && echo 'root:root' | chpasswd \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

EXPOSE 6633 6653 6640 22

ENTRYPOINT ["/ENTRYPOINT.sh"]
