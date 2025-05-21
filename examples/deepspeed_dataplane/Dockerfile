FROM horovod/horovod:latest

FROM nvidia/cuda:11.7.1-devel-ubuntu20.04

ENV DEBIAN_FRONTEND noninteractive

##############################################################################
# Temporary Installation Directory
##############################################################################
ENV STAGE_DIR=/tmp
RUN mkdir -p ${STAGE_DIR}

##############################################################################
# Installation/Basic Utilities
##############################################################################
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    software-properties-common build-essential autotools-dev \
    nfs-common pdsh \
    cmake g++ gcc \
    curl wget vim tmux emacs less unzip \
    htop iftop iotop ca-certificates openssh-client openssh-server \
    rsync iputils-ping net-tools sudo \
    llvm-9-dev

##############################################################################
# Installation Latest Git
##############################################################################
RUN add-apt-repository ppa:git-core/ppa -y && \
    apt-get update && \
    apt-get install -y git && \
    git --version

##############################################################################
# Client Liveness & Uncomment Port 22 for SSH Daemon
##############################################################################
# Keep SSH client alive from server side
RUN echo "ClientAliveInterval 30" >> /etc/ssh/sshd_config
RUN cp /etc/ssh/sshd_config ${STAGE_DIR}/sshd_config && \
    sed "0,/^#Port 22/s//Port 22/" ${STAGE_DIR}/sshd_config > /etc/ssh/sshd_config

##############################################################################
# Python
##############################################################################
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VERSION=3
RUN apt-get update && \
    apt-get install -y python3 python3-dev && \
    rm -f /usr/bin/python && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    curl -O https://bootstrap.pypa.io/pip/get-pip.py && \
    python get-pip.py && \
    rm get-pip.py && \
    pip install --upgrade pip && \
    # Print python an pip version
    python -V && pip -V
RUN pip install pyyaml
RUN pip install ipython


##############################################################################
# Some Packages
##############################################################################
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsndfile-dev \
    libcupti-dev \
    libjpeg-dev \
    libpng-dev \
    screen \
    libaio-dev
RUN pip install psutil \
    yappi \
    cffi \
    ipdb \
    pandas \
    matplotlib \
    py3nvml \
    pyarrow \
    graphviz \
    astor \
    boto3 \
    tqdm \
    sentencepiece \
    msgpack \
    requests \
    pandas \
    sphinx \
    sphinx_rtd_theme \
    scipy \
    numpy \
    scikit-learn \
    nvidia-ml-py3 \
    cupy-cuda100

##############################################################################
# PyTorch
##############################################################################
ENV PYTORCH_VERSION=1.9.0
ENV TORCHVISION_VERSION=0.10.0
ENV TENSORBOARDX_VERSION=1.8
RUN pip install torch==2.0.1+cu117 torchvision==0.15.2+cu117 torchaudio==2.0.2 --index-url https://download.pytorch.org/whl/cu117
RUN pip install tensorboardX==${TENSORBOARDX_VERSION}

##############################################################################
# PyYAML build issue
# https://stackoverflow.com/a/53926898
##############################################################################
RUN rm -rf /usr/lib/python3/dist-packages/yaml && \
    rm -rf /usr/lib/python3/dist-packages/PyYAML-*


##############################################################################
# DeepSpeed
##############################################################################
# RUN git clone https://github.com/microsoft/DeepSpeed.git ${STAGE_DIR}/DeepSpeed
# RUN cd ${STAGE_DIR}/DeepSpeed && \
#         git checkout . && \
#         git checkout master && \
#         ./install.sh --pip_sudo
# RUN rm -rf ${STAGE_DIR}/DeepSpeed
# RUN python -c "import deepspeed; print(deepspeed.__version__)"

RUN pip install deepspeed

##############################################################################
# SSH
##############################################################################
RUN mkdir -p /var/run/sshd && mkdir -p /run/sshd
RUN cat /etc/ssh/ssh_config | grep -v StrictHostKeyChecking > /etc/ssh/ssh_config.new && \
    echo "    StrictHostKeyChecking no" >> /etc/ssh/ssh_config.new && \
    mv /etc/ssh/ssh_config.new /etc/ssh/ssh_config

##############################################################################
# Strato Dependencies
##############################################################################

RUN apt-get update && \
    apt-get install -y \
    build-essential\
    pdsh \
    net-tools \
    netcat \
    iperf3

RUN python -V && pip -V && \
    pip install transformers \
    pytorch_lightning==2.0.9 \
    accelerate>=0.12.0 \
    datasets>=1.8.0 \
    sentencepiece!=0.1.92 \
    protobuf==3.20 \
    evaluate \
    bitsandbytes \
    peft \
    trl \
    tensorboard

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo "source $HOME/.cargo/env" >> $HOME/.bashrc
ENV PATH="/root/.cargo/bin:${PATH}"
RUN /bin/bash -c "cargo install tokio-console"

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
