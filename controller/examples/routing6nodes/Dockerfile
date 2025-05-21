FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && \
    apt-get install -y \
        curl\
        openssh-client\
        openssh-server\
        git\
        vim \
        build-essential\
        net-tools \
        nodejs \
        npm &&\
    apt-get clean

# Install python3 and dependencies
RUN apt-get update && \
    apt-get install -y \
        python3-pip &&\
    apt-get clean

RUN pip install --upgrade pip && \
    pip install psycopg2-binary numpy torch pytorch_lightning pandas gurobipy sqlalchemy networkx
# Initialize Postgres database
ENV INIT_POSTGRES=true
# Install PostgreSQL
RUN apt-get update && apt-get install -y postgresql
# Configure PostgreSQL to listen on all interfaces
RUN echo "listen_addresses='*'" >> $(ls /etc/postgresql/*/main/postgresql.conf)
# Allow all hosts to connect to PostgreSQL
RUN echo "host all all 0.0.0.0/0 md5" >> $(ls /etc/postgresql/*/main/pg_hba.conf)

COPY . /var/strato/server
WORKDIR /var/strato/server

COPY ./entrypoint.sh /
RUN chmod +x /entrypoint.sh

EXPOSE 5432
EXPOSE 3000

ENTRYPOINT ["/entrypoint.sh"]
