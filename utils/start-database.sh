#!/usr/bin/env bash
# Use this script to start a docker container for a local development database

# TO RUN ON WINDOWS:
# 1. Install WSL (Windows Subsystem for Linux) - https://learn.microsoft.com/en-us/windows/wsl/install
# 2. Install Docker Desktop for Windows - https://docs.docker.com/docker-for-windows/install/
# 3. Open WSL - `wsl`
# 4. Run this script - `./start-database.sh`

# On Linux and macOS you can run this script directly - `./start-database.sh`

DB_CONTAINER_NAME="nextmini-database"

container_publishes_port() {
  docker port "$DB_CONTAINER_NAME" 5432 >/dev/null 2>&1
}

if ! [ -x "$(command -v docker)" ]; then
  echo -e "Docker is not installed. Please install Docker Desktop or OrbStack and try again."
  exit 1
fi

if ! docker info > /dev/null 2>&1; then
  echo "Docker daemon is not running. Please start Docker and try again."
  exit 1
fi

if [ "$(docker ps -q -f name=$DB_CONTAINER_NAME)" ]; then
  if ! container_publishes_port; then
    echo "Database container '$DB_CONTAINER_NAME' is already running, but it is not publishing host port 5432." >&2
    echo "Remove and recreate it so host processes can reach PostgreSQL at 127.0.0.1:5432." >&2
    exit 1
  fi
  echo "Database container '$DB_CONTAINER_NAME' is already running."
  exit 0
fi

if [ "$(docker ps -q -a -f name=$DB_CONTAINER_NAME)" ]; then
  if ! container_publishes_port; then
    echo "Existing database container '$DB_CONTAINER_NAME' does not publish host port 5432." >&2
    echo "Remove and recreate it so host processes can reach PostgreSQL at 127.0.0.1:5432." >&2
    exit 1
  fi
  docker start "$DB_CONTAINER_NAME"
  echo "Existing database container '$DB_CONTAINER_NAME' has been started."
  exit 0
fi

# import env variables from .env
set -a
source .env

DB_PASSWORD=$(echo "$DATABASE_URL" | awk -F':' '{print $3}' | awk -F'@' '{print $1}')
DB_PORT=$(echo "$DATABASE_URL" | awk -F':' '{print $4}' | awk -F'/' '{print $1}')

if [ "$DB_PASSWORD" = "password" ]; then
  echo "You are using the default database password."
  read -p "Should we generate a random password for you? [y/N]: " -r REPLY
  if ! [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Please change the default password in the .env file and try again."
    exit 1
  fi
  # Generate a random URL-safe password
  DB_PASSWORD=$(openssl rand -base64 12 | tr '+/' '-_')
  sed -i -e "s#:password@#:$DB_PASSWORD@#" .env
fi

echo $DB_PASSWORD

docker run -d \
  --name $DB_CONTAINER_NAME \
  -e POSTGRES_USER="pgusr" \
  -e POSTGRES_PASSWORD="$DB_PASSWORD" \
  -e POSTGRES_DB=nextmini \
  -p "$DB_PORT":5432 \
  docker.io/postgres && echo "Database container '$DB_CONTAINER_NAME' has successfully been created and started."
