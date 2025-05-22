#!/bin/bash
set -e

export POSTGRES_USER=pgusr
export POSTGRES_PASSWORD=pgpwrd
export POSTGRES_DB=nextmini

# Wait for PostgreSQL to start
sleep 1

# Run init.sql script
if $INIT_POSTGRES ; then
    echo "Initializing PostgreSQL..."
    su - postgres -c "psql -f /var/nextmini/server/init.sql"
    INIT_POSTGRES=false
    echo "PostgreSQL initialized."
fi

echo "Initialization finished."

# Execute the command (should be the Rust binary)
echo "Starting controller..."
exec "/var/nextmini/server/target/release/controller"
