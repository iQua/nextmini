#!/bin/bash
set -e

# Start PostgreSQL server
service postgresql start

# Wait for PostgreSQL to start
sleep 1

# Run init.sql script
if $INIT_POSTGRES ; then
    echo "Initializing PostgreSQL..."
    chown -R postgres:postgres /var/strato/server/init.sql
    su - postgres -c "psql -f /var/strato/server/init.sql"
    INIT_POSTGRES=false
    echo "PostgreSQL initialized."
fi

echo "Initialization finished."

# Execute the command (should be the Rust binary)
echo "Starting controller..."
exec "$@"
