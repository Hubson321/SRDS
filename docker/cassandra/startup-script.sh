#!/bin/bash

# Wait until Cassandra is not ready
until cqlsh -e "DESCRIBE KEYSPACES"; do
    echo "CASSANDRA NOT READY - waiting 5 seconds"
    sleep 5
done

# Import data
cqlsh -f /cql-schema/create_schema.cql
# cqlsh -f /cql-schema/load_data.cql

# Sleep infinity - container is alive
sleep infinity