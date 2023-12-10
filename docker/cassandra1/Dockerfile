FROM cassandra:latest

COPY ../scripts/create_schema.cql /docker-entrypoint-initdb.d/
COPY ../scripts/* /home