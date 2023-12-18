# Cassandra Cluster

## Configuration

## Node Descriptions

### Node 1
- Name: cassandra1
- Address: 172.18.0.1

### Node 2

- Name: cassandra2
- Address: 172.18.0.2

### Node 3

- Name: cassandra3
- Address: 172.18.0.3

### Node 4

- Name: main-java
- Address: 172.18.0.100

### Short description
To start the cluster, you need to enter the command: 
```bash 
docker-compose up -d
```
This will initiate the process of building all containers, starting from `cassandra1` to `cassandra3` and then create the `java-main` container for running the code.

After building the code, it is immediately available for execution in the container - it is located in the `/build` directory.