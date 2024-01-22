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


### How to start Grafana + Prometheus?

#### Prometheus
1. Copy `jmx_prometheus_javaagent-0.19.0.jar` to `/opt/cassandra/lib/` 
 Link: https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.19.0/jmx_prometheus_javaagent-0.19.0.jar
2. Copy `jmx_exporter.yml` to `/etc/cassandra/`

Content:
```yaml
lowercaseOutputLabelNames: true
lowercaseOutputName: true
whitelistObjectNames: ["org.apache.cassandra.metrics:*"]
# ColumnFamily is an alias for Table metrics
blacklistObjectNames: ["org.apache.cassandra.metrics:type=ColumnFamily,*"]
rules:
# Generic gauges with 0-2 labels
- pattern: org.apache.cassandra.metrics<type=(\S*)(?:, ((?!scope)\S*)=(\S*))?(?:, scope=(\S*))?, name=(\S*)><>Value
  name: cassandra_$1_$5
  type: GAUGE
  labels:
    "$1": "$4"
    "$2": "$3"

#
# Emulate Prometheus 'Summary' metrics for the exported 'Histogram's.
# TotalLatency is the sum of all latencies since server start
#
- pattern: org.apache.cassandra.metrics<type=(\S*)(?:, ((?!scope)\S*)=(\S*))?(?:, scope=(\S*))?, name=(.+)?(?:Total)(Latency)><>Count
  name: cassandra_$1_$5$6_seconds_sum
  type: UNTYPED
  labels:
    "$1": "$4"
    "$2": "$3"
  # Convert microseconds to seconds
  valueFactor: 0.000001

- pattern: org.apache.cassandra.metrics<type=(\S*)(?:, ((?!scope)\S*)=(\S*))?(?:, scope=(\S*))?, name=((?:.+)?(?:Latency))><>Count
  name: cassandra_$1_$5_seconds_count
  type: UNTYPED
  labels:
    "$1": "$4"
    "$2": "$3"

- pattern: org.apache.cassandra.metrics<type=(\S*)(?:, ((?!scope)\S*)=(\S*))?(?:, scope=(\S*))?, name=(.+)><>Count
  name: cassandra_$1_$5_count
  type: UNTYPED
  labels:
    "$1": "$4"
    "$2": "$3"

- pattern: org.apache.cassandra.metrics<type=(\S*)(?:, ((?!scope)\S*)=(\S*))?(?:, scope=(\S*))?, name=((?:.+)?(?:Latency))><>(\d+)thPercentile
  name: cassandra_$1_$5_seconds
  type: GAUGE
  labels:
    "$1": "$4"
    "$2": "$3"
    quantile: "0.$6"
  # Convert microseconds to seconds
  valueFactor: 0.000001

- pattern: org.apache.cassandra.metrics<type=(\S*)(?:, ((?!scope)\S*)=(\S*))?(?:, scope=(\S*))?, name=(.+)><>(\d+)thPercentile
  name: cassandra_$1_$5
  type: GAUGE
  labels:
    "$1": "$4"
    "$2": "$3"
    quantile: "0.$6"

```
3. Edit `/etc/cassandra/cassandra-env.sh`
    
    Add: 
    ```
    JVM_OPTS="$JVM_OPTS -javaagent:$CASSANDRA_HOME/lib/jmx_prometheus_javaagent-0.19.0.jar=7070:/etc/cassandra/jmx_exporter.yml"
    ```
4. Restart Cassandra
    
    ```bash
    systemctl restart cassandra
    ```

### Grafana
1. Import dashboard with ID: 12086