# Apache Ignite 3 Docker Images

This directory contains Docker configurations for Apache Ignite 3.

## Available Images

| Image | Description | Size |
|-------|-------------|------|
| `apacheignite/ignite` | Full server image with CLI and .NET runtime | ~450 MB |
| `apacheignite/ignite-cli` | CLI-only image for cluster management | ~150 MB |

## Building Images

```bash
# Build both images (server + CLI)
./gradlew docker

# Build and push both images
./gradlew docker -Pdocker_push

# Build CLI-only image
./gradlew dockerCli
```

## Running a 3-Node Cluster

### Step 1: Start the cluster

```bash
cd packaging/docker
docker compose up -d
```

### Step 2: Verify nodes are running

```bash
# Check that all 3 nodes are running
docker compose ps

# Check logs if needed
docker compose logs -f node1
```

### Step 3: Connect with CLI

#### Interactive REPL Mode

```bash
# Run CLI in REPL mode, connected to the ignite3 network
docker run --rm -it --network ignite3_default apacheignite/ignite-cli:3.2.0-SNAPSHOT
```

Once in the REPL:

```
# Connect to node1
connect http://node1:10300

# Initialize the cluster (first time only)
cluster init --name my-cluster --metastorage-group node1,node2,node3

# Check cluster status
cluster status

# Run SQL
sql "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)"
sql "INSERT INTO test VALUES (1, 'hello')"
sql "SELECT * FROM test"
```

#### Setting Up an Alias

To simplify CLI commands, set up an alias:

```bash
alias ignite-cli='docker run --rm -it --network ignite3_default apacheignite/ignite-cli:3.2.0-SNAPSHOT'
```

#### One-liner Commands

```bash
# Initialize cluster
ignite-cli cluster init --url http://node1:10300 --name my-cluster --metastorage-group node1,node2,node3

# Check cluster status
ignite-cli cluster status --url http://node1:10300

# Run SQL query
ignite-cli sql --url http://node1:10300 "SELECT 1"

# Enter REPL mode
ignite-cli
```

Without alias:

```bash
docker run --rm -it --network ignite3_default apacheignite/ignite-cli:3.2.0-SNAPSHOT \
  cluster init --url http://node1:10300 --name my-cluster --metastorage-group node1,node2,node3
```

### Step 4: Stop the cluster

```bash
docker compose down
```

## Connecting from Host Machine

If you want to connect from your host machine (not from a Docker container):

```bash
# The docker-compose exposes ports on localhost:
# - node1: REST API on 10300, client port on 10800
# - node2: REST API on 10301, client port on 10801
# - node3: REST API on 10302, client port on 10802

# Using CLI from host
./bin/ignite3 connect http://localhost:10300
```

## Environment Variables

### Server Image (`apacheignite/ignite`)

| Variable | Default | Description |
|----------|---------|-------------|
| `JVM_MAX_MEM` | `16g` | Maximum JVM heap size |
| `JVM_MIN_MEM` | `16g` | Minimum JVM heap size |
| `JVM_GC` | `G1GC` | Garbage collector to use |
| `JVM_G1HeapRegionSize` | `32M` | G1 heap region size |
| `IGNITE3_EXTRA_JVM_ARGS` | | Additional JVM arguments |
| `IGNITE_NODE_NAME` | `defaultNode` | Node name |
| `IGNITE_WORK_DIR` | `/opt/ignite/work` | Working directory |
| `IGNITE_CONFIG_PATH` | `/opt/ignite/etc/ignite-config.conf` | Configuration file path |

### CLI Image (`apacheignite/ignite-cli`)

| Variable | Default | Description |
|----------|---------|-------------|
| `IGNITE_CLI_WORK_DIR` | `/opt/ignite3cli/work` | CLI working directory |

## Exposed Ports

| Port | Description |
|------|-------------|
| 3344 | Internal cluster communication |
| 10300 | REST API |
| 10800 | Client connector (JDBC/ODBC/thin clients) |
