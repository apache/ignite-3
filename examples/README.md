# Apache Ignite 3 Examples

## Overview

This project contains code examples for Apache Ignite 3.

Examples are shipped as a separate Gradle module, so to start running you simply need
to import provided `build.gradle` file into your favourite IDE.

The following examples are included:
* `RecordViewExample` - demonstrates the usage of the `org.apache.ignite.table.RecordView` API
* `KeyValueViewExample` - demonstrates the usage of the `org.apache.ignite.table.KeyValueView` API
* `SqlJdbcExample` - demonstrates the usage of the Apache Ignite JDBC driver.
* `SqlApiExample` - demonstrates the usage of the Java API for SQL.
* `VolatilePageMemoryStorageExample` - demonstrates the usage of the PageMemory storage engine configured with an in-memory data region.
* `PersistentPageMemoryStorageExample` - demonstrates the usage of the PageMemory storage engine configured with a persistent data region.
* `RocksDbStorageExample` - demonstrates the usage of the RocksDB storage engine.

## Running the examples with an Ignite node within a Docker container

1. Open the Ignite project in your IDE of choice.

2. Prepare an environment variable:
```shell
IGNITE_SOURCES=/path/to/ignite3-sources
```

3. Start an Ignite node:
```shell
docker run --name ignite3-node -d --rm -p 10300:10300 -p 10800:10800 \
  -v $IGNITE_SOURCES/examples/config/ignite-config.conf:/opt/ignite/etc/ignite-config.conf apacheignite/ignite3
```

4. Find out IP address of the node:
```shell
NODE_IP_ADDRESS=$(docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ignite3-node)
```

5. Initialize the node:
```shell
docker run -it apacheignite/ignite3 cli cluster init --url http://$NODE_IP_ADDRESS:10300 --name myCluster1 \
  --cluster-management-group defaultNode --metastorage-group defaultNode
```

6. Run an example via IDE.

7. Stop the Ignite node:
```shell
docker stop ignite3-node
```

## Running the examples with an Ignite node started natively

1. Open the Ignite project in your IDE of choice.

2. Download the Ignite ZIP packaging with DB and CLI parts. Or build them from Ignite sources (see [DEVNOTES.md](../DEVNOTES.md)). Unpack.

3. Prepare the environment variables. `IGNITE_HOME` is used in the Ignite startup script hence one need to export it:
```shell
export IGNITE_HOME=/path/to/ignite3-db-VERSION
IGNITE_CLI_HOME=/path/to/ignite3-cli-VERSION
IGNITE_SOURCES=/path/to/ignite3-sources
```

4. Override the default configuration file:
```shell
echo "CONFIG_FILE=$IGNITE_SOURCES/examples/config/ignite-config.conf" >> $IGNITE_HOME/etc/vars.env
```

5. Start an Ignite node using the startup script from the DB part:
```shell
$IGNITE_HOME/bin/ignite3db start
```

6. Initialize the cluster using Ignite CLI from the CLI part:
```shell
$IGNITE_CLI_HOME/bin/ignite3 cluster init --name myCluster1 --metastorage-group defaultNode --cluster-management-group defaultNode
```

7. Run an example from the IDE.

8. Stop the Ignite node using the startup script:
```shell
$IGNITE_HOME/bin/ignite3db stop
```