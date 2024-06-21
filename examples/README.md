# Apache Ignite 3 Examples

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

Before running the examples, read about [cli](https://ignite.apache.org/docs/3.0.0-beta/ignite-cli-tool).

## Running the examples with an Ignite node within a Docker container

Preapare environment variables:
```shell
IGNITE_SOURCES=/path/to/ignite3-sources
```

Start an Ignite node:
```shell
docker run --name ignite3-node -d --rm -p 10300:10300 -p 10800:10800 -v $IGNITE_SOURCES/examples/config/ignite-config.conf:/opt/ignite/etc/ignite-config.conf apacheignite/ignite3
```

Find out IP address of the node:
```shell
NODE_IP_ADDRESS=$(docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ignite3-node)
```

Activate the node:
```shell
docker run -it apacheignite/ignite3 cli cluster init --url http://$NODE_IP_ADDRESS:10300 --name myCluster1 --cluster-management-group defaultNode --metastorage-group defaultNode
```

Run an example via IDE.

Stop the Ignite node:
```shell
docker stop ignite3-node
```

## Running the examples with an Ignite node started natively

