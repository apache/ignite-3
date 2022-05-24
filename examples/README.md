# Apache Ignite 3 Examples

This project contains code examples for Apache Ignite 3.

Examples are shipped as a separate Maven project, so to start running you simply need
to import provided `pom.xml` file into your favourite IDE.

The following examples are included:
* `RecordViewExample` - demonstrates the usage of the `org.apache.ignite.table.RecordView` API
* `KeyValueViewExample` - demonstrates the usage of the `org.apache.ignite.table.KeyValueView` API
* `SqlJdbcExample` - demonstrates the usage of the Apache Ignite JDBC driver.
* `RebalanceExample` - demonstrates the data rebalancing process.

To run the `RebalanceExample`, refer to its JavaDoc for instructions.

To run any other example, do the following:
1. Import the examples project into you IDE.
2. Start a server node using the CLI tool:
   ```
   ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-first-node
   ```
3. Initialize the node using the following command:
   ```
   ignite cluster init --node-endpoint 'localhost:10300' --meta-storage-node 'my-first-node' --cluster-name 'ignite-cluster'
   ```
4. Run the preferred example in the IDE.
