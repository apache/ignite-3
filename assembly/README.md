# Apache Ignite 3 Alpha 5

Apache Ignite is a distributed database for high-performance computing with in-memory speed.

Ignite 3 is the next generation of the platform that will support a modernized modular architecture,
along with improved usability and developer experience.

The current alpha version includes the following features:
* Unified CLI tool
* New configuration engine
* New schema management engine
* Table API
* Atomic storage implementation based on Raft
* New SQL engine based on Apache Calcite and JDBC driver
* New binary client protocol and its implementation in Java

## Installation

1. Download Ignite 3 Alpha 5:
   ```
   curl -L "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=ignite/3.0.0-alpha5/apache-ignite-3.0.0-alpha5.zip" -o apache-ignite-3.0.0-alpha5.zip
   ```
2. Unzip the downloaded file:
   ```
   unzip apache-ignite-3.0.0-alpha5.zip && cd apache-ignite-3.0.0-alpha5
   ```
3. Add your installation directory to the PATH environment variable:
   ```
   echo 'export IGNITE_HOME="'`pwd`'"' >> ~/.bash_profile && echo 'export PATH="$IGNITE_HOME:$PATH"' >> ~/.bash_profile && source ~/.bash_profile
   ```
4. (optional) If you start the cluster locally then install the core artifacts:
   ```
   ignite bootstrap
   ```

## Running Examples

Examples are shipped as a separate Maven project, which is located in the `examples` folder.
To start running you simply need to import provided `pom.xml` file into your favourite IDE.

The following examples are included:
* `RecordViewExample` - demonstrates the usage of the `org.apache.ignite.table.RecordView` API
* `KeyValueViewExample` - demonstrates the usage of the `org.apache.ignite.table.KeyValueView` API
* `SqlJdbcExample` - demonstrates the usage of the Apache Ignite JDBC driver.
* `SqlApiExample` - demonstrates the usage of the Java API for SQL.
* `VolatilePageMemoryStorageExample` - demonstrates the usage of the PageMemory storage engine configured with an in-memory data region.
* `PersistentPageMemoryStorageExample` - demonstrates the usage of the PageMemory storage engine configured with a persistent data region.

To run any other example, do the following:
1. Import the examples project into your IDE.
2. Start a server node using the CLI tool:
   ```
   ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-first-node
   ```
3. (optional) If the cluster is not initialized then initialize the cluster:
   ```
   ignite cluster init --cluster-name=ignite-cluster --node-endpoint=localhost:10300 --meta-storage-node=my-first-node
   ```
4. Run the preferred example in the IDE.

## Using CLI

Alpha 5 leverages the advantage of using the CLI for the development. Updated CLI ships the following features:
* Bash auto-completions. If you are using bash/zsh you can hit the TAB during typing ignite commands.
* Interactive mode. Enter the interactive by running `ignite` without arguments.
* SQL REPL. Run SQL queries right from your terminal with `ignite sql` command.
