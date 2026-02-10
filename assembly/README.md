# Apache Ignite 3

Apache Ignite is a distributed database for high-performance computing with in-memory speed.

Ignite 3 is the next generation of the platform that will support a modernized modular architecture,
along with improved usability and developer experience.

## Installation

1. Download Ignite 3 from the link:https://ignite.apache.org/download.cgi[Ignite website].
2. Unzip the downloaded file.
3. Add your installation directory to the PATH environment variable:
   ```
   export IGNITE_HOME=$(pwd)

## Running Examples

Examples are shipped as a separate Gradle project, which is located in the `examples` folder.
To start running you simply need to import provided `build.gradle` file into your favourite IDE.

The following examples are included:
* `RecordViewExample` - demonstrates the usage of the `org.apache.ignite.table.RecordView` API
* `KeyValueViewExample` - demonstrates the usage of the `org.apache.ignite.table.KeyValueView` API
* `SqlJdbcExample` - demonstrates the usage of the Apache Ignite JDBC driver.
* `SqlApiExample` - demonstrates the usage of the Java API for SQL.
* `VolatilePageMemoryStorageExample` - demonstrates the usage of the PageMemory storage engine configured with an in-memory data region.
* `PersistentPageMemoryStorageExample` - demonstrates the usage of the PageMemory storage engine configured with a persistent data region.

To run any other example, do the following:
1. Import the examples project into your IDE.
2. Start a server node using the startup script:
   ```
   $IGNITE_HOME/bin/ignite3-db
   ```
3. (optional) Setup ignite3-cli in your terminal:
```
   alias ignite3="${ignite-3-cli folder}/bin/ignite3" >> ~/.bash_profile
   source bin/ignite3_completion.sh 
```
4. (optional) If the cluster is not initialized then initialize the cluster:
   ```
   ignite3 cluster init --name=ignite-cluster --url=http://localhost:10300
   ```
5. Run the preferred example in the IDE.

## Using CLI

Beta 1 leverages the advantage of using the CLI for the development. Updated CLI ships the following features:
* Bash auto-completions. If you are using bash/zsh you can hit the TAB during typing ignite commands.
* Interactive mode. Enter the interactive by running `ignite` without arguments.
* SQL REPL. Run SQL queries right from your terminal with `ignite sql` command.
