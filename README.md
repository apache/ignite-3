Apache Ignite 3
===

Apache Ignite 3 is a distributed database for high-performance computing.

* ACID TRANSACTIONS: Ignite can operate in a strongly consistent mode that provides full support for distributed ACID transactions executed at the Serializable isolation level.
* DISTRIBUTED SQL: Use Ignite as a traditional SQL database by leveraging JDBC drivers, ODBC drivers, or the native SQL APIs that are available for Java, C#, C++, and other programming languages.
* COMPUTE API: With traditional databases, for in-place calculations, you use stored procedures that are written in a language such as PL/SQL. With Ignite, you use modern JVM languages to develop and execute custom tasks across your distributed database.
* SCHEMA-DRIVEN ARCHITECTURE: Ignite built around schema-driven model ensuring consistency between DDL, internal models and data.
* PLUGABLE STORAGE ENGINES: Ignite's modular architecture enables the customization of underlying data and metadata storage, offering in-memory storage and RocksDB as default options.
* UNIFIED CLI TOOL AND REST API: Management tools now include built-in CLI and REST API allowing simple access and configuration of Ignite cluster.

## Documentation

The latest documentation is generated together with the releases and hosted on the apache site.

Please check [the documentation page](https://ignite.apache.org/docs/3.0.0-beta/) for more information.

## Contact

Ignite is a top level project under the [Apache Software Foundation](https://apache.org)

* Ignite [web page](https://ignite.apache.org)
* Mailing lists
    * For any questions use: [user@ignite.apache.org](https://lists.apache.org/list.html?user@ignite.apache.org) or [dev@ignite.apache.org](https://lists.apache.org/list.html?dev@ignite.apache.org)

## Download

Latest release artifacts (source release and binary packages) are [available](https://ignite.apache.org/download.cgi) from the Ignite web page.

## Quick start

### Run Ignite from released artifact

To start Ignite you need to download latest zip archive from the [Ignite web page](https://ignite.apache.org/download.cgi). 
Scroll down to the "Binary releases" section and download the version you are interested in.

Here is the command you can run to download current latest release:

```shell
https://dlcdn.apache.org/ignite/3.0.0-beta1/ignite3-3.0.0-beta1.zip
```

```shell
unzip ignite3-3.0.0-beta1.zip
```
After this you should have two directories: `ignite3-db-3.0.0-<version>` and `ignite3-cli-3.0.0-<version>`. 
The first one contains everything you need to start Ignite node, and the second one contains Ignite CLI tool.

After unpacking it, go to the `ignite3-db-3.0.0-<version>` folder and run the following command:

```shell
cd ignite3-db-3.0.0-beta1
./bin/ignite3db start
```

This command starts the Ignite node with the name `defaultNode`. 
If you want to change any node configuration, change values in `etc/ignite-config.conf` before the start.

After that you need to connect to your node with Ignite CLI in interactive mode:

```shell
cd ../ignite3-cli-3.0.0-beta1
./bin/ignite3
```

In CLI you need to initialize simple cluster via the following command:

```
cluster init --name myCluster --cmg-node defaultNode --ms-node defaultNode
```

Now CLI can be switched into SQL interactive mode with command:

```
sql
```

In SQL interactive mode user can simply type SQL commands in CLI:

```sql
CREATE TABLE IF NOT EXISTS Person (id int primary key,  city varchar,  name varchar,  age int,  company varchar);
INSERT INTO Person (id, city, name, age, company) VALUES (1, 'London', 'John Doe', 42, 'Apache');
INSERT INTO Person (id, city, name, age, company) VALUES (2, 'New York', 'Jane Doe', 36, 'Apache');
SELECT * FROM Person;
```

## Build from source

Ignite distributive zip archive can be built with [Gradle](https://gradle.org/):

```shell
./gradlew clean distZip
```

Build artifacts can be found in `packaging/db` and `packaging/cli` directories.

## Run from source using Docker

Ignite can be started with the help of Docker:

```shell
./gradlew docker
cd packaging/docker
docker compose up -d
```

You can also run the CLI within the Docker:

```shell
docker run -it --rm --net ignite3_default apacheignite/ignite3 cli
```

```
> connect http://node1:10300
> cluster init --name cluster --ms-node node1 --ms-node node2 --ms-node node3
```

For more information, you can check the [Developer notes](./DEVNOTES.md)

## Contribute

All contributions are welcome.

1. Please open a [Jira](https://issues.apache.org/jira/projects/IGNITE/issues) issue
2. And create a pull request

For more information, you can check the [Contribution guideline](./CONTRIBUTING.md)

## License

The Apache Ignite project is licensed under the Apache 2.0 License. See the [LICENSE](./LICENSE.txt) file for details.
