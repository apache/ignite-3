Apache Ignite 3.x
===

Apache Ignite 3 is a distributed database for high-performance computing with in-memory speed.

* MULTI-TIER STORAGE: Ignite scales up and out across memory and disk. By default, Ignite operates in a pure in-memory mode. But, by toggling a single configuration setting, you can turn a cluster into a database that can grow beyond the cluster's memory capacity.
* DISTRIBUTED SQL: Use Ignite as a traditional SQL database by leveraging JDBC drivers, ODBC drivers, or the native SQL APIs that are available for Java, C#, C++, Python, and other programming languages.
* ACID TRANSACTIONS: Ignite can operate in a strongly consistent mode that provides full support for distributed ACID transactions.
* COMPUTE APIS: With traditional databases, for in-place calculations, you use stored procedures that are written in a language such as PL/SQL. With Ignite, you use modern JVM languages, C# or C++ to develop and execute custom tasks across your distributed database.
* MACHINE LEARNING: Ignite machine learning uses built-in algorithms and tools, as well as TensorFlow integration, to enable the building of scalable machine learning models and avoid costly data transfers.
* CONTINUOUS QUERIES: With relational databases, you use triggers to react to certain events. With Ignite, you deploy continuous queries that are written in a modern programming language such as Java or C# and process streams of changes on the database and application side.

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
After unpacking it, go to the ignite3-db-3.0.0-beta1 folder and run the following command:

```
bin/ignite3db start
```

After that you need to connect to your node with Ignite CLI:

```
ignite3-cli-3.0.0-beta1/bin/ignite3
```

In CLI you need to initialize simple cluster via the following command:

```
cluster init -n=sampleCluster -m=defaultNode
```

Now CLI can be switched into SQL interactive mode with command:

```
sql
```

In SQL interactive mode user can simply type SQL commands in CLI:

```sql
CREATE TABLE IF NOT EXISTS Person (id int primary key,  city varchar,  name varchar,  age int,  company varchar);
INSERT INTO Person (id, city, name, age, company) VALUES ('1', 'London', 'John Doe', '42', 'Apache');
INSERT INTO Person (id, city, name, age, company) VALUES ('2', 'New York', 'Jane Doe', '36', 'Apache');
SELECT * FROM Person;
```

## Build from source

Ignite distributive zip archive can be built with [Gradle](https://gradle.org/):

```
./gradlew clean distZip
```

Build artifacts can be found in packaging/db and packaging/cli directories.

## Run from source using Docker

Ignite can be started with the help of Docker:

```
./gradlew docker
cd packaging/docker
docker compose up -d
```

For more information, you can check the [Developer notes](./DEVNOTES.md)

## Contribute

All contributions are welcome.

1. Please open a [Jira](https://issues.apache.org/jira/projects/IGNITE/issues) issue
2. And create a pull request

For more information, you can check the [Contribution guideline](./CONTRIBUTING.md)

## License

The Apache Ignite project is licensed under the Apache 2.0 License. See the [LICENSE](./LICENSE.txt) file for details.
