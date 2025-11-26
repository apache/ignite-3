# Apache Ignite 3 Migration Tools

This project provides a set of utilities for migrating from Apache Ignite 2 to Apache Ignite 3.

The project is split into two major artifacts:

* [migration-tools-cli](modules%2Fmigration-tools-cli): Packs submodules into a simple CLI tool.
  * [configuration-converter](modules%2Fconfig-converter): 
  Converts an Apache Ignite 2 configuration file into its Apache Ignite 3 counterpart.
  * [sql-ddl-generator](modules%2Fsql-ddl-generator): 
  Generates an SQL DDL script for each table in the provided Apache Ignite 2 configuration.
  * [persistence-tools](modules%2Fmigration-tools-persistence):
  Allows reading a Apache Ignite 2 node's working directory and migrate the existing data.

## Installing

The tools will be distributed through maven. Currently, they are in the CI/CD build.

### migration-tools-cli

The CLI tools are distributed in a `zip` and `tar.gz` packages.

The executable (`migration-tools`) is in the `bin` folder.

#### Requirements

* Java (any version supported by Ignite 2)
* Posix shell

### migration-tools-adapter

The **migration tools adapter** can be imported to your maven project as follows:

```xml
<dependency>
  <groupId>org.apache.ignite</groupId>
  <artifactId>migration-tools-adapter</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

## Usage

### migration-tools-cli

Display help for the available commands:
`migration-tools --help`

Convert an Apache Ignite 2 configuration file into Apache Ignite 3 cluster and node configurations:
`migration-tools configuration-converter resources/ignite-config.0.xml resources/node.conf resources/cluster.conf --include-defaults`

Generate an SQL DDL script for each table based on the Apache Ignite 2 file:
`migration-tools sql-ddl-generator resources/ignite-config.0.xml`

### persistent data

Collection of tools to interact with the Apache Ignite 2 node's work directory.

Examples:

Show `persistent-data` command help:
`migration-tools persistent-data --help`

#### List Caches

Lists the caches available in the Apache Ignite 2 node's work directory.

##### Examples:

List caches on `node1`:

```shell
migration-tools \
    persistent-data \
    node1/workDir \
    d270287b-b031-4fdb-abe1-6b124b0be9ef \
    resources/configs/ignite-config.xml \
    list-caches
```

```
id      name
-1672482954      Country
2100619      City
```

Show `list-caches` command help:
```shell
migration-tools \
    persistent-data \
    node1/workDir \
    d270287b-b031-4fdb-abe1-6b124b0be9ef \
    resources/configs/ignite-config.xml \
    list-caches \
    --help
```

#### Migrate Cache

Migrates data from an Apache Ignite 2 node's directory into a live Apache Ignite 3 cluster.
The migration is performed partition by partition.
In case of an error, you may use the saved progress file along with the `--resume-from` option to skip the partitions successfully migrated in the previous run.

The `mode` option defines the error handling policy in case of an irreconcilable mismatch between an Apache Ignite 2 record and the target Apache Ignite 3 table:

* `ABORT` (default): panics and aborts the migration.
* `IGNORE_COLUMN`: any additional columns/fields in the cache record will be ignored (not migrated to the table; lost). The others will be migrated as usual.
* `SKIP_RECORD`: The entire cache record will be ignored (not migrated to the table; lost).
* `PACK_EXTRA`: Any additional columns/fields in the cache record will be serialized to JSON and stored in the `__EXTRA__` column. This is an additional column that the tool adds to the table, it is not a native feature.

The `--rate-limiter N` option limits the number of records migrated per second by using a very basic rate limiter implementation, which may be prone to bursts.

By default, this command will save the progress in the current work directory.
`--no-save-progress` will not record the progress file.

The `--resume-from progress-file.json` allows resuming the migration from the provided progress file.
All the partitions marked as successfully migrated in this file will be skipped.

Examples:

Migrate the cache `Country` cache, halting on any error using the `ABORT` mode:

```shell
migration-tools \
    persistent-data \
    node1/workDir \
    d270287b-b031-4fdb-abe1-6b124b0be9ef \
    configs/ignite-config.xml \
    migrate-cache \
    Country \
    127.0.0.1:10800 \
    --mode ABORT
```

## Contributing

### Building

The project can be built using maven: `./gradlew build`.

Make sure you meet the following dependencies.

#### Apache Ignite 3 Docker Image

This image is necessary for running tests.
If the `ignite3.docker.version` property is set to `latest`, you must build the Docker image locally:

1. Clone the [Apache Ignite 3 repository](https://github.com/apache/ignite-3).
2. Recommended: clone the same revision as declared in the dependencies `ignite3.version.commit`. Automatically get the revision hash by running this in `migration-tools-root`:

    ```bash
    ./gradlew :properties | awk -F ': ' '/ignite3.version.commit/ {print $2}'
    ```

3. Build the Docker image (for tests only) : `./gradlew clean :packaging:docker -x check`.

#### Test

The end-to-end tests can take significant time to run.
You have the option to limit the number of caches for tests using the `-Pe2e.testLimiter=10` property, where `10` is the number of cache examples to test.

##### Test Properties

| Property                                           | Description                                                                                                                                            | Default |
|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| e2e.testLimiter                                    | Limits the amount of tests run during the end-to-end tests. Can help reduce the build duration.                                                                   | None    |
| ai2.sampleCluster.Xmx                              | Adjusts the max JVM memory for the sample cluster containers.                                                                                          | 10g     | 
| ai2.sampleCluster.checkpointChecker.maxwaitseconds | Maximum time (s) waiting for checkpoints to happen on the sample cluster. This is used to make sure the cluster shut down gracefully.                | 360     |
| ai2.sampleCluster.checkpointChecker.pollingseconds | Polling period for checking if the checkpoints have completed on the sample clusters.                                                                  | 10      |
| ai2.sampleCluster.recreate.seeding.maxwaitseconds  | Maximum time (s) spent recreating the sample cluster. The default is very optimistic. It should take around 2 hours. Check below for a faster alternative.  | 7200    |
| ai2.sampleCluster.recreate.seeding.pollingseconds  | Polling period for checking if recreating the sample cluster has finished.                                                                       | 180     |
| seeddata.nCachesPerStint                           | Number of caches recreated with the same client instance. Reusing the client helps speeding up the process, but also consumes more resources.          | 25      |

##### Sample Clusters

Some of the tests use an Apache Ignite 2 persistent directory.
Although the framework can recreate the cluster, it can take more than 2 hours.
As an alternative result, we provide a prebuilt cluster that you can download:

```shell
bash .ci/download-n-extract-sample-cluster.sh
```

### Modules

#### table-type-hints

Generates a configuration file that informs the interface adapter of the correct classes for mapping tables/caches.
This tool inspects the running Apache Ignite 2 cluster to get this information.

Currently, there are no use cases for this module.

### Known Issues

* The messages about properties not converted successfully are hard to read.
* Enable the license requirements in the checkStyle plugin: TODO: Enable License
* Remove the checkstyle suppression from the external file.
