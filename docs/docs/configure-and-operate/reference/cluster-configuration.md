---
id: cluster-configuration
title: Cluster Configuration Parameters
sidebar_label: Cluster Configuration
---

Ignite 3 cluster configuration is shared across the whole cluster. Regardless of which node you apply the configuration on, it will be propagated to all nodes in the cluster.

In Ignite 3, you can create and maintain configuration in either HOCON or JSON. The configuration file has a single root "node," called `ignite`. All configuration sections are children, grandchildren, etc., of that node.

## Checking Cluster Configuration

To get cluster configuration, use the CLI tool.

- Start the CLI tool and connect to any node in the cluster.
- Run the `cluster config show` command.

The CLI tool will print the full cluster configuration. If you only need a part of the configuration, you can narrow down the search by providing the properties you need as the command argument, for example:

```shell
cluster config show ignite.transaction
```

## Changing Cluster Configuration

Cluster configuration is changed from the CLI tool. You can update it both in the interactive (REPL) and non-interactive mode by passing a configuration file with the `--file` parameter.

:::note
Values set directly via the CLI take precedence over values set in the configuration file.
:::

### Update via REPL

Start the CLI tool and connect to any node in the cluster.

- Run the `cluster config update` command and provide the updated configuration as the command argument, for example:

```shell
cluster config update ignite.system.idleSafeTimeSyncIntervalMillis=600
```

- To update one or more parameters, pass the configuration file to the `cluster config update` command:

```shell
cluster config update --file ../ignite-config.conf
```

- You also can update the configuration combining both approaches:

```shell
cluster config update --file ../ignite-config.conf ignite.system.idleSafeTimeSyncIntervalMillis=600
```

The updated configuration will automatically be applied across the cluster.

### Update via Non-Interactive Mode

You can also modify cluster configuration via [non-interactive](/3.1.0/tools/cli-commands#non-interactive-cli-mode) CLI mode without starting the CLI tool first.

- Pass the configuration file with the `--file` parameter:

```shell
bin/ignite3 cluster config update --file ../ignite-config.conf
```

The updated configuration will automatically be applied across the cluster.

## Exporting Cluster Configuration

If you need to export cluster configuration to file, use the following command:

```shell
bin/ignite3 cluster config show > cluster-config.txt
```

## Configuration Parameters

### Event Log Configuration

```json
{
  "ignite" : {
    "eventlog" : {
      "channels" : [ ],
      "sinks" : [ ]
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| channels | | A named list of event log channels. | Yes | No | Valid channels |
| sinks | | A named list of event log sinks. | Yes | No | Valid sinks |

### Garbage Collection Configuration

```json
{
  "ignite" : {
    "gc" : {
      "batchSize" : 5,
      "lowWatermark" : {
        "dataAvailabilityTimeMillis" : 600000,
        "updateIntervalMillis" : 300000
      },
      "threads" : 16
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| batchSize | 5 | The number of entries to be removed by the garbage collection batch for each partition | Yes | No | 0 - inf |
| lowWatermark.dataAvailabilityTimeMillis | 600000 | The duration the outdated versions are available for, in milliseconds. | Yes | No | 1000 - inf |
| lowWatermark.updateIntervalMillis | 300000 | The interval of the low watermark updates. | Yes | No | 0 - inf |
| threads | Runtime.getRuntime().availableProcessors() | The number of threads used by the garbage collector. | Yes | Yes | 1 - inf |

### System Configuration

```json
{
  "ignite" : {
    "system" : {
      "idleSafeTimeSyncIntervalMillis" : 500
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| idleSafeTimeSyncIntervalMillis | 500 | Period (in milliseconds) used to determine how often to issue time sync commands when Metastorage is idle (no Writes are issued). Should not exceed schemaSync.delayDurationMillis. The optimal value is schemaSync.delayDurationMillis / 2. | Yes | No (becomes effective on Metastorage leader reelection) | 1 - inf |

### Metrics Configuration

```json
{
  "ignite" : {
    "metrics" : {
      "exporters" : [ ]
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| exporters | | The list of [metric](/3.1.0/configure-and-operate/monitoring/configuring-metrics) exporters currently used. | Yes | No | Valid exporters |

### Replication Configuration

```json
{
  "ignite" : {
    "replication" : {
      "idleSafeTimePropagationDurationMillis" : 1000,
      "leaseAgreementAcceptanceTimeLimitMillis" : 120000,
      "leaseExpirationIntervalMillis" : 5000,
      "rpcTimeoutMillis" : 60000,
      "batchSizeBytes" : 8192
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| idleSafeTimePropagationDurationMillis | 1000 | Interval between Partition Safe Time updates. | No | N/A | 1 - inf |
| leaseAgreementAcceptanceTimeLimitMillis | 120000 | The maximum duration of an election for a new partition leaseholder, in milliseconds. | Yes | N/A | 5000 - inf |
| leaseExpirationIntervalMillis | 5000 | The duration of a single lease. | Yes | N/A | 2000 - 120000 |
| rpcTimeoutMillis | 60000 | Replication request processing timeout. | Yes | No | 0 - inf |
| batchSizeBytes | 8192 | Batch length (in bytes) to be written into physical storage. Used to limit the size of an atomical Write. | Yes | No | 1 - Integer.MAX_VALUE |

### Schema Sync Configuration

```json
{
  "ignite" : {
    "schemaSync" : {
      "delayDurationMillis" : 100,
      "maxClockSkewMillis" : 500
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| delayDurationMillis | 100 | The delay after which a schema update becomes active. Should exceed the typical time to deliver a schema update to all cluster nodes, otherwise delays in handling operations are possible. Should not be less than system.idleSafeTimeSyncIntervalMillis. The optimal value is system.idleSafeTimeSyncIntervalMillis * 2. | No | N/A | 1 - inf |
| maxClockSkewMillis | 500 | Maximum physical clock skew (ms) tolerated by the cluster. If the difference between physical clocks of two nodes in the cluster exceeds this value, the cluster might demonstrate abnormal behavior. | No | N/A | 0 - inf |

### Security Configuration

```json
{
  "ignite" : {
    "security" : {
      "authentication" : {
        "providers" : [ {
          "name" : "default",
          "type" : "basic",
          "users" : [ {
            "password" : "********",
            "username" : "ignite",
            "displayName" : "ignite"
          }]
        } ]
      }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| **Authentication parameters** | | | | | |
| providers.name | default | The name of the authentication provider. | Yes | No | A valid string |
| providers.type | basic | The authentication provider type. | Yes | No | basic, ldap |
| providers.users | | The list of users registered with the specific provider. | | | |
| providers.users.displayName | ignite | Case sensitive user name. | No | N/A | A valid username |
| providers.users.password | ******** | User password. | Yes | No | A valid password |
| providers.users.username | ignite | Case-insensitive user name. | Yes | No | A valid user name |
| **Authorization parameters** | | | | | |

### SQL Configuration

```json
{
  "ignite" : {
    "sql" : {
      "createTable" : {
        "minStaleRowsCount" : 500,
        "staleRowsFraction" : 0.2
      },
      "planner" : {
        "estimatedNumberOfQueries" : 1024,
        "maxPlanningTimeMillis" : 15000
      }
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| createTable.minStaleRowsCount | 500 | Number of updates since the last query plan update required to automatically recreate query execution plan. Is overridden by `WITH min stale rows` [parameter](/3.1.0/sql/reference/language-definition/ddl#create-table). | Yes | No | 0 - Long.MAX_VALUE |
| createTable.staleRowsFraction | 0.2 | Fraction of the table that must change for query execution plan to be recreated automatically. Is overridden by `WITH stale rows fraction` [parameter](/3.1.0/sql/reference/language-definition/ddl#create-table). | Yes | No | 0 - 1 |
| planner.estimatedNumberOfQueries | 1024 | The estimated number of unique queries that are planned to be executed in the cluster in a certain period of time. Used to optimize internal caches and processes. Optional. | Yes | Yes | 0 - Integer.MAX_VALUE |
| planner.maxPlanningTimeMillis | 15000 | Query planning timeout in milliseconds. Plan optimization process stops when the timeout is reached. "0" means no timeout. | Yes | Yes | 0 - Long.MAX_VALUE |

### Transactions Configuration

```json
{
  "ignite" : {
    "transaction" : {
      "readOnlyTimeoutMillis" : 600000,
      "readWriteTimeoutMillis" : 30000
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| readOnlyTimeoutMillis | 600000 | Timeout for read-only transactions. It defines how long the transaction holds acquired resources on participating nodes. If no timeout is specified, or it is set to `0`, a default value of 10 minutes is applied. The transaction is guaranteed to remain active until the timeout expires. Once the timeout is reached, the transaction is aborted but may persist briefly beyond the timeout while corresponding resources are cleaned up. | Yes | No | 1 - inf |
| readWriteTimeoutMillis | 30000 | Timeout for read-write transactions. It defines how long the transaction holds acquired resources on participating nodes. If no timeout is specified, or it is set to `0`, a default value of 30 seconds is applied. The transaction is guaranteed to remain active until the timeout expires. Once the timeout is reached, the transaction is aborted but may persist briefly beyond the timeout while corresponding resources are cleaned up. | Yes | No | 1 - inf |

### System Configuration (Internal)

This section describes internal properties used by Ignite components. Although you can edit these properties using the `cluster config update` CLI command, we suggest discussing proposed changes with the Ignite support team. These properties apply to the cluster as a whole. For node-specific properties, see [Node Configuration](/3.1.0/configure-and-operate/reference/node-configuration#system-configuration).

:::note
Note that the property names are in `camelCase`.
:::

```json
{
  "ignite" : {
    "system" : {
      "cmgPath" : "",
      "metastoragePath" : "",
      "partitionsBasePath" : "",
      "partitionsLogPath" : "",
      "properties":[]
    }
  }
}
```

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| system.cmgPath | | The path the cluster management group information is stored to. By default, data is stored in `{IGNITE_HOME}/work/cmg`. | Yes | Yes | Valid absolute path. |
| system.metastoragePath | | The path the cluster meta information is stored to. By default, data is stored in `{IGNITE_HOME}/work/metastorage`. | Yes | Yes | Valid absolute path. |
| system.partitionsBasePath | | The path data partitions are saved to. By default, partitions are stored in `{IGNITE_HOME}/work/partitions`. | Yes | Yes | Valid absolute path. |
| system.partitionsLogPath | | The path RAFT log the partitions are stored at. By default, this log is stored in `{system.partitionsBasePath}/log`. | Yes | Yes | Valid absolute path. |
| system.properties | | System properties used by the Ignite components. | Yes | Yes | An array of properties. |
