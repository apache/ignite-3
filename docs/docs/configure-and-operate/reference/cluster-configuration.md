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

The following sections detail all available cluster configuration parameters:

- Event Log Configuration
- Garbage Collection Configuration
- System Configuration
- Metrics Configuration
- Replication Configuration
- Schema Sync Configuration
- Security Configuration
- SQL Configuration
- Transactions Configuration

For complete configuration reference including all parameters, defaults, descriptions, and acceptable values, consult the Apache Ignite 3 documentation.
