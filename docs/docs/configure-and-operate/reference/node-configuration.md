---
id: node-configuration
title: Node Configuration Parameters
sidebar_label: Node Configuration
---

Node configuration is individual for each node and is not shared across the whole cluster.

In Ignite 3, you can create and maintain configuration in either HOCON or JSON. The configuration file has a single root "node," called `ignite`. All configuration sections are children, grandchildren, etc., of that node.

## Checking Node Configuration

To get node configuration, use the CLI tool.

- Start the CLI tool and connect to the node.
- Run the `node config show` command.

The CLI tool will print the full node configuration. If you only need a part of the configuration, you can narrow down the search by providing the properties you need as the command argument, for example:

```shell
node config show ignite.clientConnector
```

## Changing Node Configuration

Node configuration is changed from the CLI tool. To change the configuration:

- Start the CLI tool and connect to the node. This becomes the "default" node for subsequent CL commands.
- To update the default node's configuration, run the `node config update` command and provide the update as the command argument, for example:

```shell
node config update ignite.clientConnector.connectTimeoutMillis=10000
```

- To update the configuration of a node other than the default one, run the `node config update` command with the target node explicitly specified. For example, for node named `node1`:

```shell
node config update -n node1 ignite.nodeAttributes.nodeAttributes.clientConnector="10900"
```

- Restart the node to apply the configuration changes.

## Exporting Node Configuration

If you need to export node configuration to a HOCON-formatted file, use the following command:

```shell
bin/ignite3 node config show > node-config.conf
```

## Configuration Parameters

The following sections detail all available node configuration parameters:

- [Client Connector Configuration](/3.1.0/develop/ignite-clients/)
- Compute Configuration
- Code Deployment Configuration
- Failure Handler Configuration
- Network Configuration
- Node Attributes
- RAFT Configuration
- REST Configuration
- SQL Configuration
- Storage Configuration
- System Configuration

For complete configuration reference including all parameters, defaults, descriptions, and acceptable values, consult the Apache Ignite 3 documentation.
