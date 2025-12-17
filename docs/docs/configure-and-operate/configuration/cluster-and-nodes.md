---
id: config-cluster-and-nodes
title: Cluster and Node Configuration
sidebar_label: Cluster and Nodes
---

In Ignite 3, configuration is performed by using the [CLI utility](/3.1.0/tools/cli-commands). Ignite 3 configuration is stored in HOCON format. You can manage and configure parameters at any point during cluster runtime and during cluster startup.

In Ignite 3, you can create and maintain configuration in either HOCON or JSON. The configuration file has a single root "node" called `ignite`. All configuration sections are children, grandchildren, etc., of that node.

## Updating Configuration from CLI

### Getting Cluster and Node Configuration

You can get cluster configuration by using the `cluster config show`, and the configuration of the node you are connected to by using the `node config show` command.

### Updating Cluster and Node Configuration

You can update the configuration by using the `cluster config update` and `node config update` commands and passing the new valid HOCON string as the parameter. Below are some examples of updating the configuration:

#### Updating a single parameter

Updating a single parameter is done by specifying the parameter and assigning it a new value:

```shell
node config update ignite.network.shutdownTimeoutMillis=20000
```

#### Updating Multiple Parameters

When updating multiple parameters at once, pass a valid HOCON configuration to Ignite. The CLI tool will then parse it and apply all required change at the same time.

```shell
cluster config update "{ignite{security.authentication.providers:[{name:basic,password:admin_password,type:basic,username:admin_user,roles:[admin]}],security.authentication.enabled:true}}"
```

## Configuration Files

When an Ignite node starts, it reads its starting configuration from the `etc/ignite-config.conf` file. You can change the file to always consistently start a node with specified configuration.

Cluster configuration is stored on the cluster nodes, and is shared to all nodes in cluster automatically. You should use the CLI tool to manage this configuration.

Ignite also uses a number of environmental parameters to define properties not related to node or cluster operations. When a node starts, it loads these parameters from the `etc/vars.env` file. Edit this file to configure locations of work-related folders, JVM properties and additional JVM arguments through the `IGNITE3_EXTRA_JVM_ARGS` parameter.
