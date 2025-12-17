---
id: cli-commands
title: CLI Commands Reference
sidebar_position: 1
---

# Apache Ignite CLI Tool

## Overview

The Apache Ignite CLI communicates with the cluster via the REST API, allowing you to configure the entire cluster or apply node-specific settings. You can run the CLI either in the interactive mode or execute commands without entering it.

### Interactive CLI Mode

To use the CLI in the interactive mode, first [run](/3.1.0/getting-started/quick-start#start-the-ignite-cli) it, then configure the [cluster](/3.1.0/configure-and-operate/configuration/config-cluster-and-nodes) or [node](/3.1.0/configure-and-operate/reference/node-configuration) using the `update` command.

For example, to add a new user to the cluster:

```bash
cluster config update ignite.security.authentication.providers.default.users=[{username=newuser,displayName=newuser,password="newpassword",passwordEncoding=PLAIN,roles=[system]}]
```

### Non-Interactive CLI Mode

Non-interactive mode is useful for quick updates or when running commands in scripts.

When running commands non-interactively, enclose arguments in quotation marks to ensure that special POSIX characters (such as `{` and `}`) are interpreted correctly:

```bash title="Linux"
bin/ignite3 cluster config update "ignite.schemaSync={delayDurationMillis=500,maxClockSkewMillis=500}"
```

```bash title="Windows"
bin/ignite3.bat cluster config update "ignite.schemaSync={delayDurationMillis=500,maxClockSkewMillis=500}"
```

Alternatively, you can use the backslash (`\`) to escape all special characters in your command. For example:

```bash title="Linux"
bin/ignite3 cluster config update ignite.security.authentication.providers.default.users=\[\{username\=newuser,displayName\=newuser,password\=\"newpassword\",passwordEncoding\=PLAIN,roles\=\[system\]\}\]
```

```bash title="Windows"
bin/ignite3.bat cluster config update ignite.security.authentication.providers.default.users=\[\{username\=newuser,displayName\=newuser,password\=\"newpassword\",passwordEncoding\=PLAIN,roles\=\[system\]\}\]
```

Non-interactive mode is also useful in automation scripts. For example, you can set configuration items in a Bash script as follows:

```bash
#!/bin/bash

...

bin/ignite3 cluster config update "ignite.schemaSync={delayDurationMillis=500,maxClockSkewMillis=500}"

bin/ignite3 cluster config update "ignite.security.authentication.providers.default.users=[{username=newuser,displayName=newuser,password=\"newpassword\",passwordEncoding=PLAIN,roles=[system]}]"
```

### Verbose Output

All CLI commands can provide additional output that can be helpful in debugging. You can specify the `-v` option multiple times to increase output verbosity. Single option shows REST request and response, second option (-vv) shows request headers, third one (-vvv) shows request body.

### CLI Tool Logs

CLI tool stores extended logs for your operations. These logs contain additional information not displayed during normal operation. You can configure the directory in the following ways:

- By setting the `IGNITE_CLI_LOGS_DIR` environment variable to the directory where logs will be stored.
- By setting the `$XDG_STATE_HOME` environment variable to set the CLI home folder. This configuration variable follows the [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/latest/) and does not override the `IGNITE_CLI_LOGS_DIR`. If `$XDG_STATE_HOME` is set and `IGNITE_CLI_LOGS_DIR` is not, logs will be stored in `$XDG_STATE_HOME/ignitecli/logs` directory.

If neither of the above properties are set, the logs are stored in the following locations:

- On Unix systems and MacOS, in the `~/.local/state/ignitecli/logs` directory.
- On Windows, in the `%USERPROFILE%\.local\state\ignitecli\logs` folder.

## SQL Commands

These commands help you execute SQL queries against the cluster.

### sql

Executes SQL query or enters the interactive SQL editor mode if no SQL query is specified.

#### Syntax

```
sql [--jdbc-url=<jdbc>] [--plain] [--file=<file>] [--profile=<profileName>] [--verbose] <command>
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--jdbc-url` | Option | No | JDBC url to ignite cluster (e.g., 'jdbc:ignite:thin://127.0.0.1:10800'). |
| `--plain` | Flag | No | Display output with plain formatting. |
| `--file` | Option | No | Path to file with SQL commands to execute. |
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |
| `<command>` | Argument | Yes | SQL query to execute. |

#### Example

```bash
sql "SELECT * FROM PUBLIC.PERSON"
```

### sql planner invalidate-cache

Invalidates SQL planner cache.

#### Syntax

```
sql planner invalidate-cache [--tables=<tables>] [--url=<clusterUrl>] [--profile=<profileName>] [--verbose]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--tables` | Option | No | Comma-separated list of tables. |
| `--url` | Option | No | URL of cluster endpoint. It can be any node URL. If not set, the default URL from the profile settings will be used. |
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |

#### Example

```bash
sql planner invalidate-cache --tables=PUBLIC.PERSON,PUBLIC.ORDERS
```

## CLI Configuration Commands

These commands help you configure Apache Ignite CLI tool profiles and settings.

### cli config profile create

Creates a profile with the given name.

#### Syntax

```
cli config profile create [--activate] [--copy-from=<copyFrom>] [--verbose] <profileName>
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--activate` | Flag | No | Activate new profile as current. |
| `--copy-from` | Option | No | Profile whose content will be copied to new one. |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |
| `<profileName>` | Argument | Yes | Name of new profile. |

#### Example

```bash
cli config profile create --activate --copy-from=default myprofile
```

### cli config profile activate

Activates the profile identified by name.

#### Syntax

```
cli config profile activate [--verbose] <profileName>
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |
| `<profileName>` | Argument | Yes | Name of profile to activate. |

#### Example

```bash
cli config profile activate myprofile
```

### cli config profile list

Lists configuration profiles.

#### Syntax

```
cli config profile list [--verbose]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |

#### Example

```bash
cli config profile list
```

### cli config profile show

Gets the current profile details.

#### Syntax

```
cli config profile show [--verbose]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |

#### Example

```bash
cli config profile show
```

### cli config get

Gets the value for the specified configuration key.

#### Syntax

```
cli config get [--profile=<profileName>] [--verbose] <key>
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |
| `<key>` | Argument | Yes | Property name. |

#### Example

```bash
cli config get ignite.jdbc-url
```

### cli config set

Sets configuration parameters using comma-separated input key-value pairs.

#### Syntax

```
cli config set [--profile=<profileName>] [--verbose] <String=String>...
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |
| `<String=String>...` | Arguments | Yes | CLI configuration parameters. |

#### Example

```bash
cli config set ignite.jdbc-url=http://localhost:10300
```

### cli config show

Shows the currently active configuration.

#### Syntax

```
cli config show [--profile=<profileName>] [--verbose]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |

#### Example

```bash
cli config show
```

### cli config remove

Removes the specified configuration key.

#### Syntax

```
cli config remove [--profile=<profileName>] [--verbose] <key>
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |
| `<key>` | Argument | Yes | Property name. |

#### Example

```bash
cli config remove ignite.jdbc-url
```

## Cluster Commands

These commands help you manage your cluster.

### cluster config show

Shows configuration of the cluster indicated by the endpoint URL and, optionally, by a configuration path selector.

#### Syntax

```
cluster config show [--url=<clusterUrl>] [--format=<format>] [--profile=<profileName>] [--verbose] [<selector>]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--url` | Option | No | URL of cluster endpoint. |
| `--format` | Option | No | Output format. Valid values: JSON, HOCON (Default: HOCON). |
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |
| `<selector>` | Argument | No | Configuration path selector. |

#### Example

```bash
cluster config show
```

### cluster config update

Updates configuration of the cluster indicated by the endpoint URL with the provided argument values.

#### Syntax

```
cluster config update [--url=<clusterUrl>] [--file=<configFile>] [--profile=<profileName>] [--verbose] [<args>...]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--url` | Option | No | URL of cluster endpoint. |
| `--file` | Option | No | Path to file with config update commands to execute. |
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |
| `<args>...` | Arguments | No | Configuration arguments and values to update. |

#### Example

```bash
cluster config update ignite.system.idleSafeTimeSyncIntervalMillis=250
```

### cluster init

Initializes an Ignite cluster.

#### Syntax

```
cluster init --name=<clusterName> [--metastorage-group=<nodeNames>] [--cluster-management-group=<nodeNames>] [--config=<config>] [--config-files=<filePaths>] [--url=<clusterUrl>] [--profile=<profileName>] [--verbose]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--name` | Option | Yes | Human-readable name of the cluster. |
| `--metastorage-group` | Option | No | Metastorage group nodes (comma-separated list). |
| `--cluster-management-group` | Option | No | Names of nodes that will host the Cluster Management Group (comma-separated list). |
| `--config` | Option | No | Cluster configuration that will be applied during initialization. |
| `--config-files` | Option | No | Path to cluster configuration files (comma-separated list). |
| `--url` | Option | No | URL of cluster endpoint. |
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |

#### Example

```bash
cluster init --name=myCluster
```

### cluster status

Prints status of the cluster.

#### Syntax

```
cluster status [--url=<clusterUrl>] [--profile=<profileName>] [--verbose]
```

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `--url` | Option | No | URL of cluster endpoint. |
| `--profile` | Option | No | Local CLI profile name (only available in non-interactive mode). |
| `--verbose` | Flag | No | Show additional information: logs, REST calls. |

#### Example

```bash
cluster status --url http://localhost:10300
```

For a complete reference of all cluster commands, node commands, disaster recovery commands, distribution commands, and miscellaneous commands, see the full CLI documentation in the Apache Ignite distribution.
