---
id: install-zip
title: Install Using ZIP Archive
sidebar_label: ZIP Archive
---

## Prerequisites

### Recommended Operating System

Apache Ignite supports Linux, macOS, and Windows operating systems.

### Recommended Java Version

Apache Ignite 3 requires Java 11 or later.

## Apache Ignite Package Structure

Apache Ignite provides 2 archives for distribution:

- `ignite3-db-{version}` - this archive contains everything related to the Apache Ignite database. When unpacked, it creates the folder where data will be stored by default. You start Apache Ignite nodes from this folder.
- `ignite3-cli-{version}` - this archive contains the [Apache Ignite CLI tool](/3.1.0/tools/cli-commands). This tool is the main way of interacting with Apache Ignite clusters and nodes.

## Installing Apache Ignite Database

To install the Apache Ignite database, [download](https://ignite.apache.org/download.cgi) the database archive from the website and then:

1. Unpack the archive:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="unix" label="Unix">

```shell
unzip ignite3-db-{version}.zip && cd ignite3-db-{version}
```

</TabItem>
<TabItem value="windows-powershell" label="Windows (PowerShell)">

```shell
Expand-Archive ignite3-{version}.zip -DestinationPath . ; cd ignite3-db-{version}
```

</TabItem>
<TabItem value="windows-cmd" label="Windows (CMD)">

```shell
unzip -xf ignite3-db-{version}.zip & cd ignite3-db-{version}
```

</TabItem>
</Tabs>

2. Create the `IGNITE_HOME` environment variable with the path to the `ignite3-db-{version}` folder.

## Starting the Node

Once you have unpacked the archive, you can start the Apache Ignite node:

```shell
bin/ignite3db
```

By default, the node loads the `etc/ignite-config.conf` configuration file on startup. You can update it to customize the node configuration, or change the configuration folder in the `etc/vars.env` file.

When the node is started, it will enter the cluster if it is already configured and initialized, or will wait for cluster initialization.

## Installing Apache Ignite CLI Tool

The CLI tool is the primary means of working with the Apache Ignite database. It is not necessary to install on every machine that is running Apache Ignite, as you can connect to the node via REST interface.

To install the Apache Ignite CLI, [download](https://ignite.apache.org/download.cgi) the database archive from the website and then unpack it:

<Tabs>
<TabItem value="unix" label="Unix">

```shell
unzip ignite3-cli-{version}.zip && cd ignite3-cli-{version}
```

</TabItem>
<TabItem value="windows-powershell" label="Windows (PowerShell)">

```shell
Expand-Archive ignite3-cli-{version}.zip -DestinationPath . ; cd ignite3-cli-{version}
```

</TabItem>
<TabItem value="windows-cmd" label="Windows (CMD)">

```shell
unzip -xf ignite3-cli-{version}.zip & cd ignite3-cli-{version}
```

</TabItem>
</Tabs>

## Next Steps

With Apache Ignite installed, you can proceed with the [Getting Started Guide](/3.1.0/getting-started/quick-start) or [use the available APIs](/3.1.0/develop/work-with-data/table-api) immediately.
