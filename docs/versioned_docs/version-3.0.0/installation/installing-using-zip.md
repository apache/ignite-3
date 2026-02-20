---
title: Installing Using ZIP Archive
sidebar_label: Installing Using ZIP Archive
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

{/*
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/}

## Prerequisites

### Recommended Operating System

Linux (Debian and Red Hat flavors), Windows 10 or 11.

### Recommended Java Version

JDK 11 and later.

## Apache Ignite Package Structure

Apache Ignite provides 2 archives for the distribution:

- `ignite3-db-3.0.0` - this archive contains everything related to the Apache Ignite database. When unpacked, it creates the folder where data will be stored by default. You start Apache Ignite nodes from this folder.
- `ignite3-cli-3.0.0` - this archive contains the [Apache Ignite CLI tool](../ignite-cli-tool.md). This tool is the main way of interacting with Apache Ignite clusters and nodes.

## Installing Apache Ignite Database

To install the Apache Ignite database, [download](https://ignite.apache.org/download.cgi) the database archive from the website and then:

1. Unpack the archive:

<Tabs groupId="operating-systems">
  <TabItem value="unix" label="Unix">

```shell
unzip ignite3-db-3.0.0.zip && cd ignite3-db-3.0.0
```

  </TabItem>
  <TabItem value="windows-powershell" label="Windows (PowerShell)">

```shell
Expand-Archive ignite3-3.0.0.zip -DestinationPath . ; cd ignite3-db-3.0.0
```

  </TabItem>
  <TabItem value="windows-cmd" label="Windows (CMD)">

```shell
unzip -xf ignite3-db-3.0.0.zip & cd ignite3-db-3.0.0
```

  </TabItem>
</Tabs>

2. Create the `IGNITE_HOME` environment variable with the path to the `ignite3-db-3.0.0` folder.

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

<Tabs groupId="operating-systems">
  <TabItem value="unix" label="Unix">

```shell
unzip ignite3-cli-3.0.0.zip && cd ignite3-cli-3.0.0
```

  </TabItem>
  <TabItem value="windows-powershell" label="Windows (PowerShell)">

```shell
Expand-Archive ignite3-cli-3.0.0.zip -DestinationPath . ; cd ignite3-cli-3.0.0
```

  </TabItem>
  <TabItem value="windows-cmd" label="Windows (CMD)">

```shell
unzip -xf ignite3-cli-3.0.0.zip & cd ignite3-cli-3.0.0
```

  </TabItem>
</Tabs>

## Next Steps

With Apache Ignite installed, you can proceed with the [Getting Started](../quick-start/getting-started-guide.md) or [use the available APIs](../developers-guide/table-api.md) immediately.
