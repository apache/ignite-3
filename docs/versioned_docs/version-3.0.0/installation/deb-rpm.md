---
title: Installing Using DEB and RPM Package
sidebar_label: Installing Using DEB and RPM Package
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

Apache Ignite can be installed by using the standard package managers for the platform.

## Prerequisites

### Recommended Operating System

Linux (Debian and Red Hat flavors), Windows 10 or 11.

### Recommended Java Version

JDK 11 and later.

## Installing Deb or RPM Package

Install the Apache Ignite 3 packages:

<Tabs groupId="package-managers">
  <TabItem value="deb" label="deb">

```shell
sudo apt-get install ./ignite3-db-3.0.0.deb --no-install-recommends
sudo apt-get install ./ignite3-cli-3.0.0.deb --no-install-recommends
```

  </TabItem>
  <TabItem value="rpm" label="RPM">

```shell
sudo rpm -i ignite3-db-3.0.0.noarch.rpm
sudo rpm -i ignite3-cli-3.0.0.noarch.rpm
```

  </TabItem>
</Tabs>

The packages will be installed in the following way:

| Folder | Description |
|--------|-------------|
| /usr/share/ignite3db | The root installation of Apache Ignite. |
| /etc/ignite3db | The location of configuration files. |
| /var/log/ignite3db | The location of node logs. |
| /usr/lib/ignite3db | The location of the CLI tool. |

## Running Apache Ignite as a Service

:::note
When running on Windows 10 WSL or Docker, you should start Apache Ignite as a stand-alone process (not as a service). We recommend to [install Apache Ignite 3 using ZIP archive](installing-using-zip.md) in these environments.
:::

To start an Apache Ignite node with a custom configuration, run the following command:

```bash
sudo systemctl start ignite3db
```

To launch the node at system startup, run the following command:

```bash
sudo systemctl enable ignite3db
```

## Running Apache Ignite as a Stand-Alone Process

Generally, you would want to run Apache Ignite as a service. However, Apache Ignite also provides a startup script that can be used to start it as a stand-alone application. To run it, use the following command:

```bash
sudo bash /usr/share/ignite3db/start.sh 1>/tmp/ignite3-start.log 2>&1 &
```

## Next Steps

With Apache Ignite installed, you can proceed with the [Getting Started](../quick-start/getting-started-guide.md) or [use the available APIs](../developers-guide/table-api.md) immediately.
