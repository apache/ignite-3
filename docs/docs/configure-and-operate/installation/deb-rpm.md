---
id: install-deb-rpm
title: Install Using DEB and RPM Package
sidebar_label: DEB/RPM Package
---

Apache Ignite can be installed by using the standard package managers for the platform.

## Prerequisites

### Recommended Operating System

Apache Ignite supports Linux, macOS, and Windows operating systems.

### Recommended Java Version

Apache Ignite 3 requires Java 11 or later.

## Installing Deb or RPM Package

Install the Apache Ignite 3 packages:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="deb" label="deb">

```shell
sudo apt-get install ./ignite3-db-{version}.deb --no-install-recommends
sudo apt-get install ./ignite3-cli-{version}.deb --no-install-recommends
```

</TabItem>
<TabItem value="rpm" label="RPM">

```shell
sudo rpm -i ignite3-db-{version}.noarch.rpm
sudo rpm -i ignite3-cli-{version}.noarch.rpm
```

</TabItem>
</Tabs>

The packages will be installed in the following way:

| Folder | Description |
|---|---|
| /usr/share/ignite3db | The root installation of Apache Ignite. |
| /etc/ignite3db | The location of configuration files. |
| /var/log/ignite3db | The location of node logs. |
| /usr/lib/ignite3db | The location of the CLI tool. |

## Running Apache Ignite as a Service

:::note
When running on Windows 10 WSL or Docker, you should start Apache Ignite as a stand-alone process (not as a service). We recommend to [install Apache Ignite 3 using ZIP archive](/3.1.0/configure-and-operate/installation/install-zip) in these environments.
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

With Apache Ignite installed, you can proceed with the [Getting Started Guide](/3.1.0/getting-started/quick-start) or [use the available APIs](/3.1.0/develop/work-with-data/table-api) immediately.
