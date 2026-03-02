---
title: Getting Started
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide shows you how to start working with Ignite. In this guide, we will download Ignite from the website, install it, start the database and perform some simple SQL queries by using the provided CLI tool.

We will be using the [zip archive](/3.1.0/configure-and-operate/installation/install-zip) to demonstrate how to use Ignite. When using [deb or rpm packages](/3.1.0/configure-and-operate/installation/install-deb-rpm), or when running Ignite in Docker, some steps may be different.

If you are more comfortable with running the database from Java code, you can try [starting Ignite from code](/3.1.0/getting-started/embedded-mode).

## Prerequisites

This section describes the platform requirements for machines running Ignite. Ignite system requirements scale depending on the size of the cluster.

| Requirement | Version |
|-------------|---------|
| JDK | 11 and later |
| OS | Linux (Debian and Red Hat flavours), Windows 10 or 11 |
| ISA | x86 or x64 |

## Install Ignite

1. [Download](https://ignite.apache.org/download.cgi) Ignite from the website. This archive contains everything related to the Ignite database itself.

2. Also from the same page, download the [Ignite command line interface](/3.1.0/tools/cli-commands). This tool is the main way of interacting with Ignite database and will be used in the tutorial.

3. Unpack the downloaded archives:

<Tabs>
<TabItem value="unix" label="Unix">

```shell
unzip ignite3-3.0.0.zip
```

</TabItem>
<TabItem value="windows-ps" label="Windows (PowerShell)">

```shell
Expand-Archive ignite3-3.0.0.zip -DestinationPath .
```

</TabItem>
<TabItem value="windows-cmd" label="Windows (CMD)">

```shell
unzip -xf ignite3-3.0.0.zip
```

</TabItem>
</Tabs>

Now you should have the `ignite3-db-3.1.0` and `ignite3-cli-3.1.0` directories that we will be using in this tutorial.

## Start Ignite Node

Ignite is a distributed database, that runs on a collection of **nodes** - Ignite database instances that contain data. When running Ignite, you would typically run multiple nodes - a **cluster**, that shares information and evenly distributes data across its nodes. In this part of the tutorial, we will only run one node, but a later part shows how you can start multiple.

To start a locally running node:

1. Navigate to the `ignite3-db-3.0.0` directory.
2. Run the `ignite3db` script:

<Tabs>
<TabItem value="linux" label="Linux">

```shell
bin/ignite3db
```

</TabItem>
<TabItem value="windows" label="Windows">

:::note
You need to install Java in the Bash environment to run Ignite on Windows.
:::

```bash
bash bin\ignite3db
```

</TabItem>
</Tabs>

## Start the Ignite CLI

The primary means of interacting with your nodes and cluster is the [Ignite CLI](/3.1.0/tools/cli-commands). It can connect to a node running on a local or remote machine, and is the main tool that is used to manually configure and manage the database. In this example, we will be connecting to a local node.

To start the Ignite CLI:

1. Navigate to the `ignite3-cli-3.0.0` directory.
2. Run the following command:

<Tabs>
<TabItem value="linux" label="Linux">

```shell
bin/ignite3
```

</TabItem>
<TabItem value="windows" label="Windows">

:::note
You need to install Java in the Bash environment to run Ignite on Windows.
:::

```bash
bash bin\ignite3
```

</TabItem>
</Tabs>

3. Confirm the connection the CLI tool attempts to establish with the node running on the default URI.

4. If your node is running at a different address, use the `connect` command to connect to the node. For example:

<Tabs>
<TabItem value="command" label="Command">

```
connect http://127.0.0.1:10300
```

</TabItem>
<TabItem value="output" label="Output">

```
Connected to http://127.0.0.1:10300
```

</TabItem>
</Tabs>

## Initialize Your Cluster

Ignite database functions as a cluster. Even if we are currently only running a single node, theoretically you could start another node and have it join the already running cluster. When the nodes are started, they find each other and wait for the user to start the cluster. The process of starting a cluster is called _initialization_.

To initialize the cluster with the node you have started (see [Start Ignite Node](#start-ignite-node)), run the following command:

<Tabs>
<TabItem value="command" label="Command">

```
cluster init --name=sampleCluster
```

</TabItem>
<TabItem value="output" label="Output">

```
Cluster was initialized successfully
```

</TabItem>
</Tabs>

Optionally, you can pass the `--metastorage-group` parameter to specify the nodes that will be used to store cluster meta information. In most scenarios, you want to have 3, 5 or 7 metastorage group nodes.  For more information on what they are and cluster lifecycle, see [Cluster Lifecycle](/3.1.0/configure-and-operate/operations/lifecycle).

:::warning
Cluster and node configurations in Ignite are separated and cannot be used interchangeably. When initializing a cluster, make sure to provide the **cluster** configuration file.
:::

## Run SQL Statements Against the Cluster

Once your cluster has been initialized, you can start working with it. In this tutorial, we will be using the CLI tool to create a table, insert some rows and retrieve data. In most real scenarios you would have a [client](/3.1.0/develop/ignite-clients/) writing data to a cluster and retrieving it, but the CLI tool can still be used for debugging or minor adjustments.

To work with the SQL in CLI:

1. Enter the SQL REPL mode. In this mode, you will have access to SQL hints and command completion:

<Tabs>
<TabItem value="command" label="Command">

```
sql
```

</TabItem>
<TabItem value="output" label="Output">

```
sql-cli>
```

</TabItem>
</Tabs>

2. Use the `CREATE TABLE` statement to create a new table:

<Tabs>
<TabItem value="command" label="Command">

```sql
CREATE TABLE IF NOT EXISTS Person (id int primary key,  city varchar,  name varchar,  age int,  company varchar)
```

</TabItem>
<TabItem value="output" label="Output">

```
Updated 0 rows.
```

</TabItem>
</Tabs>

3. Fill the table with data using the `INSERT` statement:

<Tabs>
<TabItem value="command" label="Command">

```sql
INSERT INTO Person (id, city, name, age, company) VALUES (1, 'London', 'John Doe', 42, 'Apache')
INSERT INTO Person (id, city, name, age, company) VALUES (2, 'New York', 'Jane Doe', 36, 'Apache')
```

</TabItem>
<TabItem value="output" label="Output">

```
Updated 1 rows.
```

</TabItem>
</Tabs>

4. Get all the data you inserted in the previous step:

<Tabs>
<TabItem value="command" label="Command">

```sql
SELECT * FROM Person
```

</TabItem>
<TabItem value="output" label="Output">

```
╔════╤══════════╤══════════╤═════╤═════════╗
║ ID │ CITY     │ NAME     │ AGE │ COMPANY ║
╠════╪══════════╪══════════╪═════╪═════════╣
║ 2  │ New York │ Jane Doe │ 36  │ Apache  ║
╟────┼──────────┼──────────┼─────┼─────────╢
║ 1  │ London   │ John Doe │ 42  │ Apache  ║
╚════╧══════════╧══════════╧═════╧═════════╝
```

</TabItem>
</Tabs>

5. If needed, exit the REPL mode with the `exit` command.

:::note
For more information about available SQL statements, see the [SQL Reference](/3.1.0/sql/reference/language-definition/ddl) section.
:::

## Stop the Node

After you are done working with your cluster, you need to stop the node by stopping the `ignite3db` process:

* Unix: `Control + C`
* Windows: `Ctrl+C`

You can also exit the CLI tool with the `exit` command.

The cluster will remain initialized, and ready once again when you restart the node.

## Extended Cluster Startup Tutorial

Ignite 3 is designed to work in a cluster of 3 or more nodes at once. While a single node can be used in some scenarios and can be used for the tutorial, having multiple nodes in a cluster is the most common use case. The steps below provide optional alternatives to starting your cluster, in case you want to run the tutorial on multiple nodes in a cluster that is closer to what would be encountered in real scenarios.

### Optional: Starting Multiple Ignite Nodes in Docker

To run multiple instances of Ignite, you would normally install it on multiple machines before starting a cluster. If you want to run an Ignite cluster on local VMs for this tutorial, we recommend using a Docker image:

1. Download the docker-compose (file not available in docs) and node configuration (file not available in docs) that will be used by docker compose to start the cluster. The node configuration should be in the same folder as docker compose file
2. Download the Docker image:

<Tabs>
<TabItem value="command" label="Command">

```shell
docker pull apacheignite/ignite:3.0.0
```

</TabItem>
<TabItem value="output" label="Output">

```
latest: Pulling from ignite/ignite3
3713021b0277: Pull complete
fea31cb87980: Pull complete
07f7cfe80ff6: Pull complete
ab1fd3f4849e: Pull complete
34896af28f87: Pull complete
Digest: sha256:43ab9cfb8f58b66e4a5027d4ed529216963d0bcab3fa3fc6d5e2042fa3dd5a74
Status: Downloaded newer image for ignite/ignite3:latest
docker.io/ignite/ignite3:latest
```

</TabItem>
</Tabs>

3. Run the Docker compose command, providing the previously downloaded compose file:

<Tabs>
<TabItem value="command" label="Command">

```shell
docker compose -f docker-compose.yml up -d
```

</TabItem>
<TabItem value="output" label="Output">

```
[+] Running 4/4
 ✔ Network ignite3_default    Created                                                                            0.8s
 ✔ Container ignite3-node1-1  Started                                                                            3.2s
 ✔ Container ignite3-node2-1  Started                                                                            1.7s
 ✔ Container ignite3-node3-1  Started                                                                            3.4s
```

</TabItem>
</Tabs>

3 nodes start in Docker and become available through the CLI tool that can be run locally.

4. Make sure you initialize your cluster before attempting to work with it:

<Tabs>
<TabItem value="command" label="Command">

```
cluster init --name=sampleCluster
```

</TabItem>
<TabItem value="output" label="Output">

```
Cluster was initialized successfully
```

</TabItem>
</Tabs>

### Optional: Start Multiple Ignite Nodes on Different Hosts

In the examples above, we were running a single node, or a small cluster that used predefined configuration. Creating a Ignite cluster on several hosts involves adjustments to its configuration.

#### List all Nodes in NodeFinder

When nodes are running, they use the node finder configuration. When the node starts, it loads the configuration file from `/etc/ignite-config.conf`. Add the addresses to the `network.nodeFinder` configuration, for example for the 3-node cluster:

```json
{
  "ignite" : {
    "nodeFinder" : {
      "netClusterNodes" : [
        "localhost:3344",
        "otherhost:3344",
        "thirdhost:3344"
      ]
    }
  }
}
```

Now, when the node starts, it automatically tries to find nodes at the listed addresses. You can see the current configuration of a running node at any point by running the following command from the CLI tool:

<Tabs>
<TabItem value="command" label="Command">

```
node config show ignite.network.nodeFinder
```

</TabItem>
<TabItem value="output" label="Output">

```
{
  "netClusterNodes" : [ "localhost:3344", "otherhost:3344", "thirdhost:3344" ],
  "type" : "STATIC"
}
```

</TabItem>
</Tabs>

If the node is already running, you can also use the CLI tool to change node configuration, for example:

```
node config update ignite.network.nodeFinder.netClusterNodes=["localhost:3344", "otherHost:3344"]
```

This change requires the node restart to take effect.

#### Change Node Names

You need to make sure that all nodes in the cluster have different names. Node name is defined in the `/etc/vars.env` file. Change the `NODE_NAME` variable to have unique name for each node in cluster, otherwise it will be impossible for the nodes with conflicting names to enter the same cluster.

#### Start all Nodes

Start each node as described in [Start Ignite Node](#start-ignite-node).

#### Initialize Your Cluster

Before initializing the cluster, it is important to check that all nodes found each other and can connect into a cluster. Nodes visible to each other, but not necessarily connected into a cluster form [physical topology](/3.1.0/configure-and-operate/operations/lifecycle). You can check it by connecting to any node using the CLI tool and executing the following command:

<Tabs>
<TabItem value="command" label="Command">

```shell
cluster topology physical
```

</TabItem>
<TabItem value="output" label="Output">

```
╔═══════╤════════════╤══════╤═══════════════╤══════════════════════════════════════╗
║ name  │ host       │ port │ consistent id │ id                                   ║
╠═══════╪════════════╪══════╪═══════════════╪══════════════════════════════════════╣
║ node1 │ 172.19.0.4 │ 3344 │ node1         │ 0c61dad3-bc4c-4c60-8772-1a903632dcb4 ║
╟───────┼────────────┼──────┼───────────────┼──────────────────────────────────────╢
║ node2 │ 172.19.0.2 │ 3344 │ node2         │ 21f516bd-0774-4c53-bbfb-ad21bc21c500 ║
╟───────┼────────────┼──────┼───────────────┼──────────────────────────────────────╢
║ node3 │ 172.19.0.3 │ 3344 │ node3         │ b2bbfbff-eb08-4252-b154-681c49164708 ║
╚═══════╧════════════╧══════╧═══════════════╧══════════════════════════════════════╝
```

</TabItem>
</Tabs>

The command lists the nodes visible to the node you are connecting to, their addresses, names, and IDs. Once you are certain all nodes are running and visible, initialize your cluster:

<Tabs>
<TabItem value="command" label="Command">

```shell
cluster init --name=sampleCluster
```

</TabItem>
<TabItem value="output" label="Output">

```
Cluster was initialized successfully
```

</TabItem>
</Tabs>

Once the cluster starts, the nodes in it will form the _logical topology_. You can check if all nodes have entered the cluster by using the following command:

<Tabs>
<TabItem value="command" label="Command">

```shell
cluster topology logical
```

</TabItem>
<TabItem value="output" label="Output">

```
╔═══════╤════════════╤══════╤═══════════════╤══════════════════════════════════════╗
║ name  │ host       │ port │ consistent id │ id                                   ║
╠═══════╪════════════╪══════╪═══════════════╪══════════════════════════════════════╣
║ node1 │ 172.19.0.4 │ 3344 │ node1         │ 0c61dad3-bc4c-4c60-8772-1a903632dcb4 ║
╟───────┼────────────┼──────┼───────────────┼──────────────────────────────────────╢
║ node2 │ 172.19.0.2 │ 3344 │ node2         │ 21f516bd-0774-4c53-bbfb-ad21bc21c500 ║
╟───────┼────────────┼──────┼───────────────┼──────────────────────────────────────╢
║ node3 │ 172.19.0.3 │ 3344 │ node3         │ b2bbfbff-eb08-4252-b154-681c49164708 ║
╚═══════╧════════════╧══════╧═══════════════╧══════════════════════════════════════╝
```

</TabItem>
</Tabs>

If all nodes are in the command output, the cluster is now started and can be worked with.

## Next Steps

From here, you may want to:

* Check out the [Ignite CLI Tool](/3.1.0/tools/cli-commands) page for more detail on supported commands
* Try out our [examples](https://github.com/apache/ignite-3/tree/main/examples)
