---
title: How to Start an Ignite 3 Cluster in Docker
sidebar_label: Start Cluster in Docker
---

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

This guide walks you through the process of setting up and running an Apache Ignite 3 cluster using Docker containers. Follow these steps to get a three-node cluster up and running quickly.

## Prerequisites

- Up-to-date Docker and Docker Compose installed on your system
- Basic familiarity with command-line operations
- The code editor of your choice (VS Code, IntelliJ IDEA, etc.)

## Step 1: Create a Docker Compose Configuration

1. Create a file named `docker-compose.yml` in your project directory:

```yaml
name: ignite3

x-ignite-def: &ignite-def
  image: apacheignite/ignite:3.0.0
  environment:
    JVM_MAX_MEM: "4g"
    JVM_MIN_MEM: "4g"
  configs:
    - source: node_config
      target: /opt/ignite/etc/ignite-config.conf
      mode: 0644

services:
  node1:
    <<: *ignite-def
    command: --node-name node1
    ports:
      - "10300:10300"
      - "10800:10800"
  node2:
    <<: *ignite-def
    command: --node-name node2
    ports:
      - "10301:10300"
      - "10801:10800"
  node3:
    <<: *ignite-def
    command: --node-name node3
    ports:
      - "10302:10300"
      - "10802:10800"

configs:
  node_config:
    content: |
      ignite {
        network {
          port: 3344
          nodeFinder.netClusterNodes = ["node1:3344", "node2:3344", "node3:3344"]
        }
      }
```

## Step 2: Start the Ignite Cluster

1. Open a terminal in the directory containing your `docker-compose.yml` file
2. Run the following command to start the cluster:

```bash
docker compose up -d
```

3. Verify that all containers are running:

```bash
docker compose ps
```

Here is how the command output may look:

```text
NAME              IMAGE                       COMMAND                  SERVICE   CREATED          STATUS          PORTS
ignite3-node1-1   apacheignite/ignite:3.0.0   "docker-entrypoint.s…"   node1     13 seconds ago   Up 10 seconds   0.0.0.0:10300->10300/tcp, 3344/tcp, 0.0.0.0:10800->10800/tcp
ignite3-node2-1   apacheignite/ignite:3.0.0   "docker-entrypoint.s…"   node2     13 seconds ago   Up 10 seconds   3344/tcp, 0.0.0.0:10301->10300/tcp, 0.0.0.0:10801->10800/tcp
ignite3-node3-1   apacheignite/ignite:3.0.0   "docker-entrypoint.s…"   node3     13 seconds ago   Up 10 seconds   3344/tcp, 0.0.0.0:10302->10300/tcp, 0.0.0.0:10802->10800/tcp
```

Your nodes are now running, but the cluster is not initialized.

## Step 3: Initialize the Cluster

1. Start the Ignite CLI in Docker:

```text
docker run --rm -it --network=host -e LANG=C.UTF-8 -e LC_ALL=C.UTF-8 apacheignite/ignite:3.0.0 cli
```

2. Inside the CLI, connect to one of the nodes:

```bash
connect http://localhost:10300
```

3. Confirm the connection to the default node in the CLI tool.

4. Initialize the cluster with a name and the metastorage group of all nodes:

```bash
cluster init --name=ignite3 --metastorage-group=node1,node2,node3
```

The output from this step should be similar to this:

```text
           #              ___                         __
         ###             /   |   ____   ____ _ _____ / /_   ___
     #  #####           / /| |  / __ \ / __ `// ___// __ \ / _ \
   ###  ######         / ___ | / /_/ // /_/ // /__ / / / // ___/
  #####  #######      /_/  |_|/ .___/ \__,_/ \___//_/ /_/ \___/
  #######  ######            /_/
    ########  ####        ____               _  __           _____
   #  ########  ##       /  _/____ _ ____   (_)/ /_ ___     |__  /
  ####  #######  #       / / / __ `// __ \ / // __// _ \     /_ <
   #####  #####        _/ / / /_/ // / / // // /_ / ___/   ___/ /
     ####  ##         /___/ \__, //_/ /_//_/ \__/ \___/   /____/
       ##                  /____/

                      Apache Ignite CLI version 3.0.0


You appear to have not connected to any node yet. Do you want to connect to the default node http://localhost:10300? [Y/n] y
Connected to http://localhost:10300
The cluster is not initialized. Run cluster init command to initialize it.
[node1]> cluster init --name=ignite3 --metastorage-group=node1,node2,node3
Cluster was initialized successfully
```

## Step 4: Verify Your Cluster

1. Use the `cluster status` CLI command to verify your cluster is running correctly.

```bash
cluster status
```

The output should look similar to this:

```text
[name: ignite3, nodes: 3, status: active, cmgNodes: [node1, node2, node3], msNodes: [node1, node2, node3]]
```

This means that all 3 nodes found each other and formed an active cluster.

2. Exit the CLI by typing `exit` or pressing Ctrl+D. This will also stop the CLI container.

Congratulations! You have a local Apache Ignite 3 cluster running that you can use for development.

## Understanding Port Configuration

The Docker Compose file exposes two types of ports for each node:

- **10300-10302**: REST API ports for administrative operations;
- **10800-10802**: Client connection ports for your applications.

## Stopping the Cluster

If you want to pause your cluster:

```bash
docker compose stop

[+] Stopping 3/3
 ✔ Container ignite3-node1-1  Stopped
 ✔ Container ignite3-node2-1  Stopped
 ✔ Container ignite3-node3-1  Stopped
```

This will stop the containers and retain your data.

## Removing the Cluster

When you are done working with the cluster, you can remove it using:

```bash
docker compose down

[+] Running 4/4
 ✔ Container ignite3-node3-1  Removed
 ✔ Container ignite3-node2-1  Removed
 ✔ Container ignite3-node1-1  Removed
 ✔ Network ignite3_default    Removed
```

This will stop and remove all the containers. Your data will be lost unless you have configured persistent storage.
