---
id: install-docker
title: Install Using Docker
sidebar_label: Docker
---

## Prerequisites

### Recommended Docker Version

Apache Ignite 3 requires Docker 20.10 or later.

### Recommended Operating System

Apache Ignite supports Linux, macOS, and Windows operating systems.

### Recommended Java Version

Apache Ignite 3 requires Java 11 or later.

## Running a Node

Run Apache Ignite in a docker container using the `docker run` command. Docker will automatically pull the appropriate Apache Ignite version:

```shell
docker run -d -p 10300:10300 -p 10800:10800 -p 3344:3344 ignite/ignite3:latest
```

:::note
If you plan to store persistent data, it is recommended to mount a volume for it. Otherwise, data will be deleted when the container is removed.

Consider storing the `/opt/ignite/work` folder to keep application data and persistent data.
:::

This command launches a single Apache Ignite node. After you run the command, you can check if Apache Ignite is running in the container logs.

## Running a Cluster

You can use the docker-compose file to start an entire cluster in docker. You can download a sample docker-compose file and run a 3-node cluster:

- Download the docker-compose (file not available in docs) file.

:::note
Docker compose version 2.23.1 or later is required to use the provided compose file.
:::

- Download the docker image:

```shell
docker pull apache/ignite3:{version}
```

- Run the docker compose command:

```shell
docker compose -f docker-compose.yml up -d
```

3 nodes will start in Docker and will be available from the CLI tool that can be run locally. Remember to initialise the cluster from the command line tool before working with it.

## Running CLI Tool in Docker

:::note
It is not recommended to run the CLI tool in docker. Instead, we recommend to [download and install](/3.1.0/configure-and-operate/installation/install-zip) CLI tool locally.
:::

CLI tool is used to manage Apache Ignite nodes. By default, docker nodes are isolated and run on different networks, so CLI tool will not be able to connect to the target container from another container. To fix that, you need to create a network and add all containers running the nodes to it.

- Create a new network with the `network create` command:

```shell
docker network create ignite-network
```

- Add any containers with nodes that are already running to the network:

```shell
docker network connect ignite-network {container-id}
```

- Start the container with the Apache Ignite CLI tool on the same network:

```shell
docker run -p 10301:10300 -p 10801:10800 -p 3345:3344 -it --network=ignite-network apache/ignite3:{version} cli
```

:::tip
You may need to mount configuration or data files. To provide them, mount the files you will use, for example:

```shell
docker run --rm -it --network=host -v /opt/etc/config.conf:/opt/ignite/etc/ignite-config.conf apache/ignite3:{version} cli
```
:::

The CLI will be able to connect to the IP address of the node. If you are not sure what the address is, use the `container inspect` command to check it:

```shell
docker container inspect {container-id}
```
