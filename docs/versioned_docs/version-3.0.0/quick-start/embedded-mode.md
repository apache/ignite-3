---
title: Starting With Embedded Mode
sidebar_label: Embedded Mode
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

In most scenarios, you would use Ignite CLI tool to start and manage your Ignite cluster. However, in some scenarios it is preferable to manage the cluster from a Java project. Starting and working with the cluster from code is called "embedded mode".

This tutorial covers how you can start Ignite 3 from your Java project.

:::note
Unlike in Ignite 2, nodes in Ignite 3 are not separated into client and server nodes. Nodes started from embedded mode will be used to store data by default.
:::

## Prerequisites

This section describes the platform requirements for machines running Ignite. Ignite system requirements scale depending on the size of the cluster.

| Requirement | Details |
|-------------|---------|
| JDK | 11 and later |
| OS | Linux (Debian and Red Hat flavours), Windows 10 or 11 |
| ISA | x86 or x64 |

## Add Ignite to Your Project

First, you need to add Ignite to your project. The easiest way to do this is by using Maven:

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <configuration>
                <source>11</source>
                <target>11</target>
            </configuration>
        </plugin>
    </plugins>
</build>

<dependencies>
    <dependency>
        <groupId>org.apache.ignite</groupId>
        <artifactId>ignite-api</artifactId>
        <version>3.0.0</version>
    </dependency>

    <dependency>
        <groupId>org.apache.ignite</groupId>
        <artifactId>ignite-runner</artifactId>
        <version>3.0.0</version>
    </dependency>
</dependencies>
```

## Prepare Ignite Configuration

To start an Ignite node, you will need an Ignite configuration file that specifies all configuration properties of the node. For this tutorial, we recommend [installing](../installation/installing-using-zip.md) Ignite 3 and using a default configuration file from it. This file is stored in the `ignite3-db-3.0.0/etc/ignite-config.conf` file.

## Pass JVM Parameters

The following JVM parameters need to be passed to your application to make proprietary SDK APIs available:

```
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.invoke=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/java.math=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.time=ALL-UNNAMED
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens java.base/jdk.internal.access=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
-Dio.netty.tryReflectionSetAccessible=true
```

## Start Ignite Server Nodes

To start an Ignite node, use the following code snippet:

```java
IgniteServer node = IgniteServer.start("node", configFilePath, workDir);
```

This code snippet starts an Ignite node with the name `node1`, that uses the configuration from the file specified in the `configFilePath` path parameter and uses the folder specified in the `workDir` path parameter to store data. When the node is started, this method returns an instance of `IgniteServer` class that can be used to work with the node.

## Initiate a Cluster

Started nodes find each other by default, but they do not form an operational cluster unless the cluster is initiated. You need to initiate the cluster to activate the node. If there are multiple nodes, once the cluster is activated, they will form a topology and automatically distribute workload between each other.

Use the code snippet below to initiate a cluster:

```java
InitParameters initParameters = InitParameters.builder()
    .metaStorageNodeNames("node")
    .clusterName("cluster")
    .clusterConfiguration({config-with-license})
    .build();

node.initCluster(initParameters);
```

:::note
To start an Ignite 3 cluster, you need to provide a license together with the cluster configuration in the `clusterConfiguration` parameter.
:::

## Get an Ignite Instance

Now that the cluster is started, you can get an instance of the `Ignite` class:

```java
Ignite ignite = node.api();
```

This instance can be used to start working with the cluster. The future will be returned once the cluster is active.

In the following example, you interact with the cluster using the SQL API:

```java
ignite.sql().execute(null, "CREATE TABLE IF NOT EXISTS Person (id int primary key, name varchar, age int);");
ignite.sql().execute(null, "insert into Person (id, name, age) values ('1', 'Person Man', '501'");
try (ResultSet<SqlRow> rs = ignite.sql().execute(null, "SELECT id, name, age from Person")) {
    while (rs.hasNext()) {
        SqlRow row = rs.next();
        System.out.println("    "
                + row.value(1) + ", "
                + row.value(2));
    }
}
```

:::note
Session is closable, but it is safe to skip `close()` method for DDL and DML queries, as they do not keep cursor open.
:::

More examples of working with Ignite can be found in the [examples](https://github.com/apache/ignite-3/tree/main/examples) repository.

## Next Steps

From here, you may want to:

- Check out the [Developers guide](../developers-guide/table-api.md) page for more information on available APIs
- Try out our [examples](https://github.com/apache/ignite-3/tree/main/examples)
