---
id: rest-api
title: REST API
sidebar_position: 2
---

# REST API

The Apache Ignite clusters provide an [OpenAPI](https://www.openapis.org/) specification that can be used to work with Apache Ignite by standard REST methods.

## REST Connector Configuration

By default, rest connector starts on port 10300. This port can be configured in the `ignite.rest` [node configuration](/3.1.0/configure-and-operate/reference/node-configuration).

## Using HTTP Tools

Once the cluster is started, you can use external tools to monitor the cluster over http, or manage the cluster. In this example, we will use [curl](https://curl.se/) to get cluster status:

```bash
curl 'http://localhost:10300/management/v1/cluster/state'
```

You are not limited to only monitoring, as Apache Ignite REST API provides endpoints that can be used to manage the cluster as well. For example, you can create a [snapshot](/3.1.0/configure-and-operate/operations/disaster-recovery-partitions) via REST:

```bash
curl -H "Content-Type: application/json" -d '{"snapshotType": "FULL","tableNames": "table1,table2","startTimeEpochMilli": 0}' http://localhost:10300/management/v1/snapshot/create
```

## Java Project Configuration

If you want to integrate Apache Ignite REST API closer into your application, we recommend using an [OpenAPI generator](https://github.com/OpenAPITools/openapi-generator) to generate a Java client. Once the client is generated, you can use it to work with REST API from code, for example:

```java
ApiClient client = Configuration.getDefaultApiClient();
// Set base URL
client.setBasePath("http://localhost:10300");

// Get cluster configuration.
ClusterConfigurationApi clusterConfigurationApi = new ClusterConfigurationApi(client);
String configuration = clusterConfigurationApi.getClusterConfiguration();
```
