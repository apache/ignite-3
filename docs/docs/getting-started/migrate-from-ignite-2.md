---
title: Migrating from Ignite 2
---

This section describes how to configure an Apache Ignite 3 cluster into which you will migrate all the components of your Apache Ignite 2 cluster.

## Configuration Migration

You need to configure the cluster you have created to match the Apache Ignite 2 cluster you are migrating from.

While cluster configurations in Apache Ignite 2 are XML beans, in Apache Ignite 3 they are in HOCON format. Moreover, many configuration structures in version 3 are different from those in version 2.

In Apache Ignite 3, the configuration file has a single root "node," called `ignite`. All configuration sections are children, grandchildren, etc., of that node.

:::note
In Apache Ignite 3, you can create and maintain the configuration in either JSON or HOCON format.
:::

For example:

```json
{
    "ignite" : {
        "network" : {
            "nodeFinder" : {
                "netClusterNodes" : ["localhost:3344"]
            },
            "port" : 3344
        },
        "storage" : {
            "profiles" : [
                {
                    "name" : "persistent",
                    "engine" : "aipersist"
                }
            ]
        },
        "nodeAttributes.nodeAttributes" : {
            "region" : "US",
            "storage" : "SSD"
        }
    }
}
```

When migrating your environment Apache Ignite 3 configuration is split between Cluster, Node and distribution zone configurations.

### Node Configuration

Node configuration stores information about the locally running node.

#### Storage Configuration

Apache Ignite 3 storage is configured in a completely different manner from Apache Ignite 2.

* First, you configure **storage engine** properties, which may include properties like page size or checkpoint frequency.
* Then, you create a **storage profile**, which defines a specific storage that will be used.
* Then, you create a **distribution zone** using the storage profile, which can be further used to fine-tune the storage by defining where and how to store data across the cluster.
* Finally, each **table** can be assigned to the distribution zone, or directly to a storage profile.

Note:

* Only tables and distribution zones can be configured from code. Storage profiles and engines must be configured by updating node configuration and restarting node.
* Custom affinity functions are replaced by distribution zones.
* External storage is supported via cache storage that must be configured by using SQL.

#### Client Configuration

All clients in Apache Ignite 3 are "thin", and use a similar `clientConnector` configuration. See [Apache Ignite Clients](/3.1.0/develop/ignite-clients/) section for more information on configuring client connector.

#### Network Configuration

Node network configuration is now performed in the `network` section of the [node configuration](/3.1.0/configure-and-operate/reference/node-configuration).

#### REST API Configuration

REST API is a significant part of Apache Ignite 3. It can be used for multiple purposes, including cluster and node configuration and running SQL requests.

You can configure REST properties in [node configuration](/3.1.0/configure-and-operate/reference/node-configuration).

### Cluster Configuration

Cluster configuration applies to all nodes in the cluster. It is automatically propagated across the cluster from the node you apply in at.

#### Handling Events

Events configuration is simplified in Apache Ignite 3. It is separated in 2 configurations:

* Event **channels** define what is collected.
* Event **sinks** define where the data is sent.

In the current release, only `log` sink are supported. You can configure events as described in the [Events](/3.1.0/develop/work-with-data/events) section.

#### Metrics Collection

Apache Ignite 3 has metrics disabled by default.

All metrics are grouped according to their metric sources, and are enabled in cluster configuration per metric source.

Then, these metrics will be available in Apache Ignite JMX beans.

For instructions on configuring metrics, see [Metrics Configuration](/3.1.0/configure-and-operate/configuration/metrics-configuration).

## Code Migration

Code written for Apache Ignite 2 cannot be directly reused, however as most concepts remain similar, code migration should not take too much time.
