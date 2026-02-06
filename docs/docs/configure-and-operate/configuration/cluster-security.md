---
id: config-cluster-security
title: Cluster Security
sidebar_label: Cluster Security
---

## User Security

By default, all users can perform any updates on the cluster, or [upload arbitrary code to the cluster](/3.1.0/develop/work-with-data/code-deployment) and perform remote code execution with [distributed computing](/3.1.0/develop/work-with-data/compute). To improve security, we recommend configuring user roles and permissions and enabling authorization on the cluster.

## Communication

By default, nodes use plain-text communication that is vulnerable to malicious actions. Ignite 3 separates communications between cluster nodes and communication with clients.

## Node to Node Communication

Communication between nodes usually happens within the same data center. We recommend the following to improve the security of your cluster:

- Enable SSL for cluster communication with the `ignite.network.ssl` [node configuration](/3.1.0/configure-and-operate/reference/node-configuration).
- Run the cluster in a trusted and isolated network.

## Node to Client Communication

Client to server communication may be over internet or otherwise untrusted network. Only the client port (10800 by default) is typically exposed outside of the cluster. To securely interact with your clients:

- Enable SSL for client communication with the `ignite.clientConnector.ssl` [node configuration](/3.1.0/configure-and-operate/reference/node-configuration).
- Enable [authentication](/3.1.0/configure-and-operate/configuration/config-authentication) on the cluster.
