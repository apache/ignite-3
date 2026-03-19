---
title: Cluster Security
sidebar_label: Cluster Security
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

# Cluster Security

## User Security

By default, all users can perform any updates on the cluster, or [upload arbitrary code to the cluster](../developers-guide/code-deployment/code-deployment.md) and perform remote code execution with [distributed computing](../developers-guide/compute/compute.md). To improve security, we recommend configuring [user roles](security/authentication.md) and enabling authorization on the cluster.

## Communication

By default, nodes use plain-text communication that is vulnerable to malicious actions. Ignite 3 separates communications between cluster nodes and communication with clients.

## Node to Node Communication

Communication between nodes usually happens within the same data center. We recommend the following to improve the security of your cluster:

- Enable SSL for cluster communication with the `ignite.network.ssl` [node configuration](config/node-config.md#network-configuration).
- Run the cluster in a trusted and isolated network.

## Node to Client Communication

Client to server communication may be over internet or otherwise untrusted network. Only the client port (10800 by default) is typically exposed outside of the cluster. To securely interact with your clients:

- Enable SSL for client communication with the `ignite.clientConnector.ssl` [node configuration](config/node-config.md).
- Enable [authentication](security/authentication.md) on the cluster.
