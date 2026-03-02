---
title: Storage Engines and Profiles
sidebar_label: Storage Engines and Profiles
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

## Storage Engines

Storage engines in Apache Ignite 3 are responsible for storing data on disk or in memory.

Storage engines are configured at the node level in the node configuration under the `ignite.storage.engines` section and are referenced by storage profiles, which are then used by distribution zones.

To check available storage engines on a node, use:

```
node config show ignite.storage.engines
```

## Storage Profiles

Storage profiles reference a specific engine and provide configuration parameters for that engine. Profiles are defined under the `ignite.storage.profiles` section and can be referenced by distribution zones when creating tables.

To check available storage profiles on a node, use:

```
node config show ignite.storage.profiles
```

## Scope of Configuration

Storage engine configuration is node-local, meaning each node in the cluster can have different engine configurations. While tables, zones, and profile names are cluster-wide, the actual implementation of a storage profile is specific to each node. This allows for flexibility in configuring storage characteristics based on the hardware capabilities of individual nodes.

When tables use a specific storage profile, they will be stored on each node according to that node's local configuration of the specified profile. If a node does not have a required profile configured, it will not be able to store the table data.


## Available Storage Engines

Apache Ignite 3 provides the following storage engines:

| Engine | Description | Status |
|--------|-------------|--------|
| [aimem](aimem.md) | Volatile (in-memory) Apache Ignite page memory (B+ tree) for high-performance, non-persistent workloads | Core |
| [aipersist](aipersist.md) | Persistent Apache Ignite page memory (B+ tree) for durable storage with low latency access | Core |
| [rocksdb](rocksdb.md) | Persistent RocksDB (LSM tree) optimized for high write throughput | Experimental |
