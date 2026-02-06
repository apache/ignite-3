---
title: Storage Profiles
sidebar_label: Storage Profiles
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

## What is a Storage Profile?

Storage profiles are Apache Ignite node entities that define a Storage Engine and its configuration parameters. They create a bridge between tables and the underlying storage engines that store data.

A storage profile defines:

- What storage engine is used to store data
- Configuration values for a particular Storage Engine's configuration properties

Each node in an Apache Ignite cluster can have multiple storage profiles defined, and a table can only have a single storage profile defined.

## Storage Profiles and Distribution Zones

A Distribution Zone must be configured to use a set of declared Storage Profiles, which can be used to parameterize tables created in this Zone with different Storage Engines. When creating a distribution zone, you specify which storage profiles it can use:

```sql
CREATE ZONE exampleZone (PARTITIONS 2, REPLICAS 3) STORAGE PROFILES ['profile1, profile3'];
```

In this case, the tables created in this distribution zone can only use `profile1` or `profile3`.

## Default Storage Profile

Apache Ignite creates a `default` storage profile that uses the persistent Apache Ignite storage engine (`aipersist`) to store data. Unless otherwise specified, distribution zones will use this storage profile to store data.

To check the currently available profiles on the node, use the following command:

```bash
node config show ignite.storage.profiles
```

## Creating and Using Storage Profiles

By default, only the `default` storage profile is created, however a node can have any number of storage profiles on it. To create a new profile, pass the profile configuration to the `storage.profiles` parameter:

```bash
node config update "ignite.storage.profiles:{rocksProfile{engine:rocksdb,size:10000}}"
```

After the configuration is updated, make sure to restart the node. The created storage profile will be available for use by a distribution zone after the restart.

## Defining Tables With Storage Profiles

After you have defined your storage profiles and distribution zones, you can create tables in it by using SQL or from code. Both zone and storage profile cannot be changed after the table has been created.

For example, here is how you create a simple table:

```sql
CREATE TABLE exampleTable (key INT PRIMARY KEY, my_value VARCHAR) ZONE exampleZone STORAGE PROFILE 'profile1';
```

In this case, the `exampleTable` table will be using the storage engine with the parameters specified in the `profile1` storage profile. If the node does not have the `profile1`, the table will not be stored on it. Each node may have different configuration for `profile1`, and data will be stored according to local configuration.
