---
title: Volatile Storage
sidebar_label: Volatile Storage
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

## Overview

Apache Ignite Volatile storage is designed to provide a quick and responsive storage without guarantees of data persistence.

When it is enabled for the data region, Apache Ignite stores all data in the data region in RAM. Data will be lost on cluster shutdown, so make sure to have a separate data region for persistent storage.

## Profile Configuration

Each Apache Ignite storage engine can have several storage profiles. Each profile has the following properties:

| Property | Default | Description | Changeable | Requires Restart | Acceptable Values |
|----------|---------|-------------|------------|------------------|-------------------|
| aimem.initSizeBytes | 268435456 | Initial memory region size in bytes, when the used memory size exceeds this value, new chunks of memory will be allocated. | Yes | Yes | Min 256Mb, max defined by the addressable memory limit of the OS |
| aimem.maxSizeBytes | (268435456 or 20% of physical memory) | Maximum memory region size in bytes. Calculated automatically as 256Mb or 20% of available memory, whichever is larger. | Yes | Yes | Min 256Mb, max defined by the addressable memory limit of the OS |

## Configuration Example

In Apache Ignite 3, you can create and maintain configuration in either HOCON or JSON. The configuration file has a single root "node," called `ignite`. All configuration sections are children, grandchildren, etc., of that node. The example below shows how to configure one data region that uses volatile storage.

```json
{
  "ignite" : {
    "storage" : {
      "profiles" : [
        {
           "engine": "aimem",
           "name": "default_aimem",
           "initSizeBytes": 268435456,
           "maxSizeBytes": 268435456
          }
      ]
    }
  }
}
```

You can then use the profile (in this case, `default_aimem`) in your distribution zone configuration.
