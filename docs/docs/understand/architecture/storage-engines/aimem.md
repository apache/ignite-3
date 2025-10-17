---
id: aimem
title: AIMemory Storage Engine
sidebar_position: 3
---

# Volatile Storage

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
