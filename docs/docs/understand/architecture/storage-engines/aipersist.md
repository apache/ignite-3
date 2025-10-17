---
id: aipersist
title: AIPersist Storage Engine
sidebar_position: 1
---

# Persistent Storage

## Overview

Apache Ignite Persistence is designed to provide a quick and responsive persistent storage. When using the persistent storage, Apache Ignite stores all the data on disk, and loads as much data as it can into RAM for processing.

When persistence is enabled, Apache Ignite stores each partition in a separate file on disk. In addition to data partitions, Apache Ignite stores indexes and metadata.

## Profile Configuration

Each Apache Ignite storage engine can have several storage profiles.

## Checkpointing

*Checkpointing* is the process of copying dirty pages from RAM to partition files on disk. A dirty page is a page that was updated in RAM but was not written to the respective partition file.

After a checkpoint is created, all changes are persisted to disk and will be available if the node crashes and is restarted.

Checkpointing is designed to ensure durability of data and recovery in case of a node failure.

This process helps you utilize disk space frugally by keeping pages in the most up-to-date state on disk.

You can fine-tune checkpoint settings in the [aipersist](/administrators-guide/config/node-config#storage-configuration) storage engine configuration.

## Write Throttling

If a dirty page, scheduled for checkpointing, is updated before being written to disk, its previous state is copied to a special region called a checkpointing buffer. If the buffer overflows, Apache Ignite would have to stop processing all updates until the checkpointing is over. As a result, write performance would drop to zero until the checkpointing cycle is completed.

To avoid the scenario where all updates are stopped, Apache Ignite always performs write throttling once the checkpoint buffer is two thirds full. Once the threshold is reached, checkpoint writer priority is increased, and more priority is given to checkpointing over new updates as the buffer fills more. This prevents buffer overflow while also slowing down update rate.

In most cases, write throttling is caused by a slow drive, or a high update rate, and should not be a part of normal node operation. You can track write throttling by using [throttling metrics](/administrators-guide/metrics/metrics-list#storage.aipersist.profile).

## Storage Configuration

In Apache Ignite 3, all storage configuration is consolidated under `ignite.storage` [node configuration](/administrators-guide/config/node-config#storage-configuration). For more information on how storage is configured, see [Storage Profiles and Engines](/understand/architecture/storage-architecture) documentation.
