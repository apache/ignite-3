---
id: disaster-recovery
title: Disaster Recovery
sidebar_label: Overview
---

Disaster recovery operations restore your Apache Ignite cluster when data operations become unfeasible due to consistency issues. These scenarios typically occur when nodes fail or become unreachable, requiring intervention to return data to a consistent state or declare the current state consistent.

Apache Ignite 3 provides recovery mechanisms for two categories of failures:

## Data Partition Recovery

Data partitions store your application data across distribution zones. When partition replicas become unavailable or inconsistent, you need to recover them to restore data access. Recovery procedures depend on whether a minority or majority of replicas are affected.

For detailed procedures, see [Data Partitions Recovery](disaster-recovery-partitions.md).

## System Group Recovery

System groups maintain cluster metadata and coordination. Apache Ignite uses two critical system groups:

- **Cluster Management Group (CMG)**: Manages cluster topology and node membership
- **Metastorage Group**: Stores cluster metadata and configuration

These groups require different recovery procedures than data partitions due to their role in cluster operations.

For detailed procedures, see [System Groups Recovery](disaster-recovery-system-groups.md).

## Recovery Tools

Apache Ignite provides CLI commands for disaster recovery operations:

- `recovery partition states`: Check partition health and availability
- `recovery partitions restart`: Restart partitions to resolve issues
- `recovery partitions reset`: Reset partitions to a consistent state

For complete command reference, see [CLI Commands](/3.1.0/tools/cli-commands).
