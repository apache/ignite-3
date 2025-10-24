---
title: Java API
id: java-index
sidebar_position: 1
---

# Java API

The primary API for Apache Ignite 3 with complete feature support across all platform capabilities.

## Overview

The Java API provides two deployment modes. The thin client connects to remote clusters without joining topology. The embedded node participates in cluster operations and storage.

Both modes share common APIs for tables, SQL, transactions, compute, and streaming. The embedded mode adds catalog management for schema operations and deployment control for compute jobs.

## API Categories

### Connection APIs
- [Client API](./client-api) - Thin client connection and configuration
- [Server API](./server-api) - Embedded node initialization and lifecycle

### Data Access APIs
- [Tables API](./tables-api) - Record and key-value operations with typed and binary views
- [Data Streamer API](./data-streamer-api) - High-throughput bulk loading with reactive streams
- [SQL API](./sql-api) - Query execution with prepared statements and result processing

### Transaction and Compute APIs
- [Transactions API](./transactions-api) - ACID transaction management with explicit and closure-based patterns
- [Compute API](./compute-api) - Distributed job execution with colocated processing and broadcast

### Schema and Query APIs
- [Catalog API](./catalog-api) - Schema management with annotations and fluent builders
- [Criteria API](./criteria-api) - Type-safe predicate construction for table queries

### Infrastructure APIs
- [Network API](./network-api) - Cluster topology discovery and node information
- [Security API](./security-api) - Authentication configuration and credential management

## Reference

- [Java API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/javadoc/)

## Next Steps

- [Getting Started](../../../getting-started/) - Quick start tutorials
- [Develop and Build](../../../develop/) - Application development guides
