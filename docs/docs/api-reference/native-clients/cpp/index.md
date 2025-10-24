---
title: C++ API
id: cpp-index
sidebar_position: 3
---

# C++ API

High-performance C++ client with modern C++17 features and zero-copy operations.

## Overview

The C++ API provides thin client access to Apache Ignite 3 clusters. Operations use callbacks for async execution and support timeout configuration.

The API offers both typed views with template specialization and binary views with ignite_tuple for schema-free operations. Memory management uses shared_ptr for automatic resource cleanup.

## API Categories

### Connection API
- [Client API](./client-api) - ignite_client configuration, connection lifecycle, and API access

### Data Access APIs
- [Tables API](./tables-api) - Record and key-value views with typed and binary tuple access
- [SQL API](./sql-api) - Query execution with parameterized statements, pagination, and cancellation

### Transaction and Compute APIs
- [Transactions API](./transactions-api) - Transaction control with explicit commit and rollback operations
- [Compute API](./compute-api) - Distributed job execution with targeting strategies and priority management

### Infrastructure API
- [Network API](./network-api) - Cluster node information and topology filtering

## Reference

- [C++ API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/cppdoc/)

## Next Steps

- [C++ Client Guide](../../../develop/ignite-clients/cpp-client) - Client setup and build configuration
- [Getting Started](../../../getting-started/) - Quick start tutorials
