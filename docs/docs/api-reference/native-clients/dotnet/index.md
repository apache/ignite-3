---
title: .NET API
id: dotnet-index
sidebar_position: 2
---

# .NET API

Native .NET client for C# and F# applications with async-first design and LINQ integration.

## Overview

The .NET API provides thin client access to Apache Ignite 3 clusters. All operations use async patterns with Task and ValueTask for non-blocking execution.

The API supports both strongly typed operations with POCOs and dynamic access with IIgniteTuple. LINQ queries convert to server-side execution where possible.

## API Categories

### Connection API
- [Client API](./client-api) - IIgniteClient configuration, connection management, and resource lifecycle

### Data Access APIs
- [Tables API](./tables-api) - Record and key-value views with typed, tuple, and LINQ access patterns
- [Data Streamer API](./data-streamer-api) - Bulk loading with custom server-side receivers and error handling
- [SQL API](./sql-api) - Query execution with parameterized statements, ADO.NET integration, and lazy materialization

### Transaction and Compute APIs
- [Transactions API](./transactions-api) - ACID transactions with explicit control and automatic closure-based patterns
- [Compute API](./compute-api) - Distributed job execution with serialization, colocated processing, and job monitoring

### Infrastructure API
- [Network API](./network-api) - Cluster topology, node discovery, and connection inspection

## Reference

- [.NET API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/dotnetdoc/)

## Next Steps

- [.NET Client Guide](../../../develop/ignite-clients/dotnet-client) - Client setup and usage
- [ADO.NET Integration](../../../develop/integrate/ado-net) - Database connectivity integration
- [LINQ Provider](../../../develop/integrate/linq) - Language-integrated queries
