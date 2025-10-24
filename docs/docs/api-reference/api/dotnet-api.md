---
title: .NET API Reference
id: dotnet-api-reference
sidebar_position: 2
---

# .NET API Reference

Complete reference documentation for the Apache Ignite 3 .NET API.

## Overview

The .NET API provides interfaces and classes for Apache Ignite 3 thin client operations. All APIs use async patterns and support both typed and dynamic access.

## API Documentation

API documentation is generated from XML comments in source code.

### Online Documentation

The latest API reference is published with each release:

- [Latest Release .NET API](https://ignite.apache.org/releases/ignite3/3.1.0/dotnetdoc/)

### Generating Local Documentation

Generate API documentation locally using DocFX:

```bash
cd modules/platforms/dotnet
dotnet tool restore
dotnet docfx docs/docfx.json
```

Generated documentation appears in `modules/platforms/dotnet/docs/_site/`.

Note: The .NET SDK is required to generate documentation locally.

## Core Namespaces

### Client

- `Apache.Ignite` - IIgniteClient interface and configuration

### Data Access

- `Apache.Ignite.Table` - ITable, IRecordView, IKeyValueView interfaces
- `Apache.Ignite.Table.DataStreamer` - Bulk loading with streaming
- `Apache.Ignite.Sql` - Query execution and result sets

### Transactions and Compute

- `Apache.Ignite.Transactions` - ITransactions and ITransaction interfaces
- `Apache.Ignite.Compute` - Distributed job execution

### Infrastructure

- `Apache.Ignite.Network` - Cluster node information

## NuGet Package

Install the client package from NuGet:

```bash
dotnet add package Apache.Ignite
```

The package includes:

- Client implementation
- API interfaces
- Type serialization
- Connection management

## Framework Support

The .NET client supports:

- .NET 6.0 and later
- .NET Standard 2.1 (with limitations)

Async APIs use `Task<T>`, `ValueTask<T>`, and `IAsyncEnumerable<T>` for modern async patterns.

## Next Steps

- [.NET API Documentation](../native-clients/dotnet/) - Usage guides for each API area
- [.NET Client Guide](../../develop/ignite-clients/dotnet-client) - Client setup and configuration
- [ADO.NET Integration](../../develop/integrate/ado-net) - Database connectivity
- [LINQ Provider](../../develop/integrate/linq) - Query integration
