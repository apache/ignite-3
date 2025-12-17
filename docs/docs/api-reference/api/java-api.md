---
title: Java API Reference
id: java-api-reference
sidebar_position: 1
---

# Java API Reference

Complete reference documentation for the Apache Ignite 3 Java API.

## Overview

The Java API provides interfaces and classes for all Apache Ignite 3 features. This reference documents public APIs for application development.

## API Documentation

JavaDoc documentation is generated from source code annotations and comments.

### Access the Documentation

<a href="/docs/ignite3/api/java/index.html" target="_blank" rel="noopener noreferrer" style={{
  display: 'inline-block',
  padding: '12px 24px',
  backgroundColor: '#0066cc',
  color: 'white',
  textDecoration: 'none',
  borderRadius: '4px',
  fontWeight: 'bold',
  marginBottom: '20px'
}}>Open Java API Reference →</a>

The locally generated JavaDoc includes all public APIs, with detailed documentation for classes, interfaces, methods, and fields.

### Online Documentation

The latest JavaDoc is published with each release:

- [Latest Release JavaDoc](https://ignite.apache.org/releases/ignite3/3.1.0/javadoc/)

### Generating Local Documentation

Generate JavaDoc locally from the source code:

```bash
./gradlew aggregateJavadoc
```

Generated documentation appears in `build/docs/aggregateJavadoc/`.

## Core Packages

### Client and Server

- `org.apache.ignite` - Entry point interfaces (Ignite, IgniteClient)
- `org.apache.ignite.client` - Thin client implementation

### Data Access

- `org.apache.ignite.table` - Table, RecordView, KeyValueView interfaces
- `org.apache.ignite.table.partition` - Partition management and data streaming
- `org.apache.ignite.sql` - SQL execution and result processing

### Transactions and Compute

- `org.apache.ignite.tx` - Transaction management
- `org.apache.ignite.compute` - Distributed compute jobs and tasks

### Schema Management

- `org.apache.ignite.catalog` - Schema definition with fluent builders
- `org.apache.ignite.table.mapper` - Annotation-based mapping (@Table, @Column, @Id)

### Infrastructure

- `org.apache.ignite.network` - Cluster nodes and network addressing
- `org.apache.ignite.security` - Authentication configuration

## Module Structure

Ignite 3 uses a modular architecture. Key modules include:

- `ignite-api` - Public API interfaces
- `ignite-client` - Thin client implementation
- `ignite-runner` - Embedded node implementation
- `ignite-table` - Table operations
- `ignite-sql-engine` - SQL processing
- `ignite-compute` - Compute engine

## Next Steps

- [Java API Documentation](../native-clients/java/) - Usage guides for each API area
- [Java Client Guide](../../develop/ignite-clients/java-client) - Client setup and configuration
- [Getting Started](../../getting-started/) - Tutorials and examples
