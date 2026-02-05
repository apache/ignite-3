---
title: C++ API Reference
id: cpp-api-reference
sidebar_position: 3
---

# C++ API Reference

Complete reference documentation for the Apache Ignite 3 C++ API.

## Overview

The C++ API provides headers and libraries for thin client operations. The implementation uses modern C++17 features with callback-based async patterns.

## API Documentation

API documentation is generated from source code comments using Doxygen.

### Access the Documentation

<a href="/docs/ignite3/api/cpp/index.html" target="_blank" rel="noopener noreferrer" style={{
  display: 'inline-block',
  padding: '12px 24px',
  backgroundColor: '#0066cc',
  color: 'white',
  textDecoration: 'none',
  borderRadius: '4px',
  fontWeight: 'bold',
  marginBottom: '20px'
}}>Open C++ API Reference →</a>

The locally generated Doxygen documentation includes all public APIs, with detailed documentation for classes, functions, and types.

### Generating Local Documentation

Generate API documentation from the C++ module:

```bash
cd modules/platforms/cpp
doxygen Doxyfile
```

Generated documentation appears in `modules/platforms/cpp/docs/html/`.

Open `index.html` in a browser to view the reference.

## Core Headers

### Client

- `ignite/client/ignite_client.h` - Client interface and configuration

### Data Access

- `ignite/client/table/tables.h` - Table discovery
- `ignite/client/table/table.h` - Table operations
- `ignite/client/table/record_view.h` - Typed record access
- `ignite/client/table/key_value_view.h` - Typed key-value access
- `ignite/client/table/ignite_tuple.h` - Binary tuple container

### SQL

- `ignite/client/sql/sql.h` - Query execution interface
- `ignite/client/sql/result_set.h` - Result processing

### Transactions and Compute

- `ignite/client/transaction/transactions.h` - Transaction factory
- `ignite/client/transaction/transaction.h` - Transaction control
- `ignite/client/compute/compute.h` - Job execution interface

### Infrastructure

- `ignite/client/network/cluster_node.h` - Node information

## Building Applications

Link against the Ignite client library:

```cmake
find_package(ignite-client REQUIRED)
target_link_libraries(your_app ignite-client)
```

The library provides:

- Client implementation
- Type serialization
- Connection handling
- Protocol implementation

## Compiler Requirements

The C++ client requires:

- C++17 compatible compiler
- CMake 3.10 or later
- OpenSSL (optional, for TLS)

Tested compilers:

- GCC 7.0+
- Clang 5.0+
- MSVC 2017+

## Next Steps

- [C++ API Documentation](../native-clients/cpp) - Usage guides for each API area
- [C++ Client Guide](../../develop/ignite-clients/cpp-client) - Build setup and configuration
- [Getting Started](../../getting-started) - Tutorials and examples
