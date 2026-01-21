---
title: Client API
id: client-api
sidebar_position: 1
---

# Client API

The C++ client provides a thin client connection to Apache Ignite clusters. It manages network connections, handles authentication, and provides access to all Ignite APIs through a single entry point.

## Key Concepts

### Client Lifecycle

The client uses static factory methods to establish connections. Start methods block until the connection succeeds or times out. The client maintains active connections through configurable heartbeat intervals.

### Configuration

Client configuration specifies connection endpoints, authentication, SSL/TLS settings, and connection parameters. Endpoints use `host:port` format with a default port of 10800. The client maintains a connection pool with configurable limits.

### API Access

The client provides access to all Ignite APIs through dedicated getters:

- `get_tables()` - Table operations
- `get_sql()` - SQL execution
- `get_transactions()` - Transaction management
- `get_compute()` - Distributed computing
- `get_cluster_nodes()` - Cluster topology

## Basic Usage

### Starting a Client

Start a client with default configuration:

```cpp
using namespace ignite;

ignite_client_configuration cfg{{"localhost:10800"}};
ignite_client client = ignite_client::start(cfg, std::chrono::seconds(30));
```

Configure connection parameters:

```cpp
ignite_client_configuration cfg{{"host1:10800", "host2:10800"}};
cfg.set_connection_limit(10);
cfg.set_heartbeat_interval(std::chrono::seconds(30));

ignite_client client = ignite_client::start(cfg, std::chrono::seconds(30));
```

### Asynchronous Startup

Start the client without blocking:

```cpp
ignite_client_configuration cfg{{"localhost:10800"}};

ignite_client::start_async(cfg, std::chrono::seconds(30),
    [](ignite_result<ignite_client> result) {
        if (!result.has_error()) {
            ignite_client client = std::move(result).value();
            // Use client
        }
    });
```

### Authentication

Configure basic authentication:

```cpp
ignite_client_configuration cfg{{"localhost:10800"}};
cfg.set_authenticator(std::make_shared<basic_authenticator>("username", "password"));

ignite_client client = ignite_client::start(cfg, std::chrono::seconds(30));
```

### SSL/TLS Configuration

Enable SSL with certificates:

```cpp
ignite_client_configuration cfg{{"localhost:10800"}};
cfg.set_ssl_mode(ssl_mode::REQUIRE);
cfg.set_ssl_cert_file("/path/to/client.pem");
cfg.set_ssl_key_file("/path/to/client.key");
cfg.set_ssl_ca_file("/path/to/ca.pem");

ignite_client client = ignite_client::start(cfg, std::chrono::seconds(30));
```

### Accessing APIs

Access table operations:

```cpp
auto tables = client.get_tables();
auto table = tables.get_table("my_table");
```

Access SQL:

```cpp
auto sql = client.get_sql();
auto result = sql.execute(nullptr, nullptr, sql_statement("SELECT * FROM t"), {});
```

Access transactions:

```cpp
auto transactions = client.get_transactions();
auto tx = transactions.begin();
// Perform operations
tx.commit();
```

Access compute:

```cpp
auto compute = client.get_compute();
auto nodes = client.get_cluster_nodes();
```

### Configuration Retrieval

Retrieve the active configuration:

```cpp
const ignite_client_configuration& config = client.configuration();
auto endpoints = config.get_endpoints();
auto connection_limit = config.get_connection_limit();
```

## Configuration Options

### Connection Settings

- `set_endpoints(std::vector<std::string>)` - Server endpoints (required, non-empty)
- `set_connection_limit(uint32_t)` - Maximum active connections
- `set_heartbeat_interval(std::chrono::microseconds)` - Heartbeat interval (0 disables heartbeat)

### Security Settings

- `set_authenticator(std::shared_ptr<ignite_client_authenticator>)` - Authentication provider
- `set_ssl_mode(ssl_mode)` - SSL/TLS mode (DISABLE, REQUIRE)
- `set_ssl_cert_file(std::string)` - Client certificate path
- `set_ssl_key_file(std::string)` - Private key path
- `set_ssl_ca_file(std::string)` - CA certificate path

### Logging

- `set_logger(std::shared_ptr<ignite_logger>)` - Custom logger implementation

## Error Handling

Client operations throw `ignite_error` on failure. Async operations deliver errors through the callback result:

```cpp
ignite_client::start_async(cfg, timeout, [](ignite_result<ignite_client> result) {
    if (result.has_error()) {
        // Handle error
        std::cerr << "Connection failed: " << result.error().what_str() << std::endl;
    } else {
        auto client = std::move(result).value();
        // Use client
    }
});
```

## Connection Management

### Heartbeat

Heartbeat keeps connections alive during idle periods. The default interval is 30 seconds. Set to zero to disable:

```cpp
cfg.set_heartbeat_interval(std::chrono::seconds(0)); // Disable heartbeat
```

Disabling heartbeat may cause the server to close idle connections.

### Connection Pooling

The client maintains a pool of connections to cluster nodes. Configure the maximum pool size:

```cpp
cfg.set_connection_limit(20); // Allow up to 20 active connections
```

Connection management happens automatically based on operation distribution.

## Reference

- [C++ API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/cppdoc/)
- [Tables API](./tables-api)
- [SQL API](./sql-api)
- [Transactions API](./transactions-api)
- [Compute API](./compute-api)
- [Network API](./network-api)
