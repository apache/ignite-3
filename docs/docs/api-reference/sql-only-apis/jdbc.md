---
title: JDBC Driver
id: jdbc
sidebar_position: 1
---

# JDBC Driver

The Apache Ignite 3 JDBC driver implements JDBC 4.x standard for Java applications. It connects directly to cluster nodes using a thin client protocol and requires no server-side libraries.

## Connection String Format

```
jdbc:ignite:thin://host[:port][,host[:port]][/schema][?param=value[&param=value]]
```

The driver supports multiple host addresses for failover. When one node becomes unavailable, the driver automatically attempts to connect to the next address in the list.

Default port: 10800
Default schema: PUBLIC

## Configuration Parameters

### Connection Parameters

- `connectionTimeout` - Socket connection timeout in milliseconds (default: 0, no timeout)
- `connectionTimeZone` - ZoneId string for timestamp conversions (default: system timezone)
- `queryTimeout` - Default query timeout in seconds (default: no timeout)

### Authentication

Basic authentication uses username and password credentials:

- `username` - Authentication username
- `password` - Authentication password

### SSL Configuration

Enable SSL with certificate-based authentication:

- `sslEnabled` - Set to `true` to enable SSL (default: `false`)
- `trustStorePath` - Path to Java trust store file containing trusted certificates
- `trustStorePassword` - Trust store password
- `keyStorePath` - Path to Java key store file containing client private key and certificate
- `keyStorePassword` - Key store password
- `ciphers` - Comma-separated list of allowed cipher suites

### Schema Selection

- `schema` - Default schema for queries (default: `PUBLIC`)

## Parameter Precedence

When the same parameter appears in multiple locations, the driver applies them in this order (highest to lowest):

1. Connection object method calls (e.g., `setNetworkTimeout()`)
2. Last occurrence in connection string
3. Properties object passed to `DriverManager.getConnection()`

## Driver Registration

The driver registers automatically using Java Service Provider Interface (SPI). Load it explicitly if needed:

```java
Class.forName("org.apache.ignite.jdbc.IgniteJdbcDriver");
```

## Usage Examples

### Basic Connection

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

String url = "jdbc:ignite:thin://localhost:10800";

try (Connection conn = DriverManager.getConnection(url)) {
    try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM users")) {
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                System.out.println(id + ": " + name);
            }
        }
    }
}
```

### Connection with Authentication

```java
String url = "jdbc:ignite:thin://localhost:10800?username=admin&password=secret";

try (Connection conn = DriverManager.getConnection(url)) {
    // Execute queries
}
```

Alternative using Properties:

```java
String url = "jdbc:ignite:thin://localhost:10800";
Properties props = new Properties();
props.setProperty("username", "admin");
props.setProperty("password", "secret");

try (Connection conn = DriverManager.getConnection(url, props)) {
    // Execute queries
}
```

### Connection with SSL

```java
String url = "jdbc:ignite:thin://localhost:10800" +
    "?sslEnabled=true" +
    "&trustStorePath=/path/to/truststore.jks" +
    "&trustStorePassword=changeit" +
    "&keyStorePath=/path/to/keystore.jks" +
    "&keyStorePassword=changeit";

try (Connection conn = DriverManager.getConnection(url)) {
    // Execute queries over SSL
}
```

### Multiple Node Addresses

```java
String url = "jdbc:ignite:thin://node1:10800,node2:10800,node3:10800/mySchema";

try (Connection conn = DriverManager.getConnection(url)) {
    // Connection attempts nodes in order until one succeeds
}
```

### Prepared Statements

```java
String sql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";

try (Connection conn = DriverManager.getConnection(url);
     PreparedStatement pstmt = conn.prepareStatement(sql)) {

    pstmt.setInt(1, 101);
    pstmt.setString(2, "John Doe");
    pstmt.setString(3, "john@example.com");

    int rowsAffected = pstmt.executeUpdate();
}
```

### Batch Operations

```java
String sql = "INSERT INTO users (id, name) VALUES (?, ?)";

try (Connection conn = DriverManager.getConnection(url);
     PreparedStatement pstmt = conn.prepareStatement(sql)) {

    conn.setAutoCommit(false);

    for (int i = 0; i < 1000; i++) {
        pstmt.setInt(1, i);
        pstmt.setString(2, "User " + i);
        pstmt.addBatch();
    }

    int[] results = pstmt.executeBatch();
    conn.commit();
}
```

### Transaction Control

```java
try (Connection conn = DriverManager.getConnection(url)) {
    conn.setAutoCommit(false);

    try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO accounts (id, balance) VALUES (1, 1000)");
        stmt.executeUpdate("INSERT INTO accounts (id, balance) VALUES (2, 2000)");

        conn.commit();
    } catch (SQLException e) {
        conn.rollback();
        throw e;
    }
}
```

### Query Timeout

```java
try (Connection conn = DriverManager.getConnection(url);
     Statement stmt = conn.createStatement()) {

    stmt.setQueryTimeout(30); // 30 seconds

    try (ResultSet rs = stmt.executeQuery("SELECT * FROM large_table")) {
        // Process results
    }
}
```

### Fetch Size Configuration

```java
try (Connection conn = DriverManager.getConnection(url);
     Statement stmt = conn.createStatement()) {

    stmt.setFetchSize(2048); // Fetch 2048 rows per page (default: 1024)

    try (ResultSet rs = stmt.executeQuery("SELECT * FROM large_table")) {
        while (rs.next()) {
            // Process rows in pages of 2048
        }
    }
}
```

## Ignite-Specific Behavior

### Type Mapping

The driver maps SQL types to Java types according to JDBC specification:

| SQL Type | Java Type |
|----------|-----------|
| BOOLEAN | boolean / Boolean |
| TINYINT | byte / Byte |
| SMALLINT | short / Short |
| INTEGER | int / Integer |
| BIGINT | long / Long |
| FLOAT | float / Float |
| REAL | float / Float |
| DOUBLE | double / Double |
| DECIMAL | java.math.BigDecimal |
| DATE | java.sql.Date |
| TIME | java.sql.Time |
| TIMESTAMP | java.sql.Timestamp |
| CHAR | String |
| VARCHAR | String |
| BINARY | byte[] |
| VARBINARY | byte[] |
| UUID | java.util.UUID |

### Result Set Characteristics

- **Type**: `TYPE_FORWARD_ONLY` (cursor moves forward only)
- **Concurrency**: `CONCUR_READ_ONLY` (results cannot be updated)
- **Holdability**: Configurable via connection or statement

### Pagination

The driver fetches results in pages (default: 1024 rows per page). For large result sets, increase fetch size to reduce network round trips:

```java
stmt.setFetchSize(4096);
```

### Statement Cancellation

Cancel long-running queries from another thread:

```java
Statement stmt = conn.createStatement();

// In another thread
stmt.cancel();
```

The driver uses correlation tokens to identify and cancel specific queries.

### Network Timeout

Set network-level timeout separately from query timeout:

```java
conn.setNetworkTimeout(executor, 5000); // 5 seconds
```

Query timeout controls server-side execution time. Network timeout controls socket read/write operations.

## Connection String Examples

```
# Basic
jdbc:ignite:thin://localhost:10800

# With schema
jdbc:ignite:thin://localhost:10800/analytics

# With authentication
jdbc:ignite:thin://localhost:10800?username=admin&password=secret

# With SSL
jdbc:ignite:thin://localhost:10800?sslEnabled=true&trustStorePath=/opt/certs/truststore.jks&trustStorePassword=changeit

# Multiple nodes with timeouts
jdbc:ignite:thin://node1:10800,node2:10800,node3:10800?connectionTimeout=5000&queryTimeout=60

# Complete configuration
jdbc:ignite:thin://node1:10800,node2:10800/mySchema?username=admin&password=secret&sslEnabled=true&trustStorePath=/opt/certs/truststore.jks&trustStorePassword=changeit&keyStorePath=/opt/certs/keystore.jks&keyStorePassword=changeit&connectionTimeout=5000&queryTimeout=60
```

## Reference

### Driver Class

`org.apache.ignite.jdbc.IgniteJdbcDriver`

### JDBC Compliance

- JDBC 4.x specification compliant
- Supports `java.sql.Connection`, `Statement`, `PreparedStatement`, `ResultSet`
- Implements `DatabaseMetaData` for schema discovery
- Supports `ResultSetMetaData` for column information

### Limitations

- Result sets are forward-only (no scrolling)
- Result sets are read-only (no updates via ResultSet API)
- `CallableStatement` not supported (no stored procedures)
- `lastRowid` always returns null (no auto-generated key tracking)
