---
title: ODBC Driver
id: odbc
sidebar_position: 2
---

# ODBC Driver

The Apache Ignite 3 ODBC driver implements ODBC 3.8 standard for C and C++ applications. It provides SQL access through the standard ODBC API without requiring server-side libraries.

## Connection String Format

```
DRIVER={Apache Ignite 3};ADDRESS=host:port;param=value;param=value
```

Parameters are semicolon-separated key-value pairs. The driver requires either `ADDRESS` or both `HOST` and `PORT`.

Default port: 10800
Default schema: PUBLIC

## Configuration Parameters

### Connection Parameters

- `DRIVER` - ODBC driver name (required): `{Apache Ignite 3}`
- `ADDRESS` - Host and port in format `host:port` (can specify multiple comma-separated addresses)
- `HOST` - Host address (alternative to ADDRESS)
- `PORT` - Port number (alternative to ADDRESS, default: 10800)

### Authentication

- `IDENTITY` - Username for authentication
- `SECRET` - Password for authentication

### Schema Selection

- `SCHEMA` - Default schema for queries (default: `PUBLIC`)

### Performance Tuning

- `PAGE_SIZE` - Number of rows fetched per request (default: 1024)
- `TIMEZONE` - Client timezone for timestamp conversions

### SSL Configuration

- `SSL_MODE` - SSL connection mode: `disable` or `require` (default: `disable`)
- `SSL_KEY_FILE` - Path to PEM-encoded private key file
- `SSL_CERT_FILE` - Path to PEM-encoded certificate file
- `SSL_CA_FILE` - Path to PEM-encoded CA certificate file

## DSN Configuration

Configure Data Source Names (DSN) through your system's ODBC administrator.

### Windows

Use ODBC Data Source Administrator (`odbcad32.exe`):

1. Open ODBC Data Source Administrator
2. Add new data source
3. Select "Apache Ignite 3" driver
4. Configure connection parameters
5. Test connection

### Linux

Edit `/etc/odbc.ini` or `~/.odbc.ini`:

```ini
[IgniteDS]
Driver=Apache Ignite 3
ADDRESS=localhost:10800
SCHEMA=PUBLIC
```

Configure driver location in `/etc/odbcinst.ini`:

```ini
[Apache Ignite 3]
Description=Apache Ignite 3 ODBC Driver
Driver=/usr/local/lib/libignite-odbc.so
```

### macOS

Edit `~/Library/ODBC/odbc.ini`:

```ini
[IgniteDS]
Driver=Apache Ignite 3
ADDRESS=localhost:10800
SCHEMA=PUBLIC
```

Configure driver in `~/Library/ODBC/odbcinst.ini`:

```ini
[Apache Ignite 3]
Description=Apache Ignite 3 ODBC Driver
Driver=/usr/local/lib/libignite-odbc.dylib
```

## Usage Examples

### Basic Connection

```c
#include <sql.h>
#include <sqlext.h>

SQLHENV env;
SQLHDBC dbc;
SQLHSTMT stmt;

// Allocate environment
SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

// Allocate connection
SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

// Connect
SQLCHAR connStr[] = "DRIVER={Apache Ignite 3};ADDRESS=localhost:10800";
SQLDriverConnect(dbc, NULL, connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);

// Allocate statement
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

// Execute query
SQLExecDirect(stmt, (SQLCHAR*)"SELECT id, name FROM users", SQL_NTS);

// Fetch results
SQLINTEGER id;
SQLCHAR name[256];
SQLLEN idLen, nameLen;

SQLBindCol(stmt, 1, SQL_C_LONG, &id, 0, &idLen);
SQLBindCol(stmt, 2, SQL_C_CHAR, name, sizeof(name), &nameLen);

while (SQLFetch(stmt) == SQL_SUCCESS) {
    printf("%d: %s\n", id, name);
}

// Cleanup
SQLFreeHandle(SQL_HANDLE_STMT, stmt);
SQLDisconnect(dbc);
SQLFreeHandle(SQL_HANDLE_DBC, dbc);
SQLFreeHandle(SQL_HANDLE_ENV, env);
```

### DSN Connection

```c
SQLCHAR dsn[] = "DSN=IgniteDS";
SQLConnect(dbc, dsn, SQL_NTS, NULL, 0, NULL, 0);
```

### Connection with Authentication

```c
SQLCHAR connStr[] = "DRIVER={Apache Ignite 3};ADDRESS=localhost:10800;IDENTITY=admin;SECRET=password";
SQLDriverConnect(dbc, NULL, connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
```

### Connection with SSL

```c
SQLCHAR connStr[] =
    "DRIVER={Apache Ignite 3};"
    "ADDRESS=localhost:10800;"
    "SSL_MODE=require;"
    "SSL_CERT_FILE=/path/to/client.pem;"
    "SSL_KEY_FILE=/path/to/client-key.pem;"
    "SSL_CA_FILE=/path/to/ca.pem";

SQLDriverConnect(dbc, NULL, connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
```

### Prepared Statements

```c
SQLCHAR sql[] = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";
SQLPrepare(stmt, sql, SQL_NTS);

SQLINTEGER id = 101;
SQLCHAR name[] = "John Doe";
SQLCHAR email[] = "john@example.com";
SQLLEN idLen = 0, nameLen = SQL_NTS, emailLen = SQL_NTS;

SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &id, 0, &idLen);
SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 0, 0, name, 0, &nameLen);
SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 0, 0, email, 0, &emailLen);

SQLExecute(stmt);
```

### Batch Operations

```c
#define ARRAY_SIZE 100

SQLINTEGER ids[ARRAY_SIZE];
SQLCHAR names[ARRAY_SIZE][256];
SQLLEN idLens[ARRAY_SIZE];
SQLLEN nameLens[ARRAY_SIZE];

// Set array size
SQLULEN arraySize = ARRAY_SIZE;
SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)arraySize, 0);

// Bind arrays
SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, ids, 0, idLens);
SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 256, 0, names, 256, nameLens);

// Populate arrays
for (int i = 0; i < ARRAY_SIZE; i++) {
    ids[i] = i;
    sprintf((char*)names[i], "User %d", i);
    idLens[i] = 0;
    nameLens[i] = SQL_NTS;
}

// Execute batch
SQLCHAR sql[] = "INSERT INTO users (id, name) VALUES (?, ?)";
SQLExecDirect(stmt, sql, SQL_NTS);
```

### Transaction Control

```c
// Disable auto-commit
SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

// Execute multiple statements
SQLExecDirect(stmt, (SQLCHAR*)"INSERT INTO accounts (id, balance) VALUES (1, 1000)", SQL_NTS);
SQLExecDirect(stmt, (SQLCHAR*)"INSERT INTO accounts (id, balance) VALUES (2, 2000)", SQL_NTS);

// Commit
SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);

// Or rollback on error
// SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);

// Re-enable auto-commit
SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_ON, 0);
```

### Error Handling

```c
SQLRETURN ret = SQLExecDirect(stmt, (SQLCHAR*)"SELECT * FROM invalid_table", SQL_NTS);

if (!SQL_SUCCEEDED(ret)) {
    SQLCHAR sqlState[6];
    SQLINTEGER nativeError;
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT messageLen;

    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError,
                  message, sizeof(message), &messageLen);

    printf("Error: %s (%d): %s\n", sqlState, nativeError, message);
}
```

### Metadata Queries

```c
// List tables
SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR*)"TABLE", SQL_NTS);

// List columns for a table
SQLColumns(stmt, NULL, 0, NULL, 0, (SQLCHAR*)"users", SQL_NTS, NULL, 0);

// Get result metadata
SQLCHAR columnName[256];
SQLSMALLINT nameLen, dataType, decimalDigits, nullable;
SQLULEN columnSize;

for (SQLSMALLINT i = 1; i <= columnCount; i++) {
    SQLDescribeCol(stmt, i, columnName, sizeof(columnName), &nameLen,
                   &dataType, &columnSize, &decimalDigits, &nullable);

    printf("Column %d: %s, Type: %d, Size: %lu\n",
           i, columnName, dataType, columnSize);
}
```

## Ignite-Specific Behavior

### Type Mapping

ODBC SQL types map to C types:

| SQL Type | ODBC SQL Type | C Type |
|----------|---------------|--------|
| BOOLEAN | SQL_BIT | SQL_C_BIT |
| TINYINT | SQL_TINYINT | SQL_C_STINYINT |
| SMALLINT | SQL_SMALLINT | SQL_C_SSHORT |
| INTEGER | SQL_INTEGER | SQL_C_SLONG |
| BIGINT | SQL_BIGINT | SQL_C_SBIGINT |
| FLOAT | SQL_FLOAT | SQL_C_FLOAT |
| REAL | SQL_REAL | SQL_C_FLOAT |
| DOUBLE | SQL_DOUBLE | SQL_C_DOUBLE |
| DECIMAL | SQL_DECIMAL | SQL_C_CHAR |
| DATE | SQL_TYPE_DATE | SQL_C_TYPE_DATE |
| TIME | SQL_TYPE_TIME | SQL_C_TYPE_TIME |
| TIMESTAMP | SQL_TYPE_TIMESTAMP | SQL_C_TYPE_TIMESTAMP |
| CHAR | SQL_CHAR | SQL_C_CHAR |
| VARCHAR | SQL_VARCHAR | SQL_C_CHAR |
| BINARY | SQL_BINARY | SQL_C_BINARY |
| VARBINARY | SQL_VARBINARY | SQL_C_BINARY |
| UUID | SQL_GUID | SQL_C_GUID |

### Result Set Characteristics

- **Cursor Type**: Forward-only (SQL_CURSOR_FORWARD_ONLY)
- **Concurrency**: Read-only
- **Scrollable**: No (cursor moves forward only)

### Pagination

The driver fetches results in pages. Configure page size for performance:

```c
// Set page size before executing query
SQLUINTEGER pageSize = 2048;
SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)pageSize, 0);
```

Default page size: 1024 rows.

### Connection Timeout

Set connection timeout during allocation:

```c
// Set 5-second connection timeout
SQLINTEGER timeout = 5;
SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)timeout, 0);
```

### Query Timeout

Set query timeout on statement:

```c
// Set 30-second query timeout
SQLINTEGER timeout = 30;
SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, (SQLPOINTER)timeout, 0);
```

## Connection String Examples

```
# Basic
DRIVER={Apache Ignite 3};ADDRESS=localhost:10800

# With schema
DRIVER={Apache Ignite 3};ADDRESS=localhost:10800;SCHEMA=analytics

# With authentication
DRIVER={Apache Ignite 3};ADDRESS=localhost:10800;IDENTITY=admin;SECRET=password

# With SSL
DRIVER={Apache Ignite 3};ADDRESS=localhost:10800;SSL_MODE=require;SSL_CERT_FILE=/opt/certs/client.pem;SSL_KEY_FILE=/opt/certs/client-key.pem;SSL_CA_FILE=/opt/certs/ca.pem

# Multiple nodes
DRIVER={Apache Ignite 3};ADDRESS=node1:10800,node2:10800,node3:10800

# Complete configuration
DRIVER={Apache Ignite 3};ADDRESS=node1:10800,node2:10800;SCHEMA=mySchema;IDENTITY=admin;SECRET=password;PAGE_SIZE=2048;SSL_MODE=require;SSL_CERT_FILE=/opt/certs/client.pem;SSL_KEY_FILE=/opt/certs/client-key.pem;SSL_CA_FILE=/opt/certs/ca.pem
```

## Reference

### ODBC Compliance

- ODBC 3.8 specification compliant
- Core functions implemented (Level 1)
- Supports SQLConnect, SQLDriverConnect, SQLExecDirect, SQLPrepare, SQLExecute
- Supports SQLBindCol, SQLBindParameter, SQLFetch
- Supports SQLGetInfo, SQLGetDiagRec for diagnostics
- Implements metadata functions: SQLTables, SQLColumns, SQLPrimaryKeys

### Supported ODBC Functions

**Connection Management**:
- SQLAllocHandle, SQLFreeHandle
- SQLConnect, SQLDriverConnect, SQLDisconnect

**Statement Management**:
- SQLAllocStmt, SQLFreeStmt
- SQLPrepare, SQLExecute, SQLExecDirect
- SQLCloseCursor

**Result Handling**:
- SQLBindCol, SQLFetch
- SQLRowCount
- SQLDescribeCol, SQLNumResultCols

**Parameter Binding**:
- SQLBindParameter
- SQLNumParams

**Transaction Control**:
- SQLEndTran (commit/rollback)
- SQLSetConnectAttr (auto-commit mode)

**Metadata**:
- SQLTables, SQLColumns
- SQLPrimaryKeys, SQLForeignKeys
- SQLGetInfo, SQLGetTypeInfo

### Limitations

- Scrollable cursors not supported (forward-only only)
- Positioned updates/deletes not supported (read-only results)
- Asynchronous execution not supported
- Bookmarks not supported
- Multiple active statements require multiple statement handles
