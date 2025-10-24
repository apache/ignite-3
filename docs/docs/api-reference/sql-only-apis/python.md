---
title: Python DB-API
id: python
sidebar_position: 3
---

# Python DB-API

The Apache Ignite 3 Python driver implements PEP 249 (Python Database API Specification 2.0) for Python applications. It provides SQL access through a standard Python interface backed by a native C++ extension for performance.

## Installation

```bash
pip install pyignite_dbapi
```

## Connection Function

```python
connect(address, **kwargs)
```

### Parameters

- `address` - List of node addresses in format `['host:port', 'host:port']`

### Keyword Arguments

**Connection Options**:
- `identity` - Username for authentication
- `secret` - Password for authentication
- `schema` - Default schema name (default: `'PUBLIC'`)
- `page_size` - Rows fetched per request (default: `1024`)
- `timeout` - Network timeout in seconds (default: `30`)
- `autocommit` - Enable autocommit mode (default: `True`)

**SSL Options**:
- `use_ssl` - Enable SSL connection (boolean)
- `ssl_keyfile` - Path to PEM-encoded private key file
- `ssl_certfile` - Path to PEM-encoded certificate file
- `ssl_ca_certfile` - Path to PEM-encoded CA certificate file

### Returns

`Connection` object.

## Connection Object

### Attributes

- `autocommit` - Get or set autocommit mode (property)

### Methods

- `close()` - Close connection and all associated cursors
- `commit()` - Commit pending transaction
- `rollback()` - Roll back pending transaction
- `cursor()` - Create and return new cursor object

### Context Manager Support

```python
with pyignite_dbapi.connect(address=['localhost:10800']) as conn:
    # Use connection
    pass
# Connection automatically closes
```

## Cursor Object

### Attributes

- `arraysize` - Number of rows returned by `fetchmany()` (default: `1`)
- `description` - Read-only sequence of 7-tuples describing result columns
- `rowcount` - Number of rows affected or returned (read-only)
- `rownumber` - Current row index (read-only, 0-based)
- `connection` - Parent connection object (read-only)
- `lastrowid` - Always `None` (not supported by Ignite)

### Methods

- `execute(query, params=None)` - Execute SQL query with optional parameters
- `executemany(query, params_list)` - Execute SQL query multiple times with parameter sequences
- `fetchone()` - Fetch next row as tuple or `None`
- `fetchmany(size=None)` - Fetch next `size` rows as list of tuples (uses `arraysize` if `size` is `None`)
- `fetchall()` - Fetch all remaining rows as list of tuples
- `close()` - Close cursor
- `next()` / `__next__()` - Fetch next row (iterator protocol)

### Context Manager Support

```python
with conn.cursor() as cursor:
    cursor.execute('SELECT * FROM users')
    # Use cursor
    pass
# Cursor automatically closes
```

## Column Description

The `cursor.description` attribute returns a sequence of 7-tuples for each result column:

1. `name` - Column name (string)
2. `type_code` - Python type constant
3. `display_size` - Display width (integer or `None`)
4. `internal_size` - Internal storage size (integer or `None`)
5. `precision` - Numeric precision (integer or `None`)
6. `scale` - Numeric scale (integer or `None`)
7. `null_ok` - Whether column allows NULL (boolean or `None`)

## Type Constants

The module exposes type constants for column descriptions:

- `NULL` - None type
- `BOOLEAN` - Boolean type (bool)
- `INT` - Integer type (int)
- `FLOAT` - Floating point type (float)
- `NUMBER` - Decimal type (decimal.Decimal)
- `DATE` - Date type (datetime.date)
- `TIME` - Time type (datetime.time)
- `DATETIME` - Datetime type (datetime.datetime)
- `DURATION` - Duration type (datetime.timedelta)
- `STRING` - String type (str)
- `BINARY` - Binary type (bytes)
- `UUID` - UUID type (uuid.UUID)
- `TIMESTAMP` - Timestamp type (float subclass)

## Parameter Style

The driver uses question mark (`?`) parameter style:

```python
cursor.execute('SELECT * FROM users WHERE id = ?', [101])
cursor.execute('INSERT INTO users (id, name) VALUES (?, ?)', [101, 'John'])
```

Parameters bind positionally to question marks in the query.

## Usage Examples

### Basic Connection

```python
import pyignite_dbapi

conn = pyignite_dbapi.connect(address=['127.0.0.1:10800'])

try:
    cursor = conn.cursor()
    cursor.execute('SELECT id, name FROM users')

    for row in cursor:
        print(f"{row[0]}: {row[1]}")

    cursor.close()
finally:
    conn.close()
```

### Connection with Context Manager

```python
import pyignite_dbapi

with pyignite_dbapi.connect(address=['127.0.0.1:10800']) as conn:
    with conn.cursor() as cursor:
        cursor.execute('SELECT id, name FROM users')

        for row in cursor:
            print(f"{row[0]}: {row[1]}")
```

### Connection with Authentication

```python
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    identity='admin',
    secret='password'
)
```

### Connection with SSL

```python
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    use_ssl=True,
    ssl_certfile='/path/to/client.pem',
    ssl_keyfile='/path/to/client-key.pem',
    ssl_ca_certfile='/path/to/ca.pem'
)
```

### Multiple Node Addresses

```python
conn = pyignite_dbapi.connect(
    address=['node1:10800', 'node2:10800', 'node3:10800']
)
```

The driver attempts connections to addresses in order until one succeeds.

### Parameterized Queries

```python
cursor.execute(
    'SELECT * FROM users WHERE age > ? AND city = ?',
    [25, 'New York']
)

rows = cursor.fetchall()
```

### Insert with Parameters

```python
cursor.execute(
    'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
    [101, 'John Doe', 'john@example.com']
)

print(f"Rows affected: {cursor.rowcount}")
```

### Batch Insert

```python
users = [
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com')
]

cursor.executemany(
    'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
    users
)

print(f"Rows affected: {cursor.rowcount}")
```

### Transaction Control

```python
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    autocommit=False
)

try:
    cursor = conn.cursor()
    cursor.execute('INSERT INTO accounts (id, balance) VALUES (?, ?)', [1, 1000])
    cursor.execute('INSERT INTO accounts (id, balance) VALUES (?, ?)', [2, 2000])

    conn.commit()
except Exception as e:
    conn.rollback()
    raise
finally:
    conn.close()
```

### Fetch Strategies

```python
cursor.execute('SELECT * FROM large_table')

# Fetch one row at a time
row = cursor.fetchone()
if row:
    print(row)

# Fetch specific number of rows
cursor.arraysize = 100
rows = cursor.fetchmany(100)  # Fetch 100 rows

# Fetch all remaining rows
all_rows = cursor.fetchall()
```

### Iterator Protocol

```python
cursor.execute('SELECT * FROM users')

for row in cursor:
    print(row)
```

### Column Metadata

```python
cursor.execute('SELECT id, name, created_at FROM users')

for col in cursor.description:
    print(f"Column: {col.name}")
    print(f"  Type: {col.type_code}")
    print(f"  Nullable: {col.null_ok}")
```

### Type Handling

```python
import datetime
import uuid
from decimal import Decimal

# Insert various types
cursor.execute('''
    INSERT INTO products (id, uuid, name, price, created, active)
    VALUES (?, ?, ?, ?, ?, ?)
''', [
    1,
    uuid.uuid4(),
    'Widget',
    Decimal('19.99'),
    datetime.datetime.now(),
    True
])

# Retrieve and use typed values
cursor.execute('SELECT uuid, price, created FROM products WHERE id = ?', [1])
row = cursor.fetchone()

product_uuid = row[0]  # uuid.UUID
product_price = row[1]  # Decimal
product_created = row[2]  # datetime.datetime
```

### Error Handling

```python
import pyignite_dbapi

try:
    conn = pyignite_dbapi.connect(address=['127.0.0.1:10800'])
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM nonexistent_table')
except pyignite_dbapi.DatabaseError as e:
    print(f"Database error: {e}")
except pyignite_dbapi.OperationalError as e:
    print(f"Connection error: {e}")
finally:
    if conn:
        conn.close()
```

## Exception Hierarchy

All exceptions inherit from `Error`:

```
Error (base exception)
├── Warning
├── InterfaceError
└── DatabaseError
    ├── DataError
    ├── OperationalError
    ├── IntegrityError
    ├── InternalError
    ├── ProgrammingError
    └── NotSupportedError
```

Access exceptions through the connection object or module:

```python
try:
    cursor.execute(query)
except conn.DatabaseError as e:
    # Handle error
    pass

# Or
except pyignite_dbapi.DatabaseError as e:
    # Handle error
    pass
```

## Ignite-Specific Behavior

### Type Mapping

Python types map to SQL types:

| Python Type | SQL Type |
|-------------|----------|
| None | NULL |
| bool | BOOLEAN |
| int | BIGINT |
| float | DOUBLE |
| Decimal | DECIMAL |
| str | VARCHAR |
| bytes | VARBINARY |
| date | DATE |
| time | TIME |
| datetime | TIMESTAMP |
| timedelta | INTERVAL |
| UUID | UUID |

### Autocommit Default

Autocommit defaults to `True`. Disable for explicit transaction control:

```python
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    autocommit=False
)
```

Or change after connection:

```python
conn.autocommit = False
```

### Page Size

The driver fetches results in pages (default: 1024 rows). Increase page size for large result sets to reduce network overhead:

```python
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    page_size=4096
)
```

### Network Timeout

Configure network timeout for socket operations:

```python
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    timeout=60  # 60 seconds
)
```

### Thread Safety

The module provides thread safety level 1 (module level). Each thread requires its own connection. Do not share connections across threads.

### lastrowid Limitation

The `cursor.lastrowid` attribute always returns `None`. Ignite does not track auto-generated keys through the DB-API interface.

## Connection Examples

```python
# Basic
conn = pyignite_dbapi.connect(address=['127.0.0.1:10800'])

# With schema
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    schema='analytics'
)

# With authentication
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    identity='admin',
    secret='password'
)

# With SSL
conn = pyignite_dbapi.connect(
    address=['127.0.0.1:10800'],
    use_ssl=True,
    ssl_certfile='/opt/certs/client.pem',
    ssl_keyfile='/opt/certs/client-key.pem',
    ssl_ca_certfile='/opt/certs/ca.pem'
)

# Complete configuration
conn = pyignite_dbapi.connect(
    address=['node1:10800', 'node2:10800', 'node3:10800'],
    identity='admin',
    secret='password',
    schema='mySchema',
    page_size=2048,
    timeout=60,
    autocommit=False,
    use_ssl=True,
    ssl_certfile='/opt/certs/client.pem',
    ssl_keyfile='/opt/certs/client-key.pem',
    ssl_ca_certfile='/opt/certs/ca.pem'
)
```

## Reference

### Module Attributes

- `apilevel` - `'2.0'` (PEP 249 API level)
- `threadsafety` - `1` (module-level thread safety)
- `paramstyle` - `'qmark'` (question mark parameter style)

### DB-API Compliance

- PEP 249 Database API Specification 2.0 compliant
- Implements Connection, Cursor objects
- Supports context managers (with statements)
- Implements iterator protocol for cursors
- Provides standard exception hierarchy

### Limitations

- Thread safety level 1 (connections not thread-safe)
- `lastrowid` not supported (always returns `None`)
- Binary data must use `bytes` type (no special Binary constructor)
- Date/time values use standard library types (no special constructors)
