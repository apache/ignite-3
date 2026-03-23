---
title: Python Database API Driver
sidebar_label: Python
---

{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}

Apache Ignite 3 clients connect to the cluster via a standard socket connection. Clients do not become a part of the cluster topology, never hold any data, and are not used as a destination for compute calculations.

Apache Ignite DB API driver uses the [Python Database API](https://peps.python.org/pep-0249/).


## Getting Started

### Prerequisites

To run the Python driver, the following is required:

- CMake 3.18 or newer to build the driver
- Python 3.9 or newer (3.9, 3.10, 3.11 and 3.12 are tested)
- Access to a running Ignite 3 node

### Limitations
Script execution of SQL statements is not supported in current release.



### Installation

To install Python DB API driver, download it from pip.

```
pip install pyignite3_dbapi
```

After this, you can import `pyignite_dbapi` into your project and use it.

## Connecting to Cluster

To connect to the cluster, use the `connect()` method:

```python
addr = ['127.0.0.1:10800']
return pyignite_dbapi.connect(address=addr, timeout=10)
```

After you are done working with the cluster, remember to always close the connection to it.

```python
conn.close()
```

Alternatively, you can use the `with` statement to automatically close the connection when no longer necessary:

```python
with pyignite_dbapi.connect(address=addr, timeout=10) as conn:
    conn.cursor()
```

### Configuring SSL for Connection

To ensure secure connection to the cluster, you can enable SSL for it by providing the key file and certificate, for example:

```python
def create_ssl_connection():
  """Create SSL-enabled connection to Ignite cluster."""
  addr = ['127.0.0.1:10800']
  return pyignite_dbapi.connect(
      address=addr,
      timeout=10,
      use_ssl=True,
      ssl_keyfile='<path_to_ssl_keyfile.pem>',
      ssl_certfile='<path_to_ssl_certfile.pem>',
      # Optional: ssl_ca_certfile='<path_to_ssl_ca_certfile.pem>'
  )
```

:::note
All paths to certificate file and keys should be provided in string format appropriate for the system.
:::

### Configuring Authorization

If the cluster uses [basic authorization](../../administrators-guide/security/authentication.md#basic-authentication), you need to provide user `identity` and `secret` to authorize on it, for example:

```python
def create_authenticated_connection():
  """Create authenticated connection to Ignite cluster."""
  addr = ['127.0.0.1:10800']
  return pyignite_dbapi.connect(
      address=addr,
      timeout=10,
      identity='user',
      secret='password'
  )
```

### Configuring Data Access

You can configure optional properties to fine-tune how data is accessed.


| Configuration name | Description |
|--------------------|-------------|
| schema | A schema name to be used by default. Default value: 'PUBLIC'. |
| page_size | Maximum number of rows that can be received or sent in a single request. Default value: 1024 |

The example below shows how to set these properties:

```python
def create_configured_connection():
  """Create authenticated connection to Ignite cluster."""
  addr = ['127.0.0.1:10800']
  return conn = pyignite_dbapi.connect(
    address=addr,
    timeout=10,
    schema='CUSTOM',
    page_size=2048
  )
```

## Getting Cursor Object

To work with tables from Python client, you use the `cursor` object that can be retrieved from the connection object:

```python
conn.cursor()
```

Similar to the connection, you can use the `with` statement when getting the cursor:

```python
with conn.cursor() as cursor:
```

## Executing Single Query

The cursor object can be used to execute SQL statements with the `execute` command:

```python
# Create table
cursor.execute('''
          CREATE TABLE Person(
              id INT PRIMARY KEY,
              name VARCHAR,
              age INT
          )
      ''')
```

## Executing a Batched Query

You can use the `executemany` command to execute SQL queries with a batch of parameters. This kind of operation offers much higher performance than executing individual queries. The example below inserts two rows into the Person table:

```python
# Sample data
sample_data = [
  [1, "John", 30],
  [2, "Jane", 32],
  [3, "Bob", 28]
]

# Insert data (fixed table name)
cursor.executemany('INSERT INTO Person VALUES(?, ?, ?)', sample_data)
```


## Getting Query Results

The cursor retains a reference to the operation. If the operation returns results (for example, a `SELECT`), they will also be stored in the cursor. You can then use the `fetchone()` method to retrieve query results from the cursor:

```python
# Query data
cursor.execute('SELECT * FROM Person ORDER BY id')
results = cursor.fetchall()

print("All persons in database:")
for row in results:
  print(f"ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")
```

## Working with Transactions

By default, transactions required for database operations are handled implicitly. However, you can disable automatic transaction handling and manually handle commits.

To do this, first, disable autocommit:

```python
conn.autocommit = False
```

Once autocommit is disabled, you need to commit your operations manually:

```python
# Insert valid records
cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [4, "Alice", 29])
cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [5, "Charlie", 31])

cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [6, "Invalid", new_age])

conn.commit()
print("Transaction committed successfully")
```

Operations that are not committed are sent to the cluster, but not yet written to the table. The table is only updated when the `commit` method is called. You can roll back all uncommitted operations with the `rollback` command:

```python
with conn.cursor() as cursor:
  try:
    # Insert valid records
    cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [4, "Alice", 29])
    cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [5, "Charlie", 31])

    cursor.execute('INSERT INTO Person VALUES(?, ?, ?)', [6, "Invalid", new_age])

    conn.commit()
    print("Transaction committed successfully")

  except Exception as e:
    # Rollback on any error
    conn.rollback()
    print(f"Transaction rolled back due to error: {e}")
```

:::note
The `rollback` command rolls back all uncommitted data.
:::
