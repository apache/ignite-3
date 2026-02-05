---
title: ODBC Driver
sidebar_label: ODBC Driver
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

## Overview

Ignite 3 includes an ODBC driver that allows you both to select and to modify data stored in a distributed cache by using standard SQL queries and native ODBC API. ODBC driver uses your [client connection configuration](../../clients/index.md).

ODBC driver only provides thread-safety at the connections level. This means that you should not access the same connection from multiple threads without additional synchronization, though you can create separate connections for every thread and use them simultaneously.

The ODBC driver implements version 3.8 of the ODBC API. For detailed information on ODBC please refer to [ODBC Programmer's Reference](https://msdn.microsoft.com/en-us/library/ms714177.aspx).

## Installing ODBC Driver

To use ODBC driver, register it in your system so that your ODBC Driver Manager will be able to locate it.

### Installing on Windows


#### Prerequisites

Microsoft Visual C++ 2017 Redistributable Package should be installed first.

#### Installation process

Launch the provided installer and follow the instructions.

### Configuring the Cluster

ODBC driver uses the client connector to work with the cluster. Make sure to configure the port to the one you intend to use, for example:

```shell
node config update clientConnector.port=10469
```

For more information on configuring client connector, see [Client Connector Configuration](../../clients/index.md#client-connector-configuration).

### Installing on Linux

To build and install ODBC driver on Linux, you need to first install
ODBC Driver Manager. The ODBC driver has been tested with [UnixODBC](http://www.unixodbc.org).

#### Prerequisites

Install the following prerequisites first:

- [libstdc](https://gcc.gnu.org/onlinedocs/libstdc%2B%2B) library supporting C++14 standard;
- [UnixODBC](http://www.unixodbc.org) driver manager.

#### Download from website

You can get the built rpm or deb package from the provided website. Then, install the package locally to use it.

## Supported Data Types

The following SQL data types are supported:

- `SQL_CHAR`
- `SQL_VARCHAR`
- `SQL_LONGVARCHAR`
- `SQL_SMALLINT`
- `SQL_INTEGER`
- `SQL_FLOAT`
- `SQL_DOUBLE`
- `SQL_BIT`
- `SQL_TINYINT`
- `SQL_BIGINT`
- `SQL_BINARY`
- `SQL_VARBINARY`
- `SQL_LONGVARBINARY`
- `SQL_GUID`
- `SQL_DECIMAL`
- `SQL_TYPE_DATE`
- `SQL_TYPE_TIMESTAMP`
- `SQL_TYPE_TIME`

## Using pyodbc

Ignite can be used with [pyodbc](https://pypi.org/project/pyodbc/). Here is how you can use pyodbc in Ignite 3:

- Install pyodbc

```shell
pip3 install pyodbc
```

- Import pyodbc to your project:

```python
import pyodbc
```

- Connect to the database:

```python
conn = pyodbc.connect('Driver={Apache Ignite 3};Address=127.0.0.1:10800;')
```

- Set encoding to UTF-8:

```python
conn.setencoding(encoding='utf-8')
conn.setdecoding(sqltype=pyodbc.SQL_CHAR, encoding="utf-8")
conn.setdecoding(sqltype=pyodbc.SQL_WCHAR, encoding="utf-8")
```

- Get data from your database:

```python
cursor = conn.cursor()
cursor.execute('SELECT * FROM table_name')
```

For more information on using pyodbc, use the [official documentation](https://github.com/mkleehammer/pyodbc/wiki).
