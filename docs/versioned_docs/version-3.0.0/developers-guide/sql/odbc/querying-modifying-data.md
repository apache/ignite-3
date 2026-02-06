---
title: Querying and Modifying Data
sidebar_label: Querying and Modifying Data
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

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

This page describes how to connect to a cluster and execute a variety of SQL queries by using the ODBC driver.

The ODBC driver supports DML (Data Modification Layer), which means that you can modify your data using an ODBC connection.

## Creating Tables

The simplest way to create tables by using ODBC Driver is to use DDL statements:

<Tabs>
<TabItem value="ddl" label="DDL">

```cpp
SQLHENV env;

// Allocate an environment handle
SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

// Use ODBC ver 3.8
SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3_80), 0);

SQLHDBC dbc;

// Allocate a connection handle
SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

// Prepare the connection string
SQLCHAR connectStr[] = "Driver={Apache Ignite 3};ADDRESS=localhost:10800;SCHEMA=PUBLIC;";

// Connecting to the Cluster.
SQLDriverConnect(dbc, NULL, connectStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);

SQLHSTMT stmt;

// Allocate a statement handle
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

SQLCHAR query1[] = "CREATE TABLE Person ( "
    "id LONG PRIMARY KEY, "
    "firstName VARCHAR, "
    "lastName VARCHAR, "
    "salary FLOAT) "";

SQLExecDirect(stmt, query1, SQL_NTS);

SQLCHAR query2[] = "CREATE TABLE Organization ( "
    "id LONG PRIMARY KEY, "
    "name VARCHAR) "";

SQLExecDirect(stmt, query2, SQL_NTS);

SQLCHAR query3[] = "CREATE INDEX idx_organization_name ON Organization (name)";

SQLExecDirect(stmt, query3, SQL_NTS);
```

</TabItem>
</Tabs>

As you can see, we defined two tables that will contain the data of `Person` and `Organization` types.
For both types, we listed specific fields and indexes that will be read or updated using SQL.

## Handling Errors

The section below covers how you can handle possible errors when working with ODBC. In this example we handle an issue with connecting to the cluster

```cpp
// Connecting to Ignite Cluster.
SQLRETURN ret = SQLDriverConnect(dbc, NULL, connectStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);

if (!SQL_SUCCEEDED(ret))
{
  SQLCHAR sqlstate[7] = { 0 };
  SQLINTEGER nativeCode;

  SQLCHAR errMsg[BUFFER_SIZE] = { 0 };
  SQLSMALLINT errMsgLen = static_cast<SQLSMALLINT>(sizeof(errMsg));

  SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlstate, &nativeCode, errMsg, errMsgLen, &errMsgLen);

  std::cerr << "Failed to connect to Ignite: "
            << reinterpret_cast<char*>(sqlstate) << ": "
            << reinterpret_cast<char*>(errMsg) << ", "
            << "Native error code: " << nativeCode
            << std::endl;

  // Releasing allocated handles.
  SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  SQLFreeHandle(SQL_HANDLE_ENV, env);

  return;
}
```

## Querying Data

After everything is up and running, we're ready to execute `SQL SELECT` queries using the `ODBC API`.

```cpp
SQLHSTMT stmt;

// Allocate a statement handle
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

SQLCHAR query[] = "SELECT firstName, lastName, salary, Organization.name FROM Person "
  "INNER JOIN Organization ON Person.orgId = Organization.id"
SQLSMALLINT queryLen = static_cast<SQLSMALLINT>(sizeof(queryLen));

SQLRETURN ret = SQLExecDirect(stmt, query, queryLen);

if (!SQL_SUCCEEDED(ret))
{
  SQLCHAR sqlstate[7] = { 0 };
  SQLINTEGER nativeCode;

  SQLCHAR errMsg[BUFFER_SIZE] = { 0 };
  SQLSMALLINT errMsgLen = static_cast<SQLSMALLINT>(sizeof(errMsg));

  SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlstate, &nativeCode, errMsg, errMsgLen, &errMsgLen);

  std::cerr << "Failed to perform SQL query: "
            << reinterpret_cast<char*>(sqlstate) << ": "
            << reinterpret_cast<char*>(errMsg) << ", "
            << "Native error code: " << nativeCode
            << std::endl;
}
else
{
  // Printing the result set.
  struct OdbcStringBuffer
  {
    SQLCHAR buffer[BUFFER_SIZE];
    SQLLEN resLen;
  };

  // Getting a number of columns in the result set.
  SQLSMALLINT columnsCnt = 0;
  SQLNumResultCols(stmt, &columnsCnt);

  // Allocating buffers for columns.
  std::vector<OdbcStringBuffer> columns(columnsCnt);

  // Binding columns. For simplicity we are going to use only
  // string buffers here.
  for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    SQLBindCol(stmt, i + 1, SQL_C_CHAR, columns[i].buffer, BUFFER_SIZE, &columns[i].resLen);

  // Fetching and printing data in a loop.
  ret = SQLFetch(stmt);
  while (SQL_SUCCEEDED(ret))
  {
    for (size_t i = 0; i < columns.size(); ++i)
      std::cout << std::setw(16) << std::left << columns[i].buffer << " ";

    std::cout << std::endl;

    ret = SQLFetch(stmt);
  }
}

// Releasing statement handle.
SQLFreeHandle(SQL_HANDLE_STMT, stmt);
```


:::note
### Columns binding

In the example above, we bind all columns to the SQL_C_CHAR columns. This means that all values are going to be converted to strings upon fetching. This is done for the sake of simplicity. Value conversion upon fetching can be pretty slow; so your default decision should be to fetch the value the same way as it is stored.
:::

## Inserting Data

To insert new data into the cluster, `SQL INSERT` statements can be used from the ODBC side.


```cpp
SQLHSTMT stmt;

// Allocate a statement handle
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

SQLCHAR query[] =
	"INSERT INTO Person (id, orgId, firstName, lastName, resume, salary) "
	"VALUES (?, ?, ?, ?, ?, ?)";

SQLPrepare(stmt, query, static_cast<SQLSMALLINT>(sizeof(query)));

// Binding columns.
int64_t key = 0;
int64_t orgId = 0;
char name[1024] = { 0 };
SQLLEN nameLen = SQL_NTS;
double salary = 0.0;

SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);
SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &orgId, 0, 0);
SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,	sizeof(name), sizeof(name), name, 0, &nameLen);
SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &salary, 0, 0);

// Filling cache.
key = 1;
orgId = 1;
strncpy(name, "John", sizeof(name));
salary = 2200.0;

SQLExecute(stmt);
SQLMoreResults(stmt);

++key;
orgId = 1;
strncpy(name, "Jane", sizeof(name));
salary = 1300.0;

SQLExecute(stmt);
SQLMoreResults(stmt);

++key;
orgId = 2;
strncpy(name, "Richard", sizeof(name));
salary = 900.0;

SQLExecute(stmt);
SQLMoreResults(stmt);

++key;
orgId = 2;
strncpy(name, "Mary", sizeof(name));
salary = 2400.0;

SQLExecute(stmt);

// Releasing statement handle.
SQLFreeHandle(SQL_HANDLE_STMT, stmt);
```


Next, we are going to insert additional organizations without the usage of prepared statements.


```cpp
SQLHSTMT stmt;

// Allocate a statement handle
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

SQLCHAR query1[] = "INSERT INTO Organization (id, name) VALUES (1L, 'Some company')";

SQLExecDirect(stmt, query1, static_cast<SQLSMALLINT>(sizeof(query1)));

SQLFreeStmt(stmt, SQL_CLOSE);

SQLCHAR query2[] = "INSERT INTO Organization (id, name) VALUES (2L, 'Some other company')";

  SQLExecDirect(stmt, query2, static_cast<SQLSMALLINT>(sizeof(query2)));

// Releasing statement handle.
SQLFreeHandle(SQL_HANDLE_STMT, stmt);
```


:::warning
### Error Checking

For simplicity the example code above does not check for an error return code. You will want to do error checking in production.
:::

## Updating Data

Let's now update the salary for some of the persons stored in the cluster using SQL `UPDATE` statement.


```cpp
void AdjustSalary(SQLHDBC dbc, int64_t key, double salary)
{
  SQLHSTMT stmt;

  // Allocate a statement handle
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  SQLCHAR query[] = "UPDATE Person SET salary=? WHERE id=?";

  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT,
      SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &salary, 0, 0);

  SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG,
      SQL_BIGINT, 0, 0, &key, 0, 0);

  SQLExecDirect(stmt, query, static_cast<SQLSMALLINT>(sizeof(query)));

  // Releasing statement handle.
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

...
AdjustSalary(dbc, 3, 1200.0);
AdjustSalary(dbc, 1, 2500.0);
```

## Deleting Data

Finally, let's remove a few records with the help of SQL `DELETE` statement.

```cpp
void DeletePerson(SQLHDBC dbc, int64_t key)
{
  SQLHSTMT stmt;

  // Allocate a statement handle
  SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

  SQLCHAR query[] = "DELETE FROM Person WHERE id=?";

  SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT,
      0, 0, &key, 0, 0);

  SQLExecDirect(stmt, query, static_cast<SQLSMALLINT>(sizeof(query)));

  // Releasing statement handle.
  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

...
DeletePerson(dbc, 1);
DeletePerson(dbc, 4);
```
