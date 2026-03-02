---
id: cpp-client
title: C++ Client
sidebar_position: 3
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Ignite 3 clients connect to the cluster via a standard socket connection. Unlike Ignite 2.x, there is no separate Thin and Thick clients in Ignite 3. All clients are 'thin'.

Clients do not become a part of the cluster topology, never hold any data, and are not used as a destination for compute calculations.

## Getting Started

### Prerequisites

To run C++ client, you need a C++ build environment to run the `cmake` command:

- C++ compiler supporting C++ 17
- CMake 3.10+
- One of build systems: make, ninja, MS Visual Studio, or other

### Installation {#build-ref}

The source code of the C++ client comes with the Ignite 3 distribution. To build it, use the following commands:

<Tabs groupId="os">
<TabItem value="windows" label="Windows">

```bat
mkdir cmake-build-release
cd cmake-build-release
cmake ..
cmake --build . -j8
```

</TabItem>
<TabItem value="linux" label="Linux">

```bash
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
```

</TabItem>
<TabItem value="macos" label="MacOS">

```bash
mkdir cmake-build-release
cd cmake-build-release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
```

</TabItem>
</Tabs>

### Building C++ Client on CentOS 7 and RHEL 7

If you are running on older systems, you need to set up the environment in the following way:

1. Install `epel-release` and `centos-release-scl`:

```bash
yum install epel-release centos-release-scl
```

2. Update yum and accept `epel-release` keys:

```bash
yum update
```

3. Install the build tools from the main repository and `devtoolset-11`:

```bash
yum install devtoolset-11-gcc devtoolset-11-gcc-c++ cmake3 git java-11-openjdk-devel gtest-devel gmock-devel
```

4. Create and update alternatives for `cmake` to force the use of `cmake3`:
   - Create an alternative for `cmake2` with priority 10:

```bash
sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake 10 \
--slave /usr/local/bin/ctest ctest /usr/bin/ctest \
--slave /usr/local/bin/cpack cpack /usr/bin/cpack \
--slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake \
--family cmake
```

   - Create an alternative for `cmake3` with priority 20:

```bash
sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 20 \
--slave /usr/local/bin/ctest ctest /usr/bin/ctest3 \
--slave /usr/local/bin/cpack cpack /usr/bin/cpack3 \
--slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3 \
--family cmake
```

   - Check that the default alternative points to `cmake3`:

```bash
sudo alternatives --config cmake
```

5. Enable the `devtoolset-11` compiler and start bash with the updated PATH:

```bash
scl enable devtoolset-11 bash
```

6. Start the [build](#build-ref) in the shell you have established.

## Client Connector Configuration

Client connection parameters are controlled by the client connector configuration. By default, Ignite accepts client connections on port 10800. You can change the configuration for the node by using the [CLI tool](/3.1.0/tools/cli-commands) at any time.

Here is how the client connector configuration looks like in the JSON format.

:::note
In Ignite 3, you can create and maintain the configuration in either JSON or HOCON format.
:::

```json
"ignite" : {
  "clientConnector" : {
    "port" : 10800,
    "idleTimeoutMillis" :3000,
    "sendServerExceptionStackTraceToClient" : true,
    "ssl" : {
      "enabled" : true,
      "clientAuth" : "require",
      "keyStore" : {
        "path" : "KEYSTORE_PATH",
        "password" : "SSL_STORE_PASS"
      },
      "trustStore" : {
        "path" : "TRUSTSTORE_PATH",
        "password" : "SSL_STORE_PASS"
      },
    },
  },
}
```

The table below covers the configuration for client connector:

| Property | Default | Description |
|----------|---------|-------------|
| connectTimeoutMillis | 5000 | Connection attempt timeout, in milliseconds. |
| idleTimeoutMillis | 0 | How long the client can be idle before the connection is dropped, in milliseconds. By default, there is no limit. |
| metricsEnabled | `false` | Defines if client metrics are collected. |
| port | 10800 | The port the client connector will be listening to. |
| sendServerExceptionStackTraceToClient | `false` | Defines if cluster exceptions are sent to the client. |
| ssl.ciphers | | The cipher used for SSL communication. |
| ssl.clientAuth | | Type of client authentication used by clients. For more information, see [SSL/TLS](/3.1.0/configure-and-operate/configuration/config-ssl-tls). |
| ssl.enabled | | Defines if SSL is enabled. |
| ssl.keyStore.password | | SSL keystore password. |
| ssl.keyStore.path | | Path to the SSL keystore. |
| ssl.keyStore.type | `PKCS12` | The type of SSL keystore used. |
| ssl.trustStore.password | | SSL keystore password. |
| ssl.trustStore.path | | Path to the SSL keystore. |
| ssl.trustStore.type | `PKCS12` | The type of SSL keystore used. |

Here is how you can change the parameters:

```
node config update clientConnector.port=10469
```

## Connecting to Cluster

To initialize a client, use the `IgniteClient` class, and provide it with the configuration:

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
using namespace ignite;

ignite_client_configuration cfg{"127.0.0.1"};
auto client = ignite_client::start(cfg, std::chrono::seconds(5));
```

</TabItem>
</Tabs>

## Authentication

To pass authentication information, pass it to `IgniteClient` builder:

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
auto authenticator = std::make_shared<ignite::basic_authenticator>("myUser", "myPassword");

ignite::ignite_client_configuration cfg{"127.0.0.1:10800"};
cfg.set_authenticator(authenticator);
auto client = ignite_client::start(std::move(cfg), std::chrono::seconds(30));
```

</TabItem>
</Tabs>

## User Object Serialization

Ignite supports mapping user objects to table tuples. This ensures that objects created in any programming language can be used for key-value operations directly.

### Limitations

There are limitations to user types that can be used for such a mapping. Some limitations are common, and others are platform-specific due to the programming language used.

- Only flat field structure is supported, meaning no nesting user objects. This is because Ignite tables, and therefore tuples have flat structure themselves.
- Fields should be mapped to Ignite types.
- All fields in user type should either be mapped to Table column or explicitly excluded.
- All columns from Table should be mapped to some field in the user type.
- *C++ only*: User has to provide marshalling functions explicitly as there is no reflection to generate them based on user type structure.

### Usage Examples

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
struct account {
  account() = default;
  account(std::int64_t id) : id(id) {}
  account(std::int64_t id, std::int64_t balance) : id(id), balance(balance) {}

  std::int64_t id{0};
  std::int64_t balance{0};
};

namespace ignite {

  template<>
  ignite_tuple convert_to_tuple(account &&value) {
    ignite_tuple tuple;

    tuple.set("id", value.id);
    tuple.set("balance", value.balance);

    return tuple;
  }

  template<>
  account convert_from_tuple(ignite_tuple&& value) {
    account res;

    res.id = value.get<std::int64_t>("id");

    // Sometimes only key columns are returned, i.e. "id",
    // so we have to check whether there are any other columns.
    if (value.column_count() > 1)
      res.balance = value.get<std::int64_t>("balance");

    return res;
  }

} // namespace ignite
```

</TabItem>
</Tabs>

## SQL API

Ignite 3 is focused on SQL, and SQL API is the primary way to work with the data. You can read more about supported SQL statements in the [SQL Reference](/3.1.0/sql/reference/language-definition/ddl) section. Here is how you can send SQL requests:

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
result_set result = client.get_sql().execute(nullptr, {"select name from tbl where id = ?"}, {std::int64_t{42}});
std::vector<ignite_tuple> page = result_set.current_page();
ignite_tuple& row = page.front();
```

</TabItem>
</Tabs>

### SQL Scripts

The default API executes SQL statements one at a time. If you want to execute large SQL statements, pass them to the `executeScript()` method. These statements will be executed in order.

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
std::string script = ""
	+ "CREATE TABLE IF NOT EXISTS Person (id int primary key, city_id int, name varchar, age int, company varchar);"
	+ "INSERT INTO Person (1,3, 'John', 43, 'Sample')";

client.get_sql().execute_script(script);
```

</TabItem>
</Tabs>

:::note
Execution of each statement is considered complete when the first page is ready to be returned. As a result, when working with large data sets, SELECT statement may be affected by later statements in the same script.
:::

## Transactions

All table operations in Ignite 3 are transactional. You can provide an explicit transaction as a first argument of any Table and SQL API call. If you do not provide an explicit transaction, an implicit one will be created for every call.

Here is how you can provide a transaction explicitly:

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
auto accounts = table.get_key_value_view<account, account>();

account init_value(42, 16'000);
accounts.put(nullptr, {42}, init_value);

auto tx = client.get_transactions().begin();

std::optional<account> res_account = accounts.get(&tx, {42});
res_account->balance += 500;
accounts.put(&tx, {42}, res_account);

assert(accounts.get(&tx, {42})->balance == 16'500);

tx.rollback();

assert(accounts.get(&tx, {42})->balance == 16'000);
```

</TabItem>
</Tabs>

## Table API

To execute table operations on a specific table, you need to get a specific view of the table and use one of its methods. You can only create new tables by using SQL API.

When working with tables, you can use built-in Tuple type, which is a set of key-value pairs underneath, or map the data to your own types for a strongly-typed access. Here is how you can work with tables:

### Getting a Table Instance

First, get an instance of the table. To obtain an instance of table, use the `IgniteTables.table(String)` method. You can also use `IgniteTables.tables()` method to list all existing tables.

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
using namespace ignite;

auto table_api = client.get_tables();
std::vector<table> existing_tables = table_api.get_tables();
table first_table = existing_tables.front();

std::optional<table> my_table = table_api.get_table("MY_TABLE");
```

</TabItem>
</Tabs>

### Basic Table Operations

Once you've got a table you need to get a specific view to choose how you want to operate table records.

#### Binary Record View

A binary record view. It can be used to operate table tuples directly.

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
record_view<ignite_tuple> view = table.get_record_binary_view();

ignite_tuple record{
  {"id", 42},
  {"name", "John Doe"}
};

view.upsert(nullptr, record);
std::optional<ignite_tuple> res_record = view.get(nullptr, {"id", 42});

assert(res_record.has_value());
assert(res_record->column_count() == 2);
assert(res_record->get<std::int64_t>("id") == 42);
assert(res_record->get<std::string>("name") == "John Doe");
```

</TabItem>
</Tabs>

#### Record View

A record view mapped to a user type. It can be used to operate table using user objects which are mapped to table tuples.

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
record_view<person> view = table.get_record_view<person>();

person record(42, "John Doe");

view.upsert(nullptr, record);
std::optional<person> res_record = view.get(nullptr, person{42});

assert(res.has_value());
assert(res->id == 42);
assert(res->name == "John Doe");
```

</TabItem>
</Tabs>

#### Key-Value Binary View

A binary key-value view. It can be used to operate table using key and value tuples separately.

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
key_value_view<ignite_tuple, ignite_tuple> kv_view = table.get_key_value_binary_view();

ignite_tuple key_tuple{{"id", 42}};
ignite_tuple val_tuple{{"name", "John Doe"}};

kv_view.put(nullptr, key_tuple, val_tuple);
std::optional<ignite_tuple> res_tuple = kv_view.get(nullptr, key_tuple);

assert(res_tuple.has_value());
assert(res_tuple->column_count() == 2);
assert(res_tuple->get<std::int64_t>("id") == 42);
assert(res_tuple->get<std::string>("name") == "John Doe");
```

</TabItem>
</Tabs>

#### Key-Value View

A key-value view with user objects. It can be used to operate table using key and value user objects mapped to table tuples.

<Tabs groupId="languages">
<TabItem value="cpp" label="C++">

```cpp
key_value_view<person, person> kv_view = table.get_key_value_view<person, person>();

kv_view.put(nullptr, {42}, {"John Doe"});
std::optional<person> res = kv_view.get(nullptr, {42});

assert(res.has_value());
assert(res->id == 42);
assert(res->name == "John Doe");
```

</TabItem>
</Tabs>
