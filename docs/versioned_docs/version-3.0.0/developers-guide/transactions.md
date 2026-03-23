---
title: Performing Transactions
sidebar_label: Performing Transactions
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

All queries in Apache Ignite are transactional. You can provide an explicit transaction as a first argument of any Table and SQL API call. If you do not provide an explicit transaction, an implicit one will be created for every call.

## Transaction Lifecycle

When the transaction is created, the node that the transaction was started from is chosen as **transaction coordinator**. The coordinator finds the required partitions and sends the read or write requests to the nodes holding primary partitions. For correct transaction operation, all nodes in cluster must have similar time, that can be different by no more than `schemaSync.maxClockSkewMillis`.

If the key is not locked by a different transaction, the node gets the locks on the involved keys, and attempts to apply the changes in the transaction. When the operation finishes, the lock is removed. This way, several transactions can work on the same partition, while changing separate keys. Additionally, some operations may perform **short-term** locks on the keys in advance, to ensure operations proceed correctly.

If the node with the primary replica of a partition involved in the transaction fails, the transaction is eventually automatically rolled back. Apache Ignite will return `TransactionException` on commit attempt.

## Transaction Isolation and Concurrency

All read-write transactions in Apache Ignite acquire locks during the first read or write access, and hold the lock until the transaction is committed or rolled back. All read-write transactions are `SERIALIZABLE`, so as long as the lock persists, no other transaction can make changes to locked data, however data can still be read by [Read-Only Transactions](#read-only-transactions).

### Deadlock Prevention

Apache Ignite uses the `WAIT_DIE` deadlock prevention algorithm. When a newer transaction requests data that is already locked by a different transaction, it is cancelled and the transaction operation is retried with the same timestamp. If the transaction is older, it is not cancelled and is allowed to wait for the lock to be freed.

## Executing Transactions

Here is how you  can provide a transaction explicitly:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
KeyValueView<Long, Account> accounts =
  table.keyValueView(Mapper.of(Long.class), Mapper.of(Account.class));

accounts.put(null, 42, new Account(16_000));

var tx = client.transactions().begin();

Account account = accounts.get(tx, 42);
account.balance += 500;
accounts.put(tx, 42, account);

assert accounts.get(tx, 42).balance == 16_500;

tx.rollback();

assert accounts.get(tx, 42).balance == 16_000;
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
var accounts = table.GetKeyValueView<long, Account>();
await accounts.PutAsync(transaction: null, 42, new Account(16_000));

await using ITransaction tx = await client.Transactions.BeginAsync();

(Account account, bool hasValue) = await accounts.GetAsync(tx, 42);
account = account with { Balance = account.Balance + 500 };

await accounts.PutAsync(tx, 42, account);

Debug.Assert((await accounts.GetAsync(tx, 42)).Value.Balance == 16_500);

await tx.RollbackAsync();

Debug.Assert((await accounts.GetAsync(null, 42)).Value.Balance == 16_000);

public record Account(decimal Balance);
```

</TabItem>
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

## Transaction Management

You can also manage transactions by using the `runInTransaction` method. When using it, the following will be done automatically:

- The transaction is started and substituted to the closure.
- The transaction is committed if no exceptions were thrown during the closure.
- The transaction will be retried in case of recoverable error. Closure must be purely functional, not causing side effects.

You can run transactions both synchronously and asynchronously.

This example shows how to update an account's balance synchronously:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
client.transactions().runInTransaction(tx -> {
    Account acct = accounts.get(tx, key);
    if (acct != null) {
        acct.balance += 200.0d;
    }
    accounts.put(tx, key, acct);
});

```

</TabItem>
</Tabs>

And this example performs the same logic in an asynchronous manner:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
CompletableFuture<Void> future = client.transactions().runInTransactionAsync(tx ->
        accounts.getAsync(tx, key)
                .thenCompose(acct -> {
                    acct.balance += 300.0d;
                    return accounts.putAsync(tx, key, acct);
                })
);
future.join();
```

</TabItem>
</Tabs>

## Read-Only Transactions

When starting a transaction, you can configure the transaction as a **read-only** transaction. In these transactions, no data modification can be performed, but they also do not secure locks and can be performed on non-primary partitions, further improving their performance. Read-only transactions always check the data for the moment they were started, even if new data was written to the database.

Here is how you can make a read-only transaction:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
var tx = client.transactions().begin(new TransactionOptions().readOnly(true));
int balance = accounts.get(tx, 42).balance;
tx.commit();
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
await using var tx = await client.Transactions.BeginAsync(
    new TransactionOptions { ReadOnly = true });
var account = await accounts.GetAsync(tx, 42);
int balance = account.Value.Balance;
await tx.CommitAsync();
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
auto tx_opts = transaction_options()
        .set_read_only(true);

auto tx = m_client.get_transactions().begin(tx_opts);

record_view.get(&tx, 42);

tx.commit();
```

</TabItem>
</Tabs>

:::note
Read-only transactions read data at a specific time. If new data was written since, old data will still be stored in Version Storage and will be available until low watermark. If low watermark is reached during the transaction, data will be kept available until it is over.
:::

## Transaction Timeout

In certain scenarios, it is preferable to drop the transaction if it is taking too long. When the timeout is reached, the transaction is automatically rolled back.

Here is how you can configure transaction timeout:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
KeyValueView<Long, Account> accounts =
  table.keyValueView(Mapper.of(Long.class), Mapper.of(Account.class));

var tx = client.transactions().begin(new TransactionOptions().timeoutMillis(10000));
accounts.put(tx, 42, account);
tx.commit();
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
await using var tx = await Client.Transactions.BeginAsync(
    new TransactionOptions { TimeoutMillis = 10_000 });
await accounts.PutAsync(tx, 42, account);
await tx.CommitAsync();
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
auto accounts = table.get_key_value_view<account, account>();

auto tx_opts = transaction_options()
       .set_timeout_millis(10000);

auto tx = m_client.get_transactions().begin(tx_opts);

record_view.insert(&tx, 42);

tx.commit();
```

</TabItem>
</Tabs>
