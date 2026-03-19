---
title: Streaming Data
sidebar_label: Streaming Data
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

Data streaming provides a fast, efficient method for loading, organizing, and distributing large volumes of data across your cluster.
Data streamer accepts a stream of data and distributes data entries across the cluster, where the processing takes place. Data streaming is available in all table views.

![Data streaming diagram](/img/data_streaming.png)

Data streaming provides at-least-once delivery guarantee.

## Using Data Streamer API

The [Data Streamer API](https://ignite.apache.org/releases/3.0.0/javadoc/org/apache/ignite/table/DataStreamerTarget.html) lets you load large amounts of data into your cluster quickly and reliably using a publisher–subscriber model, where you create a publisher that streams your data entries to a table view, and the system distributes these entries across the cluster. You can configure how the data is processed via a `DataStreamerOptions` object that allows to set batch sizes, auto-flush intervals, retry limits.

### Configuring Data Streamer

`DataStreamerOptions` lets you fine-tune how data is streamed into your cluster by setting parameters for batching, retries, parallelism, and auto-flush timing:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
DataStreamerOptions options = DataStreamerOptions.builder()
.pageSize(1000)
.perPartitionParallelOperations(1)
.autoFlushInterval(1000)
.retryLimit(16)
.build();
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
var options = new DataStreamerOptions
{
    PageSize = 1000,
    RetryLimit = 8,
    AutoFlushInterval = TimeSpan.FromSeconds(3)
};
```

</TabItem>
</Tabs>

- `pageSize`: Specifies the number of entries to process in each page or chunk of data.
- `perPartitionParallelOperations`: Determines the number of parallel operations allowed on each partition.
- `autoFlushInterval`: Defines the time interval (in milliseconds) after which the system automatically flushes any incomplete buffers.
- `retryLimit`: Specifies the maximum number of retry attempts for a failed data submission before giving up.

### Streaming Data

Before data is streamed to the cluster, each entry must be wrapped in an instance of the `DataStreamerItem<T>` class. This wrapper allows you to perform `PUT` and `REMOVE` operations with data:

- Use `DataStreamerItem.of(entry)` to insert new entries into the table.

- Use `DataStreamerItem.removed(entry)` to delete existing ones.

Wrapped data then can be passed to a publisher and streamed to the table.

The example below demonstrates how to use [`RecordView`](table-api.md#record-view), create a publisher, configure the data streamer, insert account records into the existing `accounts` table and then delete them:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
public class RecordViewPojoDataStreamerExample {
    private static final int ACCOUNTS_COUNT = 1000;

    public static void main(String[] args) throws Exception {
        /**
         * Assuming the 'accounts' table already exists.
         */
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {
            RecordView<Account> view = client.tables().table("accounts").recordView(Account.class);

            streamAccountDataPut(view);
            streamAccountDataRemove(view);
        }
    }

    /**
     * Streaming data using DataStreamerOperationType#PUT operation type.
     */
    private static void streamAccountDataPut(RecordView<Account> view) {
        DataStreamerOptions options = DataStreamerOptions.builder()
                .pageSize(1000)
                .perPartitionParallelOperations(1)
                .autoFlushInterval(1000)
                .retryLimit(16)
                .build();

        CompletableFuture<Void> streamerFut;
        try (var publisher = new SubmissionPublisher<DataStreamerItem<Account>>()) {
            streamerFut = view.streamData(publisher, options);
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                Account entry = new Account(i, "name" + i, rnd.nextLong(100_000), rnd.nextBoolean());
                publisher.submit(DataStreamerItem.of(entry));
            }
        }
        streamerFut.join();
    }

    /**
     * Streaming data using DataStreamerOperationType#REMOVE operation type
     */
    private static void streamAccountDataRemove(RecordView<Account> view) {
        DataStreamerOptions options = DataStreamerOptions.builder()
                .pageSize(1000)
                .perPartitionParallelOperations(1)
                .autoFlushInterval(1000)
                .retryLimit(16)
                .build();

        CompletableFuture<Void> streamerFut;
        try (var publisher = new SubmissionPublisher<DataStreamerItem<Account>>()) {
            streamerFut = view.streamData(publisher, options);
            for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                Account entry = new Account(i);
                publisher.submit(DataStreamerItem.removed(entry));
            }
        }
        streamerFut.join();
    }
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
using Apache.Ignite;
using Apache.Ignite.Table;

using var client = await IgniteClient.StartAsync(new("localhost"));
ITable? table = await client.Tables.GetTableAsync("accounts");
IRecordView<Account> view = table!.GetRecordView<Account>();

var options = new DataStreamerOptions
{
    PageSize = 10_000,
    AutoFlushInterval = TimeSpan.FromSeconds(1),
    RetryLimit = 32
};

await view.StreamDataAsync(GetAccountsToAdd(5_000), options);
await view.StreamDataAsync(GetAccountsToRemove(1_000), options);

async IAsyncEnumerable<DataStreamerItem<Account>> GetAccountsToAdd(int count)
{
    for (int i = 0; i < count; i++)
    {
        yield return DataStreamerItem.Create(
            new Account(i, $"Account {i}"));
    }
}

async IAsyncEnumerable<DataStreamerItem<Account>> GetAccountsToRemove(int count)
{
    for (int i = 0; i < count; i++)
    {
        yield return DataStreamerItem.Create(
            new Account(i, string.Empty), DataStreamerOperationType.Remove);
    }
}

public record Account(int Id, string Name);
```

</TabItem>
</Tabs>

### Streaming with Receiver

The Apache Ignite 3 streaming API supports advanced streaming scenarios by allowing you to create a custom receiver that defines server-side processing logic. Use a receiver when you need to process or transform data on the server, update multiple tables from a single data stream, or work with incoming data that does not match a table schema.

With a receiver, you can stream data in any format, as it is schema-agnostic.
The receiver also has access to the full Ignite 3 API through the [`DataStreamerReceiverContext`](https://ignite.apache.org/releases/3.0.0/javadoc/org/apache/ignite/table/DataStreamerReceiverContext.html).

The data streamer controls data flow by requesting items only when partition buffers have space. `DataStreamerOptions.perPartitionParallelOperations` controls how many buffers can be allocated per partition. When buffers are full, the streamer stops requesting more data until some items are processed.
Additionally, if a `resultSubscriber` is specified, it also applies backpressure on the streamer. If the subscriber is slow at consuming results, the streamer reduces its request rate from the publisher accordingly.

To use a receiver, you need to implement the [`DataStreamerReceiver`](https://ignite.apache.org/releases/3.0.0/javadoc/org/apache/ignite/table/DataStreamerReceiver.html) interface. The receiver's `receive` method processes each batch of items streamed to the server, so you can apply custom logic and return results for each item as needed:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
@Nullable CompletableFuture<List<R>> receive(
List<T> page,
DataStreamerReceiverContext ctx,
@Nullable A arg);

```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
ValueTask<IList<TResult>?> ReceiveAsync(
IList<TItem> page,
TArg arg,
IDataStreamerReceiverContext context,
CancellationToken cancellationToken);
```

</TabItem>
</Tabs>

- `page`: The current batch of data items to process.
- `ctx`: The receiver context, which lets you interact with Ignite 3 API.
- `arg`: An optional argument that can be used to pass custom parameters to your receiver logic.

### Examples

#### Updating Multiple Tables

The following example demonstrates how to implement a receiver that processes data containing customer and address information, and updates two separate tables on the server:

1. First, create the custom receiver that will extract data from the provided source and write it into two separate tables: `customers` and `addresses`.

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
private static class TwoTableReceiver implements DataStreamerReceiver<Tuple, Void, Void> {
@Override
public @Nullable CompletableFuture<List<Void>> receive(List<Tuple> page, DataStreamerReceiverContext ctx, @Nullable Void arg) {
// List<Tuple> is the source data. Those tuples do not conform to any table and can have arbitrary data.

            RecordView<Tuple> customersTable = ctx.ignite().tables().table("customers").recordView();
            RecordView<Tuple> addressesTable = ctx.ignite().tables().table("addresses").recordView();

            for (Tuple sourceItem : page) {
                // For each source item, receiver extracts customer and address data and upserts it into respective tables.
                Tuple customer = Tuple.create()
                        .set("id", sourceItem.intValue("customerId"))
                        .set("name", sourceItem.stringValue("customerName"))
                        .set("addressId", sourceItem.intValue("addressId"));

                Tuple address = Tuple.create()
                        .set("id", sourceItem.intValue("addressId"))
                        .set("street", sourceItem.stringValue("street"))
                        .set("city", sourceItem.stringValue("city"));

                customersTable.upsert(null, customer);
                addressesTable.upsert(null, address);
            }

            return null;
        }
    }
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
class TwoTableReceiver : IDataStreamerReceiver<IIgniteTuple, object?, object>
{
    public async ValueTask<IList<object>?> ReceiveAsync(
        IList<IIgniteTuple> page,
        object? arg,
        IDataStreamerReceiverContext context,
        CancellationToken cancellationToken)
    {
        IRecordView<IIgniteTuple> customerTable = (await context.Ignite.Tables.GetTableAsync("customers"))!.RecordBinaryView;
        IRecordView<IIgniteTuple> addressesTable = (await context.Ignite.Tables.GetTableAsync("addresses"))!.RecordBinaryView;

        foreach (IIgniteTuple sourceItem in page)
        {
            // For each source item, the receiver extracts customer and address data and upserts it into respective tables.
            var customer = new IgniteTuple
            {
                ["id"] = sourceItem["customerId"],
                ["name"] = sourceItem["customerName"],
                ["addressId"] = sourceItem["addressId"]
            };

            var address = new IgniteTuple
            {
                ["id"] = sourceItem["addressId"],
                ["street"] = sourceItem["street"],
                ["city"] = sourceItem["city"],
            };

            await customerTable.UpsertAsync(null, customer);
            await addressesTable.UpsertAsync(null, address);
        }

        return null;
    }
}
```

</TabItem>
</Tabs>

2. Create a descriptor that refers to your receiver implementation. This descriptor will be passed later to a `SubmissionPublisher` when streaming data.

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
DataStreamerReceiverDescriptor<Tuple, Void, Void> desc = DataStreamerReceiverDescriptor
.builder(TwoTableReceiver.class)
.build();
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
ReceiverDescriptor<IIgniteTuple, object?, object> desc = ReceiverDescriptor.Of(new TwoTableReceiver());
```

</TabItem>
</Tabs>

3. Next, obtain the target table to partition the data for streaming. In this example we partition by `customerId` to ensure the receiver is colocated with the customer data, enabling local upserts. Then define how to extract keys and payloads from the source, and stream the data using a `SubmissionPublisher`.

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
// Example source data
List<Tuple> sourceData = IntStream.range(1, 10)
.mapToObj(i -> Tuple.create()
.set("customerId", i)
.set("customerName", "Customer " + i)
.set("addressId", i)
.set("street", "Street " + i)
.set("city", "City " + i))
.collect(Collectors.toList());

CompletableFuture<Void> streamerFut;

RecordView<Tuple> customersTable = client.tables().table("customers").recordView();

// Extract the target table key from each source item; since the source has "customerId" but the target table uses "id", the function maps customerId to id accordingly.
Function<Tuple, Tuple> keyFunc = sourceItem -> Tuple.create().set("id", sourceItem.intValue("customerId"));

// Extract the data payload sent to the receiver. In this case, we use the entire source item as the payload.
Function<Tuple, Tuple> payloadFunc = Function.identity();

// Stream data using a publisher.
try (var publisher = new SubmissionPublisher<Tuple>()) {
    streamerFut = customersTable.streamData(
            publisher,
            desc,
            keyFunc,
            payloadFunc,
            null, // Optional receiver arguments
            null, // Result subscriber
            null // Options
    );

    for (Tuple item : sourceData) {
        publisher.submit(item);
    }
}

streamerFut.join();
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
IAsyncEnumerable<IIgniteTuple> sourceData = GetSourceData();

IRecordView<IIgniteTuple> customersTable = (await client.Tables.GetTableAsync("customers"))!.RecordBinaryView;

IAsyncEnumerable<object> streamerResults = customersTable.StreamDataAsync(
sourceData,
desc,
x => new IgniteTuple { ["id"] = x["customerId"] },
x => x,
null,
DataStreamerOptions.Default,
CancellationToken.None);

await foreach (object result in streamerResults)
{
// ...
}

static async IAsyncEnumerable<IIgniteTuple> GetSourceData()
{
await Task.Yield(); // Simulate async enumeration.

    for (int i = 0; i < 10; i++)
    {
        yield return new IgniteTuple
        {
            ["customerId"] = i,
            ["customerName"] = $"Customer {i}",
            ["addressId"] = i,
            ["street"] = $"Street {i}",
            ["city"] = $"City {i}"
        };
    }
}
```

</TabItem>
</Tabs>

#### Distributed Computations

You can also use a streamer with a receiver to perform distributed computations, such as per-item calculations and [map-reduce](compute/compute.md#mapreduce-tasks) tasks on the returned results.

This example demonstrates a simulated fraud detection process, which typically involves intensive processing of each transaction using ML models.

1. First, create a custom receiver that will handle fraud detection computations on the results:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
private static class FraudDetectorReceiver implements DataStreamerReceiver<Tuple, Void, Tuple> {
@Override
public @Nullable CompletableFuture<List<Tuple>> receive(List<Tuple> page, DataStreamerReceiverContext ctx, @Nullable Void arg) {
List<Tuple> results = new ArrayList<>(page.size());

            for (Tuple tx : page) {
                results.add(detectFraud(tx));
            }

            return CompletableFuture.completedFuture(results);
        }

        private static Tuple detectFraud(Tuple txInfo) {
            // Simulate fraud detection processing.
            double fraudRisk = Math.random();

            // Add result to the tuple and return.
            return txInfo.set("fraudRisk", fraudRisk);
        }
    }
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
class FraudDetectorReceiver : IDataStreamerReceiver<IIgniteTuple, object?, IIgniteTuple>
{
    public async ValueTask<IList<IIgniteTuple>?> ReceiveAsync(
        IList<IIgniteTuple> page,
        object? arg,
        IDataStreamerReceiverContext context,
        CancellationToken cancellationToken)
    {
        var result = new List<IIgniteTuple>(page.Count);

        foreach (var tx in page)
        {
            IIgniteTuple resTuple = await DetectFraud(tx);
            result.Add(resTuple);
        }

        return result;
    }

    private static async Task<IIgniteTuple> DetectFraud(IIgniteTuple transaction)
    {
        // Simulate fraud detection logic - add a random risk score to the tuple.
        await Task.Delay(10);
        transaction["fraudRisk"] = Random.Shared.NextDouble();
        return transaction;
    }
}
```

</TabItem>
</Tabs>

2. Next, stream a list of sample transactions across the cluster using a dummy table that partitions data by transaction ID and `FraudDetectorReceiver` for fraud detection. Subscribe to the results to log each processed transaction, handle errors, and confirm when streaming completes:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
public void runReceiverStreamProcessing() {

    // Source data is a list of financial transactions.
    // We distribute this processing across the cluster, then gather and return results.
    List<Tuple> sourceData = IntStream.range(1, 10)
                        .mapToObj(i -> Tuple.create()
                        .set("txId", i)
                        .set("txData", "{some-json-data}"))
                        .collect(Collectors.toList());

        DataStreamerReceiverDescriptor<Tuple, Void, Tuple> desc = DataStreamerReceiverDescriptor
                .builder(FraudDetectorReceiver.class)
                .build();

        CompletableFuture<Void> streamerFut;

        // Streaming requires a target table to partition data.
        // Use a dummy table for this scenario, because we are not going to store any data.
        TableDefinition txDummyTableDef = TableDefinition.builder("tx_dummy")
                .columns(column("id", ColumnType.INTEGER))
                .primaryKey("id")
                .build();

        Table dummyTable = client.catalog().createTable(txDummyTableDef);

        // Source data has "txId" field, but target dummy table has "id" column, so keyFunc maps "txId" to "id".
        Function<Tuple, Tuple> keyFunc = sourceItem -> Tuple.create().set("id", sourceItem.value("txId"));

        // Payload function is used to extract the payload (data that goes to the receiver) from the source item.
        // In our case, we want to use the whole source item as the payload.
        Function<Tuple, Tuple> payloadFunc = Function.identity();

        Flow.Subscriber<Tuple> resultSubscriber = new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Tuple item) {
                System.out.println("Transaction processed: " + item);
            }

            @Override
            public void onError(Throwable throwable) {
                System.err.println("Error during streaming: " + throwable.getMessage());
            }

            @Override
            public void onComplete() {
                System.out.println("Streaming completed.");
            }
        };

        try (var publisher = new SubmissionPublisher<Tuple>()) {
            streamerFut = dummyTable.recordView().streamData(
                    publisher,
                    desc,
                    keyFunc,
                    payloadFunc,
                    null, // Arg
                    resultSubscriber,
                    null // Options
            );

            for (Tuple item : sourceData) {
                publisher.submit(item);
            }
        }

        streamerFut.join();
    }
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
// Source data is a list of financial transactions.
// We want to distribute this processing across the cluster, then gather and return results
IAsyncEnumerable<IIgniteTuple> data = GetSourceData();

ReceiverDescriptor<IIgniteTuple, object?, IIgniteTuple> fraudDetectorReceiverDesc = ReceiverDescriptor.Of(new FraudDetectorReceiver());

// Streaming requires a target table to partition data.
// Use a dummy table for this scenario, because we are not going to store any data.
await client.Sql.ExecuteScriptAsync("CREATE TABLE IF NOT EXISTS TX_DUMMY (ID LONG)");

ITable dummyTable = await client.Tables.GetTableAsync("TX_DUMMY");

// Source data has "txId" field, but target dummy table has "id" column, so keyFunc maps "txId" to "id".
Func<IIgniteTuple, IIgniteTuple> keyFunc = tuple => new IgniteTuple { ["id"] = tuple["txId"] };

// Payload function is used to extract the payload (data that goes to the receiver) from the source item.
// In our case, we want to use the whole source item as the payload.
Func<IIgniteTuple, IIgniteTuple> payloadFunc = tuple => tuple;

IAsyncEnumerable<IIgniteTuple> results = dummyTable.RecordBinaryView.StreamDataAsync(
data,
fraudDetectorReceiverDesc,
keyFunc,
payloadFunc,
receiverArg: null);

await foreach (IIgniteTuple processedTx in results)
{
Console.WriteLine("Transaction processed: " + processedTx);
}

async IAsyncEnumerable<IIgniteTuple> GetSourceData()
{
await Task.Yield(); // Simulate async data source.

    for (int i = 0; i < 1000; i++)
    {
        yield return new IgniteTuple
        {
            ["txId"] = i,
            ["txData"] = "{some-json-data}"
        };
    }
}
```

</TabItem>
</Tabs>

#### Custom Marshallers in .NET

In .NET, you can define custom marshallers by implementing the [`IMarshaller`](https://ignite.apache.org/releases/3.0.0/dotnetdoc/api/Apache.Ignite.Marshalling.IMarshaller-1.html) interface.

For example, the code below demonstrates how to use `JsonMarshaller` to serialize data, arguments, and results.

```csharp
ITable? table = await client.Tables.GetTableAsync("my-table");

ReceiverDescriptor<MyData, MyArg, MyResult> receiverDesc = ReceiverDescriptor.Of(new MyReceiver());

IAsyncEnumerable<MyData> data = Enumerable
    .Range(1, 100)
    .Select(x => new MyData(x, $"Name {x}"))
    .ToAsyncEnumerable();

IAsyncEnumerable<MyResult> results = table!.RecordBinaryView.StreamDataAsync(
    data: data,
    receiver: receiverDesc,
    keySelector: dataItem => new IgniteTuple { ["id"] = dataItem.Id },
    payloadSelector: dataItem => dataItem,
    receiverArg: new MyArg("Some info"));

await foreach (MyResult result in results)
{
    Console.WriteLine(result);
}

public record MyData(int Id, string Name);

public record MyArg(string Info);

public record MyResult(MyData Data, MyArg Arg);

public class MyReceiver : IDataStreamerReceiver<MyData, MyArg, MyResult>
{
    public IMarshaller<MyData> PayloadMarshaller =>
        new JsonMarshaller<MyData>();

    public IMarshaller<MyArg> ArgumentMarshaller =>
        new JsonMarshaller<MyArg>();

    public IMarshaller<MyResult> ResultMarshaller =>
        new JsonMarshaller<MyResult>();

    public ValueTask<IList<MyResult>?> ReceiveAsync(IList<MyData> page, MyArg arg, IDataStreamerReceiverContext context, CancellationToken cancellationToken)
    {
        IList<MyResult> results = page
            .Select(data => new MyResult(data, arg))
            .ToList();

        return ValueTask.FromResult(results)!;
    }
}
```


## Tracking Failed Entries

If the data streamer fails to process any entries, it collects the failed items in a `DataStreamerException`. You can catch this exception and access the failed entries using the `failedItems()` method, as shown in the example below.

You can catch both asynchronous errors during background streaming and immediate submission errors:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
RecordView<Account> view = client.tables().table("accounts").recordView(Account.class);

CompletableFuture<Void> streamerFut;

try (var publisher = new SubmissionPublisher<DataStreamerItem<Account>>()) {
streamerFut = view.streamData(publisher, options)
.exceptionally(e -> {
System.out.println("Failed items during background streaming: " +
((DataStreamerException)e.getCause()).failedItems());
return null;
});

    /** Trying to insert an account record. */
    Account entry = new Account(1, "Account name", rnd.nextLong(100_000), rnd.nextBoolean());
    publisher.submit(DataStreamerItem.of(entry));
} catch (DataStreamerException e) {
      /** Handle entries that failed during submission. */
      System.out.println("Failed items during submission: " + e.failedItems());
}

streamerFut.join();
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
ITable? table = await Client.Tables.GetTableAsync("my-table");
IRecordView<IIgniteTuple> view = table!.RecordBinaryView;
IList<IIgniteTuple> data = [new IgniteTuple { ["key"] = 1L, ["val"] = "v" }];

try
{
await view.StreamDataAsync(data.ToAsyncEnumerable());
}
catch (DataStreamerException e)
{
Console.WriteLine("Failed items: " + string.Join(",", e.FailedItems));
}
```

</TabItem>
</Tabs>

### Tuning Memory Usage

The data streamer may require a significant amount of memory to handle the requests in an orderly manner. Depending on your environment, you may want to increase or reduce the amount of memory reserved by the data streamer.

For every node in the cluster, the streamer reserves an amount of memory equal to `pageSize` (1000 entries by default) multiplied by `perPartitionParallelOperations` (1 by default) setting. For example, a 10-partition table with default parameters and average entry size of 1KB will reserve 10MB for operations.

You can change these options while creating a `DataStreamerOptions` object:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
RecordView<Tuple> view = client.tables().table("accounts").recordView();
var publisher = new SubmissionPublisher<Tuple>();

var options = DataStreamerOptions.builder()
        .pageSize(10_000)
        .perPartitionParallelOperations(10)
        .build();

streamerFut = view.streamData(publisher, options);
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
// .NET streamer does not have a perPartitionParallelOperations option yet.
var options = new DataStreamerOptions
{
PageSize = 10_000
};
```

</TabItem>
</Tabs>

Additionally, the data streamer periodically flushes incomplete buffers to ensure that messages are not delayed indefinitely. This is especially useful when a buffer fills slowly or never completely fills due to uneven data distribution.

This behavior is controlled by the `autoFlushInterval` property, which is set to 5000 ms by default. You can also configure the `retryLimit` parameter to define the maximum number of retry attempts for failed submissions, with a default value of 16.
