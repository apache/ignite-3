---
title: Data Streamer API
id: data-streamer-api
sidebar_position: 4
---

# Data Streamer API

The Data Streamer API provides high-throughput data ingestion into Ignite tables. Applications stream data using reactive publishers that batch records for efficient network transmission and processing. This approach achieves higher performance than individual put operations.

## Key Concepts

Data streaming uses the Java Flow API for backpressure-aware data delivery. Publishers produce items that contain operation types and payloads. The streamer batches items, sends them to appropriate nodes, and executes operations in parallel across partitions.

Both RecordView and KeyValueView implement DataStreamerTarget, enabling streaming to either view type. Configure streaming behavior through DataStreamerOptions to control batch sizes, parallelism, and retry behavior.

## Basic Streaming

Stream data using a Flow publisher:

```java
RecordView<Tuple> view = table.recordView();

// Create publisher
List<DataStreamerItem<Tuple>> items = Arrays.asList(
    DataStreamerItem.of(Tuple.create().set("id", 1).set("name", "Alice")),
    DataStreamerItem.of(Tuple.create().set("id", 2).set("name", "Bob")),
    DataStreamerItem.of(Tuple.create().set("id", 3).set("name", "Carol"))
);

SubmissionPublisher<DataStreamerItem<Tuple>> publisher =
    new SubmissionPublisher<>();

// Stream data
CompletableFuture<Void> future = view.streamData(
    publisher,
    DataStreamerOptions.DEFAULT
);

// Submit items
items.forEach(publisher::submit);
publisher.close();

future.join();
```

The operation completes when all items are processed.

## Stream Options

Configure streaming behavior with options:

```java
DataStreamerOptions options = DataStreamerOptions.builder()
    .pageSize(1000)
    .perPartitionParallelOperations(4)
    .autoFlushInterval(1000)
    .retryLimit(3)
    .build();

CompletableFuture<Void> future = view.streamData(publisher, options);
```

The pageSize parameter controls batch size. Higher values increase throughput but consume more memory. The perPartitionParallelOperations setting determines concurrent operations per partition.

## Operation Types

Specify operation types for each item:

```java
List<DataStreamerItem<Tuple>> items = Arrays.asList(
    DataStreamerItem.of(
        Tuple.create().set("id", 1).set("name", "Alice"),
        DataStreamerOperationType.PUT
    ),
    DataStreamerItem.of(
        Tuple.create().set("id", 2).set("status", "active"),
        DataStreamerOperationType.PUT
    ),
    DataStreamerItem.removed(
        Tuple.create().set("id", 3)
    ),
    DataStreamerItem.of(
        Tuple.create().set("id", 4).set("name", "David")
    )
);
```

Available operations:
- PUT: Insert or update records (default when using `of()` method)
- REMOVE: Remove records (use `removed()` method or explicit `DataStreamerOperationType.REMOVE`)

## Custom Publishers

Implement custom publishers for streaming from external sources:

```java
class FilePublisher implements Flow.Publisher<DataStreamerItem<Tuple>> {
    private final Path file;

    public FilePublisher(Path file) {
        this.file = file;
    }

    public void subscribe(Flow.Subscriber<? super DataStreamerItem<Tuple>> subscriber) {
        subscriber.onSubscribe(new Flow.Subscription() {
            private BufferedReader reader;

            public void request(long n) {
                try {
                    if (reader == null) {
                        reader = Files.newBufferedReader(file);
                    }

                    for (long i = 0; i < n; i++) {
                        String line = reader.readLine();
                        if (line == null) {
                            reader.close();
                            subscriber.onComplete();
                            return;
                        }

                        String[] parts = line.split(",");
                        Tuple tuple = Tuple.create()
                            .set("id", Integer.parseInt(parts[0]))
                            .set("name", parts[1]);

                        subscriber.onNext(DataStreamerItem.of(tuple));
                    }
                } catch (IOException e) {
                    subscriber.onError(e);
                }
            }

            public void cancel() {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                } catch (IOException e) {
                    // Ignore
                }
            }
        });
    }
}
```

Publishers must respect backpressure signals to avoid overwhelming the system.

## Receiver-Based Streaming

Execute custom processing logic on server nodes using receivers:

```java
class AggregationReceiver
    implements DataStreamerReceiver<Tuple, String, Integer> {

    @Override
    public CompletableFuture<List<Integer>> receive(
        List<Tuple> items,
        DataStreamerReceiverContext context,
        String arg
    ) {
        Table table = context.ignite().tables().table("aggregates");
        RecordView<Tuple> view = table.recordView();

        Map<String, Integer> counts = new HashMap<>();
        for (Tuple item : items) {
            String category = item.stringValue("category");
            counts.merge(category, 1, Integer::sum);
        }

        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            Tuple record = Tuple.create()
                .set("category", entry.getKey())
                .set("count", entry.getValue());
            view.put(null, record);
        }

        return CompletableFuture.completedFuture(
            Collections.singletonList(counts.size())
        );
    }
}
```

Register and use the receiver:

```java
DataStreamerReceiverDescriptor<Tuple, String, Integer> descriptor =
    DataStreamerReceiverDescriptor.<Tuple, String, Integer>builder(
        "com.example.AggregationReceiver"
    ).build();

SubmissionPublisher<Tuple> publisher = new SubmissionPublisher<>();

CompletableFuture<Void> future = view.streamData(
    publisher,
    descriptor,
    tuple -> tuple.value("id"),
    tuple -> tuple,
    "aggregation-arg",
    null,
    DataStreamerOptions.DEFAULT
);

// Submit items
List<Tuple> items = Arrays.asList(
    Tuple.create().set("id", 1).set("category", "A"),
    Tuple.create().set("id", 2).set("category", "B")
);
items.forEach(publisher::submit);
publisher.close();

future.join();
```

Receivers process batches on server nodes, enabling custom logic like aggregations or complex transformations.

## Error Handling

Handle streaming errors through the returned future:

```java
CompletableFuture<Void> future = view.streamData(publisher, options);

future.exceptionally(ex -> {
    if (ex instanceof DataStreamerException) {
        System.err.println("Streaming failed: " + ex.getMessage());
    }
    return null;
});
```

Configure retry behavior through DataStreamerOptions.retryLimit to automatically retry failed batches.

## Auto-Flush Interval

Configure periodic flushing for low-volume streams:

```java
DataStreamerOptions options = DataStreamerOptions.builder()
    .autoFlushInterval(500)
    .build();
```

The streamer flushes incomplete batches after the specified interval in milliseconds. This prevents data from remaining buffered indefinitely in low-throughput scenarios.

## Key-Value View Streaming

Stream to key-value views using Entry payloads:

```java
KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

List<DataStreamerItem<Map.Entry<Tuple, Tuple>>> items = Arrays.asList(
    DataStreamerItem.of(Map.entry(
        Tuple.create().set("id", 1),
        Tuple.create().set("name", "Alice")
    )),
    DataStreamerItem.of(Map.entry(
        Tuple.create().set("id", 2),
        Tuple.create().set("name", "Bob")
    ))
);

SubmissionPublisher<DataStreamerItem<Map.Entry<Tuple, Tuple>>> publisher =
    new SubmissionPublisher<>();

CompletableFuture<Void> future = kvView.streamData(publisher, DataStreamerOptions.DEFAULT);

items.forEach(publisher::submit);
publisher.close();

future.join();
```

## Performance Considerations

Optimize streaming throughput by tuning configuration:

```java
DataStreamerOptions options = DataStreamerOptions.builder()
    .pageSize(10000)
    .perPartitionParallelOperations(8)
    .retryLimit(5)
    .build();
```

Larger page sizes reduce per-batch overhead but increase memory usage. Higher parallelism improves throughput on multi-core systems but may cause resource contention.

## Reference

- Streaming interface: `org.apache.ignite.table.DataStreamerTarget<T>`
- Configuration: `org.apache.ignite.table.DataStreamerOptions`
- Stream items: `org.apache.ignite.table.DataStreamerItem<T>`
- Custom processing: `org.apache.ignite.table.DataStreamerReceiver<T, A, R>`
- Receiver context: `org.apache.ignite.table.DataStreamerReceiverContext`
- Receiver descriptor: `org.apache.ignite.table.DataStreamerReceiverDescriptor<T, A, R>`

### DataStreamerTarget Methods

- `CompletableFuture<Void> streamData(Publisher<DataStreamerItem<T>>, DataStreamerOptions)` - Stream data to table
- `<E, V, A, R> CompletableFuture<Void> streamData(Publisher<E>, DataStreamerReceiverDescriptor<V, A, R>, Function<E, T>, Function<E, V>, A, Subscriber<R>, DataStreamerOptions)` - Stream with custom receiver

### DataStreamerOptions Configuration

- `pageSize` - Number of items per batch (default: 1000)
- `perPartitionParallelOperations` - Concurrent operations per partition (default: 1)
- `autoFlushInterval` - Auto-flush interval in milliseconds (default: 5000)
- `retryLimit` - Number of retry attempts for failed batches (default: 16)

### DataStreamerItem Operations

- `PUT` - Insert or update record
- `REMOVE` - Remove record

### DataStreamerReceiver Interface

- `CompletableFuture<List<R>> receive(List<T>, DataStreamerReceiverContext, A)` - Process batch on server
- `Marshaller<T, byte[]> payloadMarshaller()` - Custom payload serialization
- `Marshaller<A, byte[]> argumentMarshaller()` - Custom argument serialization
- `Marshaller<R, byte[]> resultMarshaller()` - Custom result serialization
