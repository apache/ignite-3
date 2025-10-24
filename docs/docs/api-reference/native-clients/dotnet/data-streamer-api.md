---
title: Data Streamer API
id: data-streamer-api
sidebar_position: 3
---

# Data Streamer API

The Data Streamer API provides high-throughput bulk data loading into Ignite tables. It automatically batches data into pages, distributes them across cluster nodes, and optionally processes them server-side through custom receivers.

## Key Concepts

Data streaming optimizes bulk loading by grouping records into pages and sending them to the cluster in batches. This reduces network round-trips and enables parallel processing across partitions.

### Streaming Targets

Both IRecordView and IKeyValueView implement IDataStreamerTarget, allowing you to stream data directly to any table view. The streamer automatically routes data to the correct partition nodes.

### Server-Side Receivers

Receivers execute custom logic on the server for each page of streamed data. Use receivers to transform data, perform aggregations, or implement custom merge logic during loading. Receivers run colocated with the data for maximum performance.

### Page-Based Processing

The streamer divides input data into pages based on DataStreamerOptions.PageSize. Each page is sent to the appropriate cluster node where a receiver processes all items in the page together. This batching reduces overhead and enables efficient bulk operations.

## Usage Examples

### Basic Streaming

```csharp
var table = await client.Tables.GetTableAsync("events");
var view = table.GetRecordView<Event>();

// Generate data asynchronously
async IAsyncEnumerable<Event> GenerateEvents()
{
    for (int i = 0; i < 100000; i++)
    {
        yield return new Event
        {
            Id = i,
            Timestamp = DateTime.UtcNow,
            Type = "sensor_reading",
            Value = Random.Shared.NextDouble() * 100
        };
    }
}

// Stream the data
await view.StreamDataAsync(GenerateEvents());
```

### Streaming with Options

```csharp
var options = new DataStreamerOptions
{
    PageSize = 1000,  // Items per page
    RetryLimit = 16,  // Retry failed pages
    AutoFlushInterval = TimeSpan.FromSeconds(1)
};

await view.StreamDataAsync(GenerateEvents(), options);
```

### Streaming to Key-Value View

```csharp
var table = await client.Tables.GetTableAsync("metrics");
var kvView = table.GetKeyValueView<MetricKey, MetricValue>();

async IAsyncEnumerable<KeyValuePair<MetricKey, MetricValue>> GenerateMetrics()
{
    for (int i = 0; i < 50000; i++)
    {
        var key = new MetricKey { MetricId = i };
        var value = new MetricValue
        {
            Name = $"metric_{i}",
            Value = Random.Shared.NextDouble()
        };
        yield return new KeyValuePair<MetricKey, MetricValue>(key, value);
    }
}

await kvView.StreamDataAsync(GenerateMetrics());
```

### Custom Server-Side Receiver

```csharp
// Define receiver that processes data on the server
public class AggregatingReceiver : IDataStreamerReceiver<SensorReading, string, int>
{
    public IMarshaller<SensorReading>? PayloadMarshaller => null;
    public IMarshaller<string>? ArgumentMarshaller => null;
    public IMarshaller<int>? ResultMarshaller => null;

    public async ValueTask<IList<int>?> ReceiveAsync(
        IList<SensorReading> page,
        string arg,
        IDataStreamerReceiverContext context,
        CancellationToken cancellationToken)
    {
        // Process page on the server
        var sum = 0;
        var table = await context.Ignite.Tables.GetTableAsync("sensor_data");
        var view = table.GetRecordView<SensorReading>();

        foreach (var reading in page)
        {
            // Custom merge logic
            var existing = await view.GetAsync(null, new SensorReading { SensorId = reading.SensorId });
            if (existing.HasValue)
            {
                reading.Value += existing.Value.Value;
            }
            await view.UpsertAsync(null, reading);
            sum += (int)reading.Value;
        }

        return new[] { sum };
    }
}

// Register and use receiver
var receiverDescriptor = new ReceiverDescriptor<SensorReading, string, int>(
    "AggregatingReceiver");

var results = view.StreamDataAsync(
    data: GenerateReadings(),
    receiver: receiverDescriptor,
    keySelector: r => new SensorReading { SensorId = r.SensorId },
    payloadSelector: r => r,
    receiverArg: "aggregate_mode",
    options: new DataStreamerOptions { PageSize = 100 });

await foreach (var sum in results)
{
    Console.WriteLine($"Page sum: {sum}");
}
```

### Error Handling

```csharp
var options = new DataStreamerOptions
{
    PageSize = 500,
    RetryLimit = 16
};

try
{
    await view.StreamDataAsync(GenerateEvents(), options);
    Console.WriteLine("Streaming completed successfully");
}
catch (Exception ex)
{
    Console.WriteLine($"Streaming failed: {ex.Message}");
    // Handle failure (page that failed after retries)
}
```

### Streaming with Cancellation

```csharp
using var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromMinutes(5));

try
{
    await view.StreamDataAsync(
        GenerateEvents(),
        options: new DataStreamerOptions { PageSize = 1000 },
        cancellationToken: cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Streaming cancelled");
}
```

### Streaming with Transformations

```csharp
async IAsyncEnumerable<Event> GenerateAndTransform()
{
    await foreach (var rawEvent in LoadFromExternalSource())
    {
        // Transform during generation
        yield return new Event
        {
            Id = rawEvent.Id,
            Timestamp = DateTime.UtcNow,
            Type = NormalizeType(rawEvent.Type),
            Value = rawEvent.Value * 1.5
        };
    }
}

await view.StreamDataAsync(GenerateAndTransform());
```

## Reference

### IDataStreamerTarget&lt;T&gt; Interface

Basic streaming methods:

- **StreamDataAsync(IAsyncEnumerable&lt;T&gt; data, DataStreamerOptions?, CancellationToken)** - Stream raw data items
- **StreamDataAsync(IAsyncEnumerable&lt;DataStreamerItem&lt;T&gt;&gt; data, DataStreamerOptions?, CancellationToken)** - Stream with operation types

Receiver-based streaming:

- **StreamDataAsync&lt;TSource, TPayload, TArg, TResult&gt;** - Stream with receiver that returns results per page
- **StreamDataAsync&lt;TSource, TPayload, TArg&gt;** - Stream with receiver (void results)

Parameters:
- **data** - Async sequence of items to stream
- **receiver** - Server-side receiver descriptor
- **keySelector** - Function to extract key from source items
- **payloadSelector** - Function to extract payload from source items
- **receiverArg** - Argument passed to receiver
- **options** - Streaming configuration
- **cancellationToken** - Cancellation support

### DataStreamerOptions Class

Configuration properties:

- **PageSize** - Number of items per page (default: 1000)
- **RetryLimit** - Maximum retry attempts for failed pages (default: 16)
- **AutoFlushInterval** - Time interval for automatic page flushing

The page size controls the granularity of batching. Larger pages reduce network overhead but increase memory usage and reduce parallelism across partitions.

### IDataStreamerReceiver&lt;TItem, TArg, TResult&gt; Interface

Properties:

- **PayloadMarshaller** - Optional custom marshaller for payload items
- **ArgumentMarshaller** - Optional custom marshaller for arguments
- **ResultMarshaller** - Optional custom marshaller for results

Methods:

- **ReceiveAsync(IList&lt;TItem&gt; page, TArg arg, IDataStreamerReceiverContext context, CancellationToken)** - Process a page of items on the server

The receiver executes on the server node that owns the partition. It receives a page of items (controlled by PageSize), processes them with access to the full Ignite API through the context, and optionally returns results.

### IDataStreamerReceiverContext Interface

Properties:

- **Ignite** - Access to full Ignite client API for server-side operations

Use the context to access tables, execute SQL, or perform other operations during data processing. The context operates within the server environment.

### ReceiverDescriptor Class

Describes a server-side receiver:

- **ReceiverDescriptor&lt;TPayload, TArg, TResult&gt;(string className)** - Create descriptor with class name
- **ReceiverDescriptor&lt;TArg&gt;(string className)** - Create descriptor for receivers with no result

The receiver must be deployed to the server before streaming. The class name identifies the receiver implementation on the server.

### DataStreamerItem&lt;T&gt; Type

Wraps streamed items with operation type:

- **Data** - The data item
- **OperationType** - Operation to perform (Put, Remove)

Use this when you need to stream mixed operations (inserts, updates, deletes) in a single stream.
