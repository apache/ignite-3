---
title: Compute API
id: compute-api
sidebar_position: 6
---

# Compute API

The Compute API executes distributed jobs across cluster nodes. Jobs run colocated with data for maximum performance, accessing the full Ignite API through an execution context.

## Key Concepts

Compute jobs are C# classes deployed to the cluster that implement IComputeJob. Submit jobs for execution using the Compute API, which routes them to appropriate nodes based on the specified target.

### Job Targets

Job targets control where jobs execute. Target by specific node, node name, or colocated partition to run jobs near data. The client routes job submissions to the appropriate cluster node based on the target.

### Broadcast Execution

Broadcast jobs run on multiple nodes simultaneously. Use this pattern for operations that need to execute across the entire cluster or a subset of nodes.

### Execution Tracking

Job submission returns an execution handle for monitoring progress and retrieving results. Query job state, change priority, or wait for completion through the execution interface.

## Usage Examples

### Basic Job Execution

```csharp
// Define job class (must be deployed to cluster)
public class HelloJob : IComputeJob<string, string>
{
    public IMarshaller<string>? InputMarshaller => null;
    public IMarshaller<string>? ResultMarshaller => null;

    public async ValueTask<string> ExecuteAsync(
        IJobExecutionContext context,
        string arg,
        CancellationToken cancellationToken)
    {
        return $"Hello, {arg}!";
    }
}

// Submit job
var compute = client.Compute;
var jobDescriptor = new JobDescriptor<string, string>(typeof(HelloJob));

var nodes = await client.GetClusterNodesAsync();
var target = JobTarget.Node(nodes[0]);

var execution = await compute.SubmitAsync(target, jobDescriptor, "World");
var result = await execution.GetResultAsync();

Console.WriteLine(result);  // "Hello, World!"
```

### Job with Data Access

```csharp
public class DataProcessingJob : IComputeJob<long, decimal>
{
    public IMarshaller<long>? InputMarshaller => null;
    public IMarshaller<decimal>? ResultMarshaller => null;

    public async ValueTask<decimal> ExecuteAsync(
        IJobExecutionContext context,
        long customerId,
        CancellationToken cancellationToken)
    {
        // Access Ignite APIs through context
        var tables = context.Ignite.Tables;
        var ordersTable = await tables.GetTableAsync("orders");
        var view = ordersTable.GetRecordView<Order>();

        // Execute SQL through context
        var sql = context.Ignite.Sql;
        var statement = new SqlStatement(
            "SELECT SUM(amount) FROM orders WHERE customer_id = ?");
        var resultSet = await sql.ExecuteAsync<SumResult>(
            null, statement, customerId);

        var sum = await resultSet.FirstOrDefaultAsync();
        return sum?.Total ?? 0m;
    }
}

// Submit job colocated with data
var compute = client.Compute;
var jobDescriptor = new JobDescriptor<long, decimal>(typeof(DataProcessingJob));

var target = JobTarget.Colocated("orders", 12345L);
var execution = await compute.SubmitAsync(target, jobDescriptor, 12345L);
var totalAmount = await execution.GetResultAsync();

Console.WriteLine($"Total orders: ${totalAmount}");
```

### Broadcast Execution

```csharp
public class DiagnosticsJob : IComputeJob<string, NodeInfo>
{
    public IMarshaller<string>? InputMarshaller => null;
    public IMarshaller<NodeInfo>? ResultMarshaller => null;

    public async ValueTask<NodeInfo> ExecuteAsync(
        IJobExecutionContext context,
        string arg,
        CancellationToken cancellationToken)
    {
        // Gather node information
        return new NodeInfo
        {
            NodeName = Environment.MachineName,
            Timestamp = DateTime.UtcNow,
            MemoryUsed = GC.GetTotalMemory(false)
        };
    }
}

// Broadcast to all nodes
var compute = client.Compute;
var jobDescriptor = new JobDescriptor<string, NodeInfo>(typeof(DiagnosticsJob));

var nodes = await client.GetClusterNodesAsync();
var target = BroadcastTarget.Nodes(nodes);

var execution = await compute.SubmitBroadcastAsync(
    target, jobDescriptor, "diagnostics");

// Get results from all nodes
foreach (var jobExecution in execution.JobExecutions)
{
    var nodeInfo = await jobExecution.GetResultAsync();
    Console.WriteLine($"{nodeInfo.NodeName}: {nodeInfo.MemoryUsed} bytes");
}
```

### Job with Custom Marshallers

```csharp
public class ComplexDataJob : IComputeJob<CustomInput, CustomOutput>
{
    // Provide custom serialization
    public IMarshaller<CustomInput>? InputMarshaller => new CustomInputMarshaller();
    public IMarshaller<CustomOutput>? ResultMarshaller => new CustomOutputMarshaller();

    public async ValueTask<CustomOutput> ExecuteAsync(
        IJobExecutionContext context,
        CustomInput input,
        CancellationToken cancellationToken)
    {
        // Process complex input
        return new CustomOutput
        {
            ProcessedData = input.RawData.Select(x => x * 2).ToList()
        };
    }
}
```

### Monitoring Job Execution

```csharp
var compute = client.Compute;
var jobDescriptor = new JobDescriptor<string, string>(typeof(LongRunningJob));

var target = JobTarget.Node(nodes[0]);
var execution = await compute.SubmitAsync(target, jobDescriptor, "input");

// Monitor job state
while (true)
{
    var state = await execution.GetStateAsync();
    if (state == null)
    {
        Console.WriteLine("Job information expired");
        break;
    }

    Console.WriteLine($"Job state: {state.Status}");

    if (state.Status == JobStatus.Completed ||
        state.Status == JobStatus.Failed ||
        state.Status == JobStatus.Canceled)
    {
        break;
    }

    await Task.Delay(1000);
}

// Get final result
try
{
    var result = await execution.GetResultAsync();
    Console.WriteLine($"Result: {result}");
}
catch (Exception ex)
{
    Console.WriteLine($"Job failed: {ex.Message}");
}
```

### Changing Job Priority

```csharp
var execution = await compute.SubmitAsync(target, jobDescriptor, "input");

// Increase priority
var changed = await execution.ChangePriorityAsync(10);
if (changed == true)
{
    Console.WriteLine("Priority changed");
}
else if (changed == false)
{
    Console.WriteLine("Job already executing or completed");
}
else
{
    Console.WriteLine("Job not found (retention expired)");
}
```

### Colocated Execution

```csharp
// Execute job on node that owns the data
var compute = client.Compute;
var jobDescriptor = new JobDescriptor<long, ProcessingResult>(typeof(ColocatedProcessor));

// Target partition that contains the key
var target = JobTarget.Colocated("customers", customerId);
var execution = await compute.SubmitAsync(target, jobDescriptor, customerId);
var result = await execution.GetResultAsync();
```

Colocated execution minimizes network traffic by running jobs on the node that stores the data.

### Exception Handling

```csharp
try
{
    var execution = await compute.SubmitAsync(target, jobDescriptor, "input");
    var result = await execution.GetResultAsync();
}
catch (IgniteException ex)
{
    Console.WriteLine($"Job execution failed: {ex.Message}");
}
catch (TimeoutException ex)
{
    Console.WriteLine($"Job timed out: {ex.Message}");
}
```

### Cancellation Support

```csharp
using var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromSeconds(30));

try
{
    var execution = await compute.SubmitAsync(
        target, jobDescriptor, "input", cts.Token);
    var result = await execution.GetResultAsync();
}
catch (OperationCanceledException)
{
    Console.WriteLine("Job submission cancelled");
}
```

## Reference

### ICompute Interface

Job submission methods:

- **SubmitAsync&lt;TTarget, TArg, TResult&gt;(IJobTarget&lt;TTarget&gt; target, JobDescriptor&lt;TArg, TResult&gt; jobDescriptor, TArg arg, CancellationToken cancellationToken)** - Submit job to target with cancellation
- **SubmitAsync&lt;TTarget, TArg, TResult&gt;(IJobTarget&lt;TTarget&gt; target, JobDescriptor&lt;TArg, TResult&gt; jobDescriptor, TArg arg)** - Submit job to target

Broadcast methods:

- **SubmitBroadcastAsync&lt;TTarget, TArg, TResult&gt;(IBroadcastJobTarget&lt;TTarget&gt; target, JobDescriptor&lt;TArg, TResult&gt; jobDescriptor, TArg arg, CancellationToken cancellationToken)** - Broadcast job with cancellation
- **SubmitBroadcastAsync&lt;TTarget, TArg, TResult&gt;(IBroadcastJobTarget&lt;TTarget&gt; target, JobDescriptor&lt;TArg, TResult&gt; jobDescriptor, TArg arg)** - Broadcast job

Map-reduce methods:

- **SubmitMapReduceAsync&lt;TArg, TResult&gt;(TaskDescriptor&lt;TArg, TResult&gt; taskDescriptor, TArg arg, CancellationToken cancellationToken)** - Submit map-reduce task with cancellation
- **SubmitMapReduceAsync&lt;TArg, TResult&gt;(TaskDescriptor&lt;TArg, TResult&gt; taskDescriptor, TArg arg)** - Submit map-reduce task

### IComputeJob&lt;TArg, TResult&gt; Interface

Properties:

- **InputMarshaller** - Optional custom marshaller for job arguments
- **ResultMarshaller** - Optional custom marshaller for job results

Methods:

- **ExecuteAsync(IJobExecutionContext context, TArg arg, CancellationToken cancellationToken)** - Execute the job on the server

Jobs must be deployed to cluster nodes before submission. The job implementation has full access to the Ignite API through the execution context.

### IJobExecutionContext Interface

Properties:

- **Ignite** - Full Ignite API access for the server environment

Use the context to access tables, execute SQL, start transactions, or perform other operations from within the job. All operations execute in the server context on the cluster node.

### IJobExecution&lt;T&gt; Interface

Properties:

- **Id** - Unique job identifier (Guid)
- **Node** - Cluster node where job is executing

Methods:

- **GetResultAsync()** - Wait for and retrieve job result
- **GetStateAsync()** - Get current job state (returns null if retention expired)
- **ChangePriorityAsync(int priority)** - Change job priority (returns true if changed, false if executing/completed, null if not found)

The execution handle tracks a submitted job. Use it to monitor progress, adjust priority, or wait for completion.

### IJobTarget&lt;T&gt; Interface

Properties:

- **Data** - Target data (node, partition, etc.)

Static factory methods (on JobTarget class):

- **JobTarget.Node(IClusterNode node)** - Target specific node
- **JobTarget.AnyNode(IEnumerable&lt;IClusterNode&gt; nodes)** - Target any node from collection
- **JobTarget.AnyNode(params IClusterNode[] nodes)** - Target any node from array
- **JobTarget.Colocated(string tableName, TKey key)** - Target partition containing key
- **JobTarget.Colocated(QualifiedName tableName, TKey key)** - Target partition containing key with qualified table name

### JobDescriptor&lt;TArg, TResult&gt; Class

Constructors:

- **JobDescriptor(Type type)** - Create descriptor from job type implementing IComputeJob&lt;TArg, TResult&gt;
- **JobDescriptor(string jobClassName)** - Create descriptor with Java job class name for server-side execution

The job type must implement IComputeJob&lt;TArg, TResult&gt;. Use the Type constructor for .NET jobs, or the string constructor for Java jobs on the server.

### JobState Record

Job state information:

- **Id** - Job identifier (Guid)
- **Status** - Current job status (JobStatus enum)
- **CreateTime** - Job creation timestamp
- **StartTime** - Job start timestamp (null when not yet started)
- **FinishTime** - Job completion timestamp (null when not yet finished)

State information may expire based on cluster configuration. GetStateAsync returns null when state information is no longer available.

### JobStatus Enum

Possible job status values:

- **Queued** - Job is submitted and waiting for execution start
- **Executing** - Job is currently running
- **Failed** - Job was unexpectedly terminated during execution
- **Completed** - Job executed successfully and returned result
- **Canceling** - Job received cancel command but is still running
- **Canceled** - Job was successfully cancelled

### Best Practices

**Deploy jobs before submission**. Jobs must exist on cluster nodes before the client can submit them.

**Use colocated execution** when jobs access specific data. This eliminates network transfers between compute and data nodes.

**Keep jobs focused**. Each job should perform a specific task. Use multiple jobs for complex workflows.

**Handle exceptions in jobs**. Unhandled exceptions fail the job and return errors to the client.

**Consider job serialization**. Job arguments and results cross network boundaries. Use efficient serialization or custom marshallers for large data.

**Monitor long-running jobs**. Use GetStateAsync to track progress and detect failures early.
