---
title: Compute API
id: compute-api
sidebar_position: 7
---

# Compute API

The Compute API executes custom code on cluster nodes. Applications submit jobs that run on selected nodes and return results. This enables data-local processing, distributed algorithms, and workload distribution across the cluster.

## Key Concepts

Compute jobs implement the ComputeJob interface. Job descriptors identify which class to execute and where to find it. Job targets specify execution location using strategies like specific nodes, any available node, or colocated with table partitions.

Jobs execute asynchronously and return JobExecution handles. Use these handles to retrieve results, monitor status, cancel execution, or adjust priority. Broadcast jobs execute on multiple nodes and aggregate results.

## Job Implementation

Implement ComputeJob for custom processing:

```java
public class WordCountJob implements ComputeJob<String, Integer> {
    @Override
    public CompletableFuture<Integer> executeAsync(
        JobExecutionContext context,
        String text
    ) {
        int count = text.split("\\s+").length;
        return CompletableFuture.completedFuture(count);
    }
}
```

Jobs receive execution context and arguments. Return CompletableFuture for asynchronous processing.

## Job Submission

Submit jobs with descriptors and targets:

```java
JobDescriptor<String, Integer> descriptor =
    JobDescriptor.builder("com.example.WordCountJob").build();

CompletableFuture<JobExecution<Integer>> executionFuture =
    ignite.compute().submitAsync(
        JobTarget.anyNode(ignite.clusterNodes()),
        descriptor,
        "the quick brown fox"
    );

Integer result = executionFuture
    .thenCompose(JobExecution::resultAsync)
    .join();

System.out.println("Word count: " + result);
```

The submitAsync method returns immediately while the job executes on the target node.

## Job Targets

Target specific execution locations:

```java
// Execute on specific node
ClusterNode node = ignite.cluster().localNode();
JobTarget target = JobTarget.node(node);

// Execute on any node from set
Collection<ClusterNode> nodes = ignite.clusterNodes();
JobTarget target = JobTarget.anyNode(nodes);

// Execute on all nodes (broadcast)
BroadcastJobTarget target = BroadcastJobTarget.nodes(nodes);
```

Choose targets based on workload characteristics and data locality requirements.

## Colocated Execution

Execute jobs colocated with data:

```java
JobDescriptor<Integer, String> descriptor =
    JobDescriptor.builder("com.example.DataProcessor").build();

Tuple key = Tuple.create().set("id", 100);
QualifiedName tableName = QualifiedName.of("products");

JobTarget target = JobTarget.colocated(tableName, key, Mapper.of(Tuple.class));

CompletableFuture<JobExecution<String>> execution =
    ignite.compute().submitAsync(target, descriptor, 100);
```

Colocated execution eliminates network overhead by running jobs on nodes that store the data.

## Job Context

Access cluster resources within jobs:

```java
public class DataProcessorJob implements ComputeJob<Integer, String> {
    @Override
    public CompletableFuture<String> executeAsync(
        JobExecutionContext context,
        Integer productId
    ) {
        Ignite ignite = context.ignite();
        Table table = ignite.tables().table("products");
        RecordView<Tuple> view = table.recordView();

        Tuple key = Tuple.create().set("id", productId);
        Tuple record = view.get(null, key);

        return CompletableFuture.completedFuture(
            record.stringValue("name")
        );
    }
}
```

JobExecutionContext provides access to the Ignite instance, partition information, deployment units, and cancellation status.

## Job Cancellation

Cancel running jobs using CancellationToken:

```java
CancelHandle cancelHandle = CancelHandle.create();

CompletableFuture<JobExecution<Integer>> executionFuture =
    ignite.compute().submitAsync(
        target,
        descriptor,
        input,
        cancelHandle.token()
    );

// Cancel the job
cancelHandle.cancel();
System.out.println("Cancellation requested");
```

Cancelled jobs stop execution and release resources. Check cancellation status within job implementation using context.isCancelled().

## Cancellation Tokens

Respond to cancellation within jobs:

```java
public class CancellableJob implements ComputeJob<String, Integer> {
    @Override
    public CompletableFuture<Integer> executeAsync(
        JobExecutionContext context,
        String input
    ) {
        return CompletableFuture.supplyAsync(() -> {
            int count = 0;
            for (String word : input.split("\\s+")) {
                if (context.isCancelled()) {
                    throw new CancellationException("Job cancelled");
                }
                count++;
            }
            return count;
        });
    }
}
```

Check cancellation status periodically during long-running operations.

## Job Priority

Adjust job priority dynamically:

```java
JobExecution<Integer> execution = executionFuture.join();

execution.changePriorityAsync(10).thenAccept(changed -> {
    if (changed) {
        System.out.println("Priority updated");
    }
});
```

Higher priority jobs execute before lower priority jobs in the queue.

## Job Status

Monitor job execution status:

```java
JobExecution<Integer> execution = executionFuture.join();

execution.stateAsync().thenAccept(state -> {
    System.out.println("Job state: " + state);
});

execution.idAsync().thenAccept(id -> {
    System.out.println("Job ID: " + id);
});
```

Job states include queued, executing, completed, cancelled, and failed.

## Broadcast Execution

Execute jobs on multiple nodes:

```java
JobDescriptor<String, Integer> descriptor =
    JobDescriptor.builder("com.example.MetricsCollector").build();

Collection<ClusterNode> nodes = ignite.clusterNodes();

CompletableFuture<BroadcastExecution<Integer>> broadcastFuture =
    ignite.compute().submitAsync(
        BroadcastJobTarget.nodes(nodes),
        descriptor,
        "collect"
    );

BroadcastExecution<Integer> broadcast = broadcastFuture.join();

// Get individual job executions by node
Map<ClusterNode, JobExecution<Integer>> executions = broadcast.executions();

for (Map.Entry<ClusterNode, JobExecution<Integer>> entry : executions.entrySet()) {
    Integer result = entry.getValue().resultAsync().join();
    System.out.println("Node " + entry.getKey().name() + ": " + result);
}
```

Broadcast execution returns results from all target nodes.

## Broadcast Results Collection

Access all broadcast results asynchronously:

```java
BroadcastExecution<Integer> broadcast = broadcastFuture.join();

CompletableFuture<List<Integer>> allResults = broadcast.resultsAsync();

List<Integer> values = allResults.join();

int total = values.stream().mapToInt(Integer::intValue).sum();
System.out.println("Total: " + total);
```

## Deployment Units

Reference jobs from deployment units:

```java
DeploymentUnit unit = new DeploymentUnit("my-jobs", "1.0.0");

JobDescriptor<String, Integer> descriptor =
    JobDescriptor.builder("com.example.CustomJob")
        .units(unit)
        .build();

CompletableFuture<JobExecution<Integer>> execution =
    ignite.compute().submitAsync(target, descriptor, "input");
```

Deployment units enable versioned job deployment and isolation.

## Custom Serialization

Implement custom marshallers for job arguments and results:

```java
public class CustomJob implements ComputeJob<MyData, MyResult> {
    @Override
    public CompletableFuture<MyResult> executeAsync(
        JobExecutionContext context,
        MyData input
    ) {
        // Process input
        return CompletableFuture.completedFuture(new MyResult());
    }

    @Override
    public Marshaller<MyData, byte[]> inputMarshaller() {
        return new MyDataMarshaller();
    }

    @Override
    public Marshaller<MyResult, byte[]> resultMarshaller() {
        return new MyResultMarshaller();
    }
}
```

Custom marshallers control serialization for non-standard types.

## Map-Reduce Tasks

Execute map-reduce computations:

```java
public class WordCountTask implements MapReduceTask<String, String, Map<String, Integer>, Map<String, Integer>> {
    @Override
    public String name() {
        return "word-count";
    }

    @Override
    public CompletableFuture<List<MapReduceJob<String, Map<String, Integer>>>>
        splitAsync(TaskExecutionContext context, String input) {

        List<MapReduceJob<String, Map<String, Integer>>> jobs = new ArrayList<>();
        String[] lines = input.split("\n");

        for (String line : lines) {
            jobs.add(new WordCountJob(line));
        }

        return CompletableFuture.completedFuture(jobs);
    }

    @Override
    public CompletableFuture<Map<String, Integer>> reduceAsync(
        TaskExecutionContext context,
        List<Map<String, Integer>> results
    ) {
        Map<String, Integer> combined = new HashMap<>();
        for (Map<String, Integer> result : results) {
            result.forEach((word, count) ->
                combined.merge(word, count, Integer::sum)
            );
        }
        return CompletableFuture.completedFuture(combined);
    }
}
```

Submit map-reduce tasks:

```java
TaskDescriptor<String, Map<String, Integer>> taskDescriptor =
    TaskDescriptor.builder(new WordCountTask()).build();

CompletableFuture<JobExecution<Map<String, Integer>>> execution =
    ignite.compute().executeMapReduceAsync(
        taskDescriptor,
        "line one\nline two\nline three"
    );

Map<String, Integer> wordCounts = execution
    .thenCompose(JobExecution::resultAsync)
    .join();
```

## Error Handling

Handle compute exceptions:

```java
try {
    JobExecution<Integer> execution = executionFuture.join();
    Integer result = execution.resultAsync().join();
} catch (CompletionException e) {
    if (e.getCause() instanceof ComputeException) {
        System.err.println("Compute error: " + e.getCause().getMessage());
    } else if (e.getCause() instanceof NodeNotFoundException) {
        System.err.println("Target node not found");
    }
}
```

## Reference

- Compute facade: `org.apache.ignite.compute.IgniteCompute`
- Job interface: `org.apache.ignite.compute.ComputeJob<T, R>`
- Job execution: `org.apache.ignite.compute.JobExecution<R>`
- Job targeting: `org.apache.ignite.compute.JobTarget`
- Job descriptor: `org.apache.ignite.compute.JobDescriptor<T, R>`
- Job context: `org.apache.ignite.compute.JobExecutionContext`
- Broadcast execution: `org.apache.ignite.compute.BroadcastExecution<R>`
- Map-reduce: `org.apache.ignite.compute.task.MapReduceTask<I, M, T, R>`
- Task descriptor: `org.apache.ignite.compute.task.TaskDescriptor<I, R>`
- Deployment: `org.apache.ignite.deployment.DeploymentUnit`

### IgniteCompute Methods

- `<T, R> CompletableFuture<JobExecution<R>> submitAsync(JobTarget, JobDescriptor<T, R>, T)` - Submit job
- `<T, R> CompletableFuture<JobExecution<R>> submitAsync(JobTarget, JobDescriptor<T, R>, T, CancellationToken)` - Submit with cancellation
- `<T, R> CompletableFuture<BroadcastExecution<R>> submitAsync(BroadcastJobTarget, JobDescriptor<T, R>, T)` - Submit broadcast
- `<T, R> CompletableFuture<JobExecution<R>> executeMapReduceAsync(TaskDescriptor<T, R>, T)` - Execute map-reduce

### JobExecution Methods

- `CompletableFuture<R> resultAsync()` - Get job result
- `CompletableFuture<JobState> stateAsync()` - Get job state
- `CompletableFuture<UUID> idAsync()` - Get job ID
- `CompletableFuture<Boolean> changePriorityAsync(int)` - Change priority

### JobTarget Factory Methods

- `static JobTarget node(ClusterNode)` - Target specific node
- `static JobTarget anyNode(ClusterNode...)` - Target any from set
- `static JobTarget colocated(QualifiedName, Object, Mapper)` - Target colocated with data

### BroadcastJobTarget Factory Methods

- `static BroadcastJobTarget nodes(Collection<ClusterNode>)` - Target all specified nodes

### JobExecutionContext Methods

- `Ignite ignite()` - Get Ignite instance
- `boolean isCancelled()` - Check if job is cancelled
- `int partition()` - Get partition number for colocated jobs
- `List<DeploymentUnit> deploymentUnits()` - Get deployment units

### BroadcastExecution Methods

- `Map<ClusterNode, JobExecution<R>> executions()` - Get job executions by node
- `CompletableFuture<List<R>> resultsAsync()` - Get all results asynchronously

### ComputeJob Interface

- `CompletableFuture<R> executeAsync(JobExecutionContext, T)` - Execute job
- `Marshaller<T, byte[]> inputMarshaller()` - Custom input serialization
- `Marshaller<R, byte[]> resultMarshaller()` - Custom result serialization
