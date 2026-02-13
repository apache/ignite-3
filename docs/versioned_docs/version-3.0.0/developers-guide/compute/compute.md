---
title: Distributed Computing
sidebar_label: Distributed Computing
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

Apache Ignite lets you run your own code on the cluster in a distributed, balanced, and fault-tolerant way.

Tasks can run on a single node, multiple nodes, or across the entire cluster, and you can choose between synchronous and asynchronous execution.

:::note
Apache Ignite compute engine now supports jobs implemented both in Java and in .NET. As .NET compute jobs require a bit of extra setup, see the [.NET Compute Jobs](#net-compute-jobs) subsection for details.
:::

In addition to standard compute tasks, Apache Ignite supports [Colocated Execution](#colocated-execution). This means your tasks can run directly on the nodes that store the required data, reducing network overhead and improving performance.
The cluster also supports [MapReduce Tasks](#mapreduce-tasks), allowing for efficient processing of large datasets. In this case, tasks will be executed on nodes that hold the data required for them.

When sending code and data between nodes, objects are converted into a transferable format so they can be accurately rebuilt. Apache Ignite automatically handles marshalling for common types like tuples, POJOs, and native types, but for more complex or custom objects, you may need to implement your own [marshalling](../compute/serialization.md) logic.

## Compute Job Code Deployment

Before submitting your compute job, ensure that the required code is [deployed](../code-deployment/code-deployment.md) to the nodes where it will execute.

If you are using [embedded nodes](../../quick-start/embedded-mode.md), any code that is included in the project classpath will also be available to your compute jobs.

## Configuring Jobs

In Apache Ignite, compute job's execution is defined by two key components: `JobTarget` and `JobDescriptor`. These components determine on which nodes the job will run and how it will be structured, including input and output types, marshallers, and the deployed class that represents the job.

### Job Target
Before submitting a job, you must create a `JobTarget` object that specifies which nodes will execute the job. Job target can point to a specific node, any node on the cluster, or start a [colocated](#colocated-execution) compute job, that will be executed on nodes that hold a specific key. The following methods are available:

- `JobTarget.anyNode()` - the job will be executed on any of the specified nodes.
- `JobTarget.node()` - the job will be executed on the specific node.
- `JobTarget.colocated()` - the job will be executed on a node that holds the specified key.

:::note
Use the `BroadcastJobTarget` object instead in case you want to execute a job across [multiple nodes](#multiple-node-execution).
:::

### Job Descriptor

The `JobDescriptor` object contains all the details required for job execution. The following arguments must be provided:

- The job descriptor is created using a builder that specifies the input type for the job arguments, the expected output type, and the fully qualified name of the job class to execute.
- `units` takes your deployment unit. You create it with the unit's name and specify `Version.LATEST` so that your job always runs the most recently deployed version.
- `resultClass` sets the expected result type so the system can correctly process the job's output.
- `argumentMarshaller` and `resultMarshaller` defines how to serialize the job's input argument and output result. For common types, you can omit the marshallers and pass `null` to the builder since Apache Ignite automatically handles marshalling.

Examples below assumes that the `NodeNameJob` class has been deployed to the node by using [code deployment](../code-deployment/code-deployment.md).

- If you are working with common types, you don't need to define custom marshallers. Apache Ignite will handle them automatically. The following example shows a simpler job descriptor that uses built-in marshalling:

```java
String result = client.compute().execute(
    JobTarget.anyNode(client.cluster().nodes()),
    JobDescriptor.builder(WordPrintJob.class)
        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
        .resultClass(String.class)
        .build(),
    "Hello, Ignite!"
);
```

- This example shows how to create a custom job descriptor for a job that takes a user-defined `MyJobArgument`, runs on a random cluster node, and returns a `MyJobResult` object using custom marshallers:

```java
MyJobResult result = client.compute().execute(
    JobTarget.anyNode(client.cluster().nodes()),
    JobDescriptor.<MyJobArgument, MyJobResult>builder(WordPrintJob.class)
        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
        .resultClass(MyJobResult.class)
        .argumentMarshaller(new ArgMarshaller())
        .resultMarshaller(new ResultMarshaller())
        .build(),
    new MyJobArgument("Hello, Ignite!")
);
```

For more details on configuring jobs refer to the corresponding API section.

### Deployment Unit Information

The `JobExecutionContext` object contains the information about the deployment units the job is using as a collection of `DeploymentUnitInfo` for each deployment unit involved in the job.

Each `DeploymentUnitInfo` object provides the following information:

* `name()` - The name of the deployment unit
* `version()` - The version of the deployment unit
* `path()` - The filesystem path to the deployment unit contents

```java
public class DiagnosticJob implements ComputeJob<Void, String> {
    @Override
    public CompletableFuture<String> executeAsync(JobExecutionContext context, Void input) {
        // Access deployment unit information
        String deploymentInfo = context.deploymentUnits().stream()
            .map(unit -> String.format("%s:%s at %s",
                unit.name(),
                unit.version(),
                unit.path()))
            .collect(Collectors.joining(", "));

        return CompletableFuture.completedFuture(deploymentInfo);
    }
}
```

## Executing Jobs

Apache Ignite compute jobs can run on a specific node, any node, or using a colocated approach when job is executed on the node holding the relevant data key.

### Single Node Execution

Often, you need to perform a job on one node in the cluster. In this case, there are multiple ways to start job execution:

- `submitAsync()` - sends the job to the cluster and returns a future that will be completed with the `JobExecution` object when the job is submitted for execution.
- `executeAsync()` - sends the job to the cluster and returns a future that will be completed when job execution result is ready.
- `execute()` - sends the job to the cluster and waits for the result of job execution.

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder()
        .addresses("127.0.0.1:10800")
        .build()
) {

    System.out.println("\nConfiguring compute job...");

    JobDescriptor<String, Void> job = JobDescriptor.builder(WordPrintJob.class)
            .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
            .build();

    JobTarget jobTarget = JobTarget.anyNode(client.clusterNodes());


    for (String word : "Print words using runnable".split(" ")) {

        System.out.println("\nExecuting compute job for word '" + word + "'...");

        client.compute().execute(jobTarget, job, word);
    }
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
ICompute compute = Client.Compute;
IList<IClusterNode> nodes = await Client.GetClusterNodesAsync();

IJobExecution<string> execution = await compute.SubmitAsync(
JobTarget.AnyNode(nodes),
new JobDescriptor<string, string>("org.example.NodeNameJob"),
arg: "Hello");

string result = await execution.GetResultAsync();
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
using namespace ignite;

compute comp = client.get_compute();
std::vector<cluster_node> nodes = client.get_nodes();

// Unit `unitName:1.1.1` contains NodeNameJob class.
auto job_desc = job_descriptor::builder("org.company.package.NodeNameJob")
.deployment_units({deployment_unit{"unitName", "1.1.1"}})
.build();

job_execution execution = comp.submit(job_target::any_node(nodes), job_desc, {std::string("Hello")}, {});
std::string result = execution.get_result()->get<std::string>();
```

</TabItem>
</Tabs>

### Multiple Node Execution

To execute the compute task on multiple nodes, you use the same methods as for single node execution, except instead of creating a `JobTarget` object to designate execution nodes you use the `BroadcastJobTarget` and specify the list of nodes that the job must be executed on.

The `BroadcastJobTarget` object can specify the following:

- `BroadcastJobTarget.nodes()` - the job will be executed on all nodes in the list.
- `BroadcastJobTarget.table()` - the job will be executed on all nodes that hold partitions of the specified table.

You can control what nodes the task is executed on by setting the list of nodes:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder()
        .addresses("127.0.0.1:10800")
        .build()
) {

    System.out.println("\nConfiguring compute job...");


    JobDescriptor<String, Void> job = JobDescriptor.builder(HelloMessageJob.class)
            .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
            .build();

    BroadcastJobTarget target = BroadcastJobTarget.nodes(client.cluster().nodes());


    System.out.println("\nExecuting compute job...");

    client.compute().execute(target, job, "John");

    System.out.println("\nCompute job executed...");
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
ICompute compute = Client.Compute;
IList<IClusterNode> nodes = await Client.GetClusterNodesAsync();

IBroadcastExecution<string> execution = await compute.SubmitBroadcastAsync(
BroadcastJobTarget.Nodes(nodes),
new JobDescriptor<object, string>("org.example.NodeNameJob"),
arg: "Hello");

foreach (IJobExecution<string> jobExecution in execution.JobExecutions)
{
string jobResult = await jobExecution.GetResultAsync();
Console.WriteLine($"Job result from node {jobExecution.Node}: {jobResult}");
}
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
using namespace ignite;

compute comp = client.get_compute();
std::vector<cluster_node> nodes = client.get_nodes();

// Unit `unitName:1.1.1` contains NodeNameJob class.
auto job_desc = job_descriptor::builder("org.company.package.NodeNameJob")
.deployment_units({deployment_unit{"unitName", "1.1.1"}})
.build();

broadcast_execution execution = comp.submit_broadcast(broadcast_job_target::nodes(nodes), job_desc, {std::string("Hello")}, {});
for (auto &exec: execution.get_job_executions()) {
std::string result = exec.get_result()->get<std::string>();
}
```

</TabItem>
</Tabs>

### Colocated Execution

In Apache Ignite, you can execute colocated computations by specifying a job target that directs the task to run on the node holding the required data.

In the example below, the job runs on the node that owns the partition for the row in the `accounts` table identified by the primary key `accountNumber`.
We pass the key both to `JobTarget.colocated()` to select the node and as the
job argument, so the job knows which record to read.


<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder()
        .addresses("127.0.0.1:10800")
        .build()) {

    System.out.println("\nConfiguring compute job...");

    JobDescriptor<Integer, Void> job = JobDescriptor.builder(PrintAccountInfoJob.class)
                    .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                    .build();

    int accountNumber = ThreadLocalRandom.current().nextInt(ACCOUNTS_COUNT);

    JobTarget jobTarget = JobTarget.colocated("accounts", accountKey(accountNumber));
    client.compute().execute(jobTarget, job, accountNumber);
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
string table = "Person";
string key = "John";

IJobExecution<string> execution = await Client.Compute.SubmitAsync(
JobTarget.Colocated(table, key),
new JobDescriptor<string, string>("org.example.NodeNameJob"),
arg: "Hello");

string result = await execution.GetResultAsync();

```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
using namespace ignite;

compute comp = client.get_compute();
std::string table{"Person"};
std::string key{"John"};

// Unit `unitName:1.1.1` contains NodeNameJob class.
auto job_desc = job_descriptor::builder("org.company.package.NodeNameJob")
.deployment_units({deployment_unit{"unitName", "1.1.1"}})
.build();

job_execution execution = comp.submit(job_target::colocated(table, key), job_desc, {std::string("Hello")}, {});
std::string result = execution.get_result()->get<std::string>();
```

</TabItem>
</Tabs>

Alternatively, you can execute the compute job on all nodes in the cluster that hold partitions for the specified table by creating a `BroadcastJobTarget.table()` target. In this case, Apache Ignite will automatically find all nodes that hold data partitions for the specified table and execute the job on all of them.

## Using Qualified Table Names

If you do not specify the table schema, the `PUBLIC` schema will be used. To use a different schema, specify a fully qualified table name. You can provide it in a string or by creating the `QualifiedName` object:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
QualifiedName myTableName = QualifiedName.parse("PUBLIC.MY_QUALIFIED_TABLE");
String executionResult = client.compute()
.execute(
JobTarget.colocated(myTableName, Tuple.create(Map.of("k", 1))),
JobDescriptor.builder(NodeNameJob.class).build(),
null
);
```

</TabItem>
<TabItem value="dotnet" label=".NET">

Not supported

</TabItem>
<TabItem value="cpp" label="C++">

Not supported

</TabItem>
</Tabs>

Just like with execution on a single node, you can use the `QualifiedName` object to specify a qualified table name and run a job on multiple nodes using `BroadcastJobTarget`:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
QualifiedName customSchemaTable = QualifiedName.parse("CUSTOM_SCHEMA.MY_QUALIFIED_TABLE");

client.compute().execute(BroadcastJobTarget.table(customSchemaTable), JobDescriptor.builder(HelloMessageJob.class).build(), null);
```

</TabItem>
</Tabs>

You can also use the `of` method to instead specify the table name and the schema separately:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
QualifiedName customSchemaTableName = QualifiedName.of("PUBLIC", "MY_TABLE");

client.compute().execute(BroadcastJobTarget.table(customSchemaTableName), JobDescriptor.builder(HelloMessageJob.class).build(), null);
```

</TabItem>
</Tabs>

The provided names must follow SQL syntax rules for identifiers:

- Identifier must start from a character in the "Lu", "Ll", "Lt", "Lm", "Lo", or "Nl" Unicode categories;
- Identifier characters (except for the first one) may be `U+00B7` (middle dot), `U+0331` (underscore), or any character in the "Mn", "Mc", "Nd", "Pc", or "Cf" Unicode categories;
- Identifiers that contain any other characters must be quoted with double-quotes;
- Double-quote inside the identifier must be 2 double-quote chars.

Any unquoted names will be cast to upper case. In this case, `Person` and `PERSON` names are equivalent. To avoid this, add escaped quotes around the name. For example, `\"Person\"` will be encoded as a case-sensitive `Person` name. If the name contains the `U+2033` (double quote) symbol, it must be escaped as `""` (2 double quote symbols).

## .NET Compute Jobs

When working with compute jobs written in .NET, resulting binaries (DLL files) should be deployed to server nodes and invoked by the assembly-qualified type name. Every deployment unit combination is loaded into a separate [AssemblyLoadContext](https://learn.microsoft.com/en-us/dotnet/core/dependency-loading/understanding-assemblyloadcontext).

You can have multiple versions of the same job (assembly) deployed to the cluster as Apache Ignite supports deployment unit isolation. One job can consist of multiple deployment units. Assemblies and types are looked up in the order you list them.

:::note
.NET compute jobs are executed in a separate process ([Sidecar](https://learn.microsoft.com/en-us/azure/architecture/patterns/sidecar)) on the server node. The process is started on the first .NET job call and then reused for subsequent jobs.
:::

Compute job classes may implement `IDisposable` and `IAsyncDisposable` interfaces. Apache Ignite will call `Dispose` or `DisposeAsync` after job execution whether it succeeds or fails.

#### .NET Compute Requirements

* .NET 8 Runtime or later (not SDK) is required on each server node.
* When using ZIP, DEB, RPM installation, you have to install .NET runtime yourself. Apache Ignite Docker image includes .NET 8 runtime, so you can run .NET jobs in Docker out of the box.

### Implementing .NET Compute Jobs

Below is an example on implementing a .NET compute job:

1. First, prepare a "class library" project for the job implementation using `dotnet new classlib`.

:::tip
In most cases, it is better to use a separate project for compute jobs to reduce deployment size.
:::

```bash
dotnet new classlib -n MyComputeJobs
cd MyComputeJobs
dotnet add package Apache.Ignite
```

2. Add a reference to `Apache.Ignite` package to the class library project:

```bash
dotnet add package Apache.Ignite
```

3. Then create a class that implements `IComputeJob<TArg, TRes>` interface, for example:

```csharp
public class HelloJob : IComputeJob<string, string>
{
public ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken) =>
ValueTask.FromResult("Hello " + arg);
}
```

4. Publish the project by using the `dotnet publish -c Release` command:

```bash
dotnet publish -c Release
mkdir deploy
cp bin/Release/net8.0/MyComputeJobs.dll deploy/
# Exclude Ignite assemblies; no subdirectories allowed
ignite cluster unit deploy --name MyDotNetJobsUnit --path ./deploy
```

5. Copy the resulting dll file and any extra dependencies to a separate directory, **excluding** Apache Ignite dlls.

:::note
The directory with the dll must not contain any subdirectories.
:::

6. Use the Apache Ignite CLI command `cluster unit deploy command` to [deploy](../code-deployment/code-deployment.md) the directory to the cluster as a deployment unit. The deployed code will be available on the cluster.

### Running .NET Compute Jobs

You can execute .NET compute jobs from any client (.NET, Java, C++, etc) as long as you created a `JobDescriptor` with the assembly-qualified job class name and set `JobExecutionOptions` with `JobExecutorType.DotNetSidecar`.

- For example, this is how to run your job on a single node from .NET:

```csharp
var jobTarget = JobTarget.AnyNode(await client.GetClusterNodesAsync());
var jobDesc = new JobDescriptor<string, string>(
JobClassName: typeof(HelloJob).AssemblyQualifiedName!,
DeploymentUnits: [new DeploymentUnit("MyDeploymentUnit")],
Options: new JobExecutionOptions(ExecutorType: JobExecutorType.DotNetSidecar));

IJobExecution<string> jobExec = await client.Compute.SubmitAsync(jobTarget, jobDesc, "world");
```

Alternatively, use the `JobDescriptor.Of` shortcut method to create a job descriptor from a job instance:

```csharp
JobDescriptor<string, string> jobDesc = JobDescriptor.Of(new HelloJob())
with { DeploymentUnits = [new DeploymentUnit("MyDeploymentUnit")] };
```

- You can call [Java computing jobs](../compute/compute.md) from your .NET code, for example:

```csharp
IList<IClusterNode> nodes = await client.GetClusterNodesAsync();
IJobTarget<IEnumerable<IClusterNode>> jobTarget = JobTarget.AnyNode(nodes);

var jobDesc = new JobDescriptor<string, string>(JobClassName: "org.foo.bar.MyJob", DeploymentUnits: [new DeploymentUnit("MyDeploymentUnit")]);

IJobExecution<string> jobExecution = await client.Compute.SubmitAsync(jobTarget, jobDesc, "Job Arg");

string jobResult = await jobExecution.GetResultAsync();
```

- You can also run .NET compute jobs from Java client, for example:

```java
try (IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800")
.build()
) {

JobDescriptor<String, String> jobDesc = JobDescriptor.<String, String>builder().jobClassName("MyNamespace.HelloJob, MyComputeJobsAssembly").deploymentUnits(new DeploymentUnit("MyDeploymentUnit")).executionOptions(new JobExecutionOptions().executorType(JobExecutorType.DotNetSidecar)).build();

JobTarget jobTarget = JobTarget.anyNode(client.clusterNodes());
for (String word : "Print words using runnable".split(" ")) {

    System.out.println("\nExecuting compute job for word '" + word + "'...");

    client.compute().execute(jobTarget, job, word);
    }
}

```


## Job Ownership

If the cluster has [Authentication](../../administrators-guide/security/authentication.md) enabled, compute jobs are executed by a specific user. If user permissions are configured on the cluster, the user needs the appropriate [distributed computing permissions](../../administrators-guide/security/authentication.md) to work with distributed computing jobs. Only users with `JOBS_ADMIN` action can interact with jobs of other users.

## Job Execution States

When using asynchronous API, you can keep track of the status of the job on the server and react to status changes. For example:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
public static void example() throws ExecutionException, InterruptedException {
IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800").build();

CompletableFuture<JobExecution<Void>> execution = client.compute().submitAsync(JobTarget.anyNode(client.cluster().nodes()), JobDescriptor.builder(WordPrintJob.class).build(), null);

execution.get().stateAsync().thenApply(state -> {
                if (state.status() == FAILED) {
                    System.out.println("\nJob failed...");
                }
                return null;
            });
            System.out.println(execution.resultAsync().get());
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
IList<IClusterNode> nodes = await Client.GetClusterNodesAsync();

IJobExecution<string> execution = await Client.Compute.SubmitAsync(
    JobTarget.AnyNode(nodes),
    new JobDescriptor<string, string>("org.example.NodeNameJob"),
    arg: "Hello");

JobState? state = await execution.GetStateAsync();

if (state?.Status == JobStatus.Failed)
{
    // Handle failure
}

string result = await execution.GetResultAsync();
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
using namespace ignite;

compute comp = client.get_compute();
std::vector<cluster_node> nodes = client.get_nodes();

// Unit `unitName:1.1.1` contains NodeNameJob class.
auto job_desc = job_descriptor::builder("org.company.package.NodeNameJob")
	.deployment_units({deployment_unit{"unitName", "1.1.1"}})
	.build();

job_execution execution = comp.submit(job_target::any_node(nodes), job_desc, {std::string("Hello")}, {});

std::optional<job_status> status = execution.get_status();
if (status && status->state == job_state::FAILED)
{
    // Handle failure
}
std::string result = execution.get_result()->get<std::string>();
```

</TabItem>
</Tabs>

### Possible States and Transitions

The diagram below depicts the possible transitions of job statuses:

![Job Status Diagram](/img/compute_job_statuses.png)

The table below lists the possible job statuses:

| Status | Description | Transitions to |
|--------|-------------|----------------|
| `Queued` | The job was added to the queue and is waiting for execution. | `Executing`, `Canceled` |
| `Executing` | The job is being executed. | `Canceling`, `Completed`, `Queued` |
| `Completed` | The job was executed successfully and the execution result was returned. | |
| `Failed` | The job was unexpectedly terminated during execution. | `Queued` |
| `Canceling` | Job has received the cancel command, but is still running. | `Completed`, `Canceled` |
| `Canceled` | Job was successfully cancelled. | |

If all job execution threads are busy, new jobs received by the node are put into job queue according to their [Job Priority](#job-priority). Apache Ignite sorts all incoming jobs first by priority, then by the time, executing jobs queued earlier first.

### Cancelling Executing Jobs

When the node receives the command to cancel the job in the `Executing` status, it will immediately send an interrupt to the thread that is responsible for the job. In most cases, this will lead to the job being immediately canceled, however there are cases in which the job will continue. If this happens, the job will be in the `Canceling` state. Depending on specific code being executed, the job may complete successfully, be canceled once the uninterruptible operation is finished, or remain in unfinished state (for example, if code is stuck in a loop). You can use the `JobExecution.stateAsync()` method to keep track of what status the job is in, and react to status change.

To be able to cancel a compute job, you first create a cancel handler and retrieve a token from it. You can then use this token to cancel the compute job:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
CancelHandle cancelHandle = CancelHandle.create();
CancellationToken cancelToken = cancelHandle.token();

CompletableFuture<Void> execution = client.compute().executeAsync(JobTarget.anyNode(client.clusterNodes()), JobDescriptor.builder(NodeNameJob.class).build(), cancelToken, null);

cancelHandle.cancel();
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
var cts = new CancellationTokenSource();
CancellationToken cancelToken = cts.Token;

var execution = client.Compute.ExecuteAsync(
JobTarget.AnyNode(await client.GetClusterNodesAsync()),
new JobDescriptor(typeof(NodeNameJob)),
cancelToken);

cts.Cancel();
```

</TabItem>
</Tabs>

Another way to cancel jobs is by using the SQL [KILL COMPUTE](../../sql-reference/operational-commands.md#kill-compute) command. The job id can be retrieved via the `COMPUTE_JOBS` [system view](../../administrators-guide/metrics/system-views.md).

### Job Priority

You can specify a job priority by setting the `JobExecutionOptions.priority` property. Jobs with a higher priority will be queued before jobs with lower priority (for example, a job with priority 4 will be executed before the job with priority 2).

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
public static void example() throws ExecutionException, InterruptedException {
try (IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {

    // Create job execution options
    JobExecutionOptions options = JobExecutionOptions.builder().priority(1).build();

    String executionResult = client.compute().execute(JobTarget.anyNode(client.cluster().nodes()),
            JobDescriptor.builder(HighPriorityJob.class).options(options).build(), null
    );

    System.out.println(executionResult);
    }
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
var options = JobExecutionOptions.Default with { Priority = 1 };

IJobExecution<string> execution = await Client.Compute.SubmitAsync(
    JobTarget.AnyNode(await Client.GetClusterNodesAsync()),
    new JobDescriptor<string, string>("org.example.NodeNameJob", Options: options),
    arg: "Hello");

string result = await execution.GetResultAsync();
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
using namespace ignite;

compute comp = client.get_compute();
std::vector<cluster_node> nodes = client.get_nodes();

// Unit `unitName:1.1.1` contains NodeNameJob class.
auto job_desc = job_descriptor::builder("org.company.package.NodeNameJob")
	.deployment_units({deployment_unit{"unitName", "1.1.1"}})
	.build();

job_execution_options options{1, 0};
job_execution execution = comp.submit(job_target::any_node(nodes), job_desc, {std::string("Hello")}, std::move(options));
std::string result = execution.get_result()->get<std::string>();
```

</TabItem>
</Tabs>

### Job Retries

You can set the number the job will be retried on failure by setting the `JobExecutionOptions.maxRetries` property. If set, the failed job will be retried the specified number of times before moving to `Failed` state.

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
public static void example() throws ExecutionException, InterruptedException {
try (IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {

   // Create job execution options with maxRetries set to 5.
    JobExecutionOptions options = JobExecutionOptions.builder()
                                                          .maxRetries(5)
                                                          .build();

    String executionResult = client.compute().execute(JobTarget.anyNode(client.clusterNodes()),
            JobDescriptor.builder(NodeNameJob.class).options(options).build(), null
    );

    System.out.println(executionResult);
    }
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
var options = JobExecutionOptions.Default with { MaxRetries = 5 };

IJobExecution<string> execution = await Client.Compute.SubmitAsync(
    JobTarget.AnyNode(await Client.GetClusterNodesAsync()),
    new JobDescriptor<string, string>("org.example.NodeNameJob", Options: options),
    arg: "Hello");

string result = await execution.GetResultAsync();
```

</TabItem>
<TabItem value="cpp" label="C++">

```cpp
using namespace ignite;

compute comp = client.get_compute();
std::vector<cluster_node> nodes = client.get_nodes();

// Unit `unitName:1.1.1` contains NodeNameJob class.
std::vector<deployment_unit> units{deployment_unit{"unitName", "1.1.1"}};

job_execution_options options{0, 5};
job_execution execution = comp.submit(nodes, units, NODE_NAME_JOB, {std::string("Hello")}, std::move(options));
std::string result = execution.get_result()->get<std::string>();
```

</TabItem>
</Tabs>

## Job Failover

Apache Ignite implements mechanics to handle issues that happen during job execution. The following situations are handled:

### Worker Node Shutdown

If the worker node is shut down, the coordinator node will redistribute all jobs assigned to worker to other viable nodes. If no nodes are found, the job will fail and an exception will be sent to the client.

### Coordinator Node Shutdown

If the coordinator node shuts down, all jobs will be cancelled as soon as the node detects that the coordinator is shut down. Note that [some jobs](#cancelling-executing-jobs) may take a long time to cancel.

### Client Disconnect

If the client disconnects, all jobs will be cancelled as soon as the coordinator node detects the disconnect. Note that [some jobs](#cancelling-executing-jobs) may take a long time to cancel.

## MapReduce Tasks

Apache Ignite provides an API for performing MapReduce operations in the cluster. This allows you to split your computing task between multiple nodes before aggregating the result and returning it to the user.

### Understanding MapReduce Tasks

A MapReduce task must be executed on a node that has a [deployed](../code-deployment/code-deployment.md) class implementing the `MapReduceTask` interface. This interface provides a way to implement custom map and reduce logic. A node that receives the task becomes a coordinator node, that will be responsible for both mapping tasks to other nodes, reducing their results and returning the final result to the client.

The class must implement two methods: `splitAsync` and `reduceAsync`.

The `splitAsync()` method should be implemented to create compute jobs based on input parameters and map them to worker nodes. The method receives the execution context and your task arguments and returns a completable future containing the list of the job descriptors that will be sent to the worker nodes.

The `reduceAsync()` method is called during the reduce step, when all the jobs have completed. The method receives a map from the worker node to the completed job result and returns the final result of the computation.

### Creating a Mapper Class

All MapReduce jobs must be submitted to a node that has an appropriate class [deployed](../code-deployment/code-deployment.md). Below is an example of a map reduce job:

<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
public static class PhraseWordLengthCountMapReduceTask implements MapReduceTask<String, String, Integer, Integer> {
    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<MapReduceJob<String, Integer>>> splitAsync(
            TaskExecutionContext taskContext,
            String input) {
        assert input != null;

        var job = JobDescriptor.builder(WordLengthJob.class)
                .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                .build();

        List<MapReduceJob<String, Integer>> jobs = new ArrayList<>();

        for (String word : input.split(" ")) {
            jobs.add(
                    MapReduceJob.<String, Integer>builder()
                            .jobDescriptor(job)
                            .nodes(taskContext.ignite().cluster().nodes())
                            .args(word)
                            .build()
            );
        }

        return completedFuture(jobs);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Integer> reduceAsync(TaskExecutionContext taskContext, Map<UUID, Integer> results) {
        return completedFuture(results.values().stream()
                .reduce(Integer::sum)
                .orElseThrow());
    }
}
```

</TabItem>
</Tabs>

### Executing a MapReduce Task

To execute the MapReduce task, you use one of the following methods:

- `submitMapReduce()` - sends the MapReduce job to the cluster and returns the `TaskExecution` object that can be used to monitor or modify the compute task execution.
- `executeMapReduceAsync()` - sends the MapReduce job to the cluster in the cluster and gets the future for job execution results.
- `executeMapReduce()` - sends the job to the cluster and waits for the result of job execution.

The node that the MapReduce task is sent to must have a class implementing the `MapReduceTask` interface.


<Tabs groupId="programming-languages">
<TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {

    System.out.println("\nConfiguring map reduce task...");


    TaskDescriptor<String, Integer> taskDescriptor = TaskDescriptor.builder(PhraseWordLengthCountMapReduceTask.class)
            .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
            .build();


    System.out.println("\nExecuting map reduce task...");

    String phrase = "Count characters using map reduce";

    Integer result = client.compute().executeMapReduce(taskDescriptor, phrase);


    System.out.println("\nTotal number of characters in the words is '" + result + "'.");
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
ICompute compute = Client.Compute;
var taskDescriptor = new TaskDescriptor<string, string>("com.example.MapReduceNodeNameTask");
ITaskExecution<string> exec = await compute.SubmitMapReduceAsync(taskDescriptor, "arg");
string result = await exec.GetResultAsync();
Console.WriteLine(result);
```

</TabItem>
<TabItem value="cpp" label="C++">

Not supported

</TabItem>
</Tabs>
