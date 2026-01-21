---
title: Compute API
id: compute-api
sidebar_position: 5
---

# Compute API

The Compute API executes distributed compute jobs across cluster nodes. It supports single-node execution, multi-node execution, colocated execution, and broadcast patterns.

## Key Concepts

### Job Execution Model

Jobs are Java classes deployed on cluster nodes. The C++ client submits job execution requests with binary arguments. The server executes the job and returns results as binary objects.

### Job Targeting

Jobs execute on specific nodes based on targeting strategies:

- **Single Node** - Execute on one specific node
- **Any Node** - Execute on any node from a set
- **Colocated** - Execute on the node containing table partition data
- **Broadcast** - Execute on all nodes in a set

### Job Descriptors

Job descriptors specify the job class name, deployment units, and execution options. Deployment units identify code locations on the cluster. Execution options configure priority and other runtime parameters.

### Execution Handles

Submit operations return execution handles. Handles provide job monitoring, result retrieval, priority changes, and cancellation. Results become available after job completion.

### Broadcast Execution

Broadcast submits a single job to multiple nodes. It returns a broadcast execution handle containing individual execution handles for each node.

## Basic Usage

### Getting Cluster Nodes

Retrieve cluster topology:

```cpp
using namespace ignite;

auto nodes = client.get_cluster_nodes();

for (const auto& node : nodes) {
    std::cout << "Node: " << node.get_name() << std::endl;
    std::cout << "ID: " << node.get_id() << std::endl;
    std::cout << "Address: " << node.get_address().host
              << ":" << node.get_address().port << std::endl;
}
```

### Submitting Jobs

Submit a job to any node:

```cpp
auto compute = client.get_compute();
auto nodes = client.get_cluster_nodes();

auto descriptor = job_descriptor::builder("com.example.MyJob").build();
auto target = job_target::any_node(nodes);

binary_object arg;  // Job argument
auto execution = compute.submit(target, descriptor, arg);

auto result = execution.get_result();
if (result.has_value()) {
    // Process result
}
```

### Async Submission

Submit without blocking:

```cpp
compute.submit_async(target, descriptor, arg,
    [](ignite_result<job_execution> result) {
        if (!result.has_error()) {
            auto execution = std::move(result).value();
            // Use execution handle
        }
    });
```

## Job Targets

### Single Node Target

Execute on a specific node:

```cpp
auto nodes = client.get_cluster_nodes();
auto target_node = nodes[0];

auto target = job_target::node(target_node);
auto execution = compute.submit(target, descriptor, arg);
```

### Any Node Target

Execute on any node from a set:

```cpp
auto nodes = client.get_cluster_nodes();
auto target = job_target::any_node(nodes);

auto execution = compute.submit(target, descriptor, arg);
```

Create from vector:

```cpp
std::vector<cluster_node> node_list = {node1, node2, node3};
auto target = job_target::any_node(node_list);
```

Create from set:

```cpp
std::set<cluster_node> node_set = {node1, node2, node3};
auto target = job_target::any_node(node_set);
```

### Colocated Target

Execute on the node containing table data:

```cpp
ignite_tuple key{{"id", 42}};
auto target = job_target::colocated("accounts", key);

auto execution = compute.submit(target, descriptor, arg);
```

Use qualified table names:

```cpp
qualified_name table_name = qualified_name::parse("my_schema.accounts");
auto target = job_target::colocated(table_name, key);
```

Colocated execution minimizes network overhead by running compute jobs where data resides.

## Job Descriptors

### Building Descriptors

Create a basic descriptor:

```cpp
auto descriptor = job_descriptor::builder("com.example.MyJob").build();
```

Add deployment units:

```cpp
std::vector<deployment_unit> units{
    {"my-app", "1.0.0"},
    {"my-lib", "2.1.0"}
};

auto descriptor = job_descriptor::builder("com.example.MyJob")
    .deployment_units(units)
    .build();
```

Add execution options:

```cpp
job_execution_options opts;
opts.priority(10);

auto descriptor = job_descriptor::builder("com.example.MyJob")
    .execution_options(opts)
    .build();
```

### Descriptor Components

**Job Class Name** - Fully qualified Java class name implementing the compute job interface

**Deployment Units** - List of units containing job code and dependencies

**Execution Options** - Priority and other runtime configuration

## Job Execution

### Monitoring Execution

Check job state:

```cpp
auto execution = compute.submit(target, descriptor, arg);

auto state = execution.get_state();
if (state.has_value()) {
    // Examine state
}
```

Get state asynchronously:

```cpp
execution.get_state_async([](ignite_result<std::optional<job_state>> result) {
    if (!result.has_error()) {
        auto state = std::move(result).value();
        if (state.has_value()) {
            // Examine state
        }
    }
});
```

State may be unavailable if the job has expired from the execution history.

### Retrieving Results

Get result (blocks until completion):

```cpp
auto result = execution.get_result();

if (result.has_value()) {
    auto data = result.value();
    // Process binary object
}
```

Get result asynchronously:

```cpp
execution.get_result_async([](ignite_result<std::optional<binary_object>> result) {
    if (!result.has_error()) {
        auto obj = std::move(result).value();
        if (obj.has_value()) {
            // Process result
        }
    }
});
```

### Execution Information

Access execution metadata:

```cpp
auto job_id = execution.get_id();
auto node = execution.get_node();

std::cout << "Job ID: " << job_id << std::endl;
std::cout << "Executing on: " << node.get_name() << std::endl;
```

## Job Control

### Cancelling Jobs

Cancel a running job:

```cpp
auto execution = compute.submit(target, descriptor, arg);

auto result = execution.cancel();

switch (result) {
    case job_execution::operation_result::SUCCESS:
        std::cout << "Job cancelled" << std::endl;
        break;
    case job_execution::operation_result::INVALID_STATE:
        std::cout << "Job already completed" << std::endl;
        break;
    case job_execution::operation_result::NOT_FOUND:
        std::cout << "Job not found" << std::endl;
        break;
}
```

Cancel asynchronously:

```cpp
execution.cancel_async([](ignite_result<job_execution::operation_result> result) {
    if (!result.has_error()) {
        auto status = result.value();
        // Check status
    }
});
```

### Changing Priority

Modify job execution priority:

```cpp
auto execution = compute.submit(target, descriptor, arg);

auto result = execution.change_priority(5);

if (result == job_execution::operation_result::SUCCESS) {
    std::cout << "Priority changed" << std::endl;
}
```

Change priority asynchronously:

```cpp
execution.change_priority_async(5,
    [](ignite_result<job_execution::operation_result> result) {
        // Handle result
    });
```

Higher priority values execute before lower priority jobs in the queue.

## Broadcast Execution

### Broadcasting to Multiple Nodes

Execute on all nodes in a set:

```cpp
auto nodes = client.get_cluster_nodes();
std::set<cluster_node> node_set(nodes.begin(), nodes.end());

auto target = broadcast_job_target::nodes(node_set);
auto broadcast = compute.submit_broadcast(target, descriptor, arg);
```

Broadcast to a single node:

```cpp
auto target = broadcast_job_target::node(specific_node);
auto broadcast = compute.submit_broadcast(target, descriptor, arg);
```

### Async Broadcast

Submit broadcast without blocking:

```cpp
compute.submit_broadcast_async(target, descriptor, arg,
    [](ignite_result<broadcast_execution> result) {
        if (!result.has_error()) {
            auto broadcast = std::move(result).value();
            // Use broadcast execution
        }
    });
```

### Processing Broadcast Results

Access individual executions:

```cpp
auto broadcast = compute.submit_broadcast(target, descriptor, arg);
auto executions = broadcast.get_job_executions();

for (auto& exec_result : executions) {
    if (exec_result.has_value()) {
        auto execution = exec_result.value();
        auto result = execution.get_result();

        if (result.has_value()) {
            std::cout << "Node " << execution.get_node().get_name()
                      << " result: " << /* process result */ << std::endl;
        }
    }
}
```

Each execution in the broadcast operates independently. Retrieve results individually from each execution handle.

## Binary Arguments

### Passing Primitive Arguments

Jobs accept binary_object arguments. Wrap primitive values:

```cpp
binary_object arg(42);  // Integer argument
auto execution = compute.submit(target, descriptor, arg);
```

### Passing Complex Arguments

Create binary objects from serialized data:

```cpp
// Serialize your data structure to bytes
std::vector<std::byte> data = serialize_my_data(my_object);
binary_object arg(data);

auto execution = compute.submit(target, descriptor, arg);
```

### No Argument Jobs

Pass empty binary object for jobs without arguments:

```cpp
binary_object empty_arg;
auto execution = compute.submit(target, descriptor, empty_arg);
```

## Error Handling

### Handling Job Errors

Job execution errors propagate to the client:

```cpp
try {
    auto result = execution.get_result();
    // Process result
} catch (const ignite_error& e) {
    std::cerr << "Job failed: " << e.what_str() << std::endl;
}
```

With async operations:

```cpp
execution.get_result_async([](ignite_result<std::optional<binary_object>> result) {
    if (result.has_error()) {
        std::cerr << "Error: " << result.error().what_str() << std::endl;
    } else {
        // Process result
    }
});
```

### Handling Submission Errors

Handle submission failures:

```cpp
try {
    auto execution = compute.submit(target, descriptor, arg);
} catch (const ignite_error& e) {
    std::cerr << "Submission failed: " << e.what_str() << std::endl;
}
```

Common errors include missing deployment units, invalid job class names, and network failures.

## Use Cases

### Map-Reduce Pattern

Submit jobs to multiple nodes and aggregate results:

```cpp
auto nodes = client.get_cluster_nodes();
std::vector<job_execution> executions;

// Map: Submit jobs to all nodes
for (const auto& node : nodes) {
    auto target = job_target::node(node);
    executions.push_back(compute.submit(target, map_job, arg));
}

// Reduce: Collect and aggregate results
std::vector<binary_object> results;
for (auto& execution : executions) {
    auto result = execution.get_result();
    if (result.has_value()) {
        results.push_back(result.value());
    }
}

auto final_result = reduce(results);
```

### Colocated Processing

Process data where it resides:

```cpp
// Execute compute job on the node containing this key
ignite_tuple key{{"customer_id", 12345}};
auto target = job_target::colocated("orders", key);

auto descriptor = job_descriptor::builder("com.example.OrderProcessor").build();
auto execution = compute.submit(target, descriptor, arg);

auto result = execution.get_result();
```

### Batch Job Submission

Submit multiple jobs in parallel:

```cpp
std::vector<job_execution> executions;

for (const auto& work_item : work_items) {
    auto execution = compute.submit(target, descriptor, work_item);
    executions.push_back(std::move(execution));
}

// Wait for all to complete
for (auto& execution : executions) {
    execution.get_result();
}
```

## Reference

- [C++ API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/cppdoc/)
- [Compute Concept](../../../develop/work-with-data/compute)
- [Client API](./client-api)
- [Network API](./network-api)
