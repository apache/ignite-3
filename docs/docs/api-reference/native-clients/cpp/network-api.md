---
title: Network API
id: network-api
sidebar_position: 6
---

# Network API

The Network API provides access to cluster topology information. It exposes cluster node metadata used for compute job targeting and cluster monitoring.

## Key Concepts

### Cluster Nodes

Cluster nodes represent individual server instances in the Ignite cluster. Each node has a unique identifier, a stable name, and network address information.

### Node Identity

Nodes have two forms of identification:

- **Node ID** - A UUID that changes when the node restarts
- **Node Name** - A stable string name that persists across restarts

Use node names for stable references. Use node IDs for runtime identification.

### Network Addresses

Each node exposes a network endpoint containing host and port information. Clients use these addresses to establish connections for operations.

## Cluster Node

### Node Properties

Cluster nodes provide three key properties:

```cpp
using namespace ignite;

auto nodes = client.get_cluster_nodes();
for (const auto& node : nodes) {
    // Unique ID (changes on restart)
    uuid id = node.get_id();

    // Stable name (persists across restarts)
    std::string name = node.get_name();

    // Network address
    end_point address = node.get_address();
}
```

### Accessing Node Information

Get the node ID:

```cpp
uuid node_id = node.get_id();
std::cout << "Node ID: " << node_id << std::endl;
```

Get the node name:

```cpp
std::string node_name = node.get_name();
std::cout << "Node: " << node_name << std::endl;
```

Get the network address:

```cpp
end_point addr = node.get_address();
std::cout << "Host: " << addr.host << std::endl;
std::cout << "Port: " << addr.port << std::endl;
```

### Node Comparison

Cluster nodes support full comparison:

```cpp
cluster_node node1 = nodes[0];
cluster_node node2 = nodes[1];

if (node1 == node2) {
    std::cout << "Same node" << std::endl;
}

if (node1 < node2) {
    std::cout << "node1 sorts before node2" << std::endl;
}
```

Comparison enables sorting and set operations:

```cpp
// Sort nodes by name
std::sort(nodes.begin(), nodes.end(),
    [](const auto& n1, const auto& n2) {
        return n1.get_name() < n2.get_name();
    });

// Create a set of nodes
std::set<cluster_node> node_set(nodes.begin(), nodes.end());
```

## Retrieving Cluster Nodes

### Synchronous Retrieval

Get all cluster nodes:

```cpp
auto nodes = client.get_cluster_nodes();

std::cout << "Cluster has " << nodes.size() << " nodes" << std::endl;

for (const auto& node : nodes) {
    std::cout << node.get_name() << " at "
              << node.get_address().host << ":"
              << node.get_address().port << std::endl;
}
```

### Asynchronous Retrieval

Get nodes without blocking:

```cpp
client.get_cluster_nodes_async([](ignite_result<std::vector<cluster_node>> result) {
    if (!result.has_error()) {
        auto nodes = std::move(result).value();
        std::cout << "Found " << nodes.size() << " nodes" << std::endl;
    } else {
        std::cerr << "Error: " << result.error().what_str() << std::endl;
    }
});
```

## Use Cases

### Node Selection for Compute

Select specific nodes for job execution:

```cpp
auto nodes = client.get_cluster_nodes();

// Execute on first node
if (!nodes.empty()) {
    auto target = job_target::node(nodes[0]);
    auto execution = client.get_compute().submit(target, descriptor, arg);
}

// Execute on any node
auto target = job_target::any_node(nodes);
auto execution = client.get_compute().submit(target, descriptor, arg);
```

### Finding Specific Nodes

Locate nodes by name:

```cpp
auto nodes = client.get_cluster_nodes();

auto it = std::find_if(nodes.begin(), nodes.end(),
    [](const auto& node) {
        return node.get_name() == "my-node-01";
    });

if (it != nodes.end()) {
    cluster_node target_node = *it;
    // Use node
}
```

### Cluster Monitoring

Monitor cluster size:

```cpp
auto nodes = client.get_cluster_nodes();
size_t cluster_size = nodes.size();

if (cluster_size < 3) {
    std::cerr << "Warning: Cluster has only " << cluster_size << " nodes" << std::endl;
}
```

Track node addresses:

```cpp
std::map<std::string, end_point> node_map;

for (const auto& node : nodes) {
    node_map[node.get_name()] = node.get_address();
}

// Check if specific node is available
if (node_map.find("my-node-01") != node_map.end()) {
    std::cout << "Node my-node-01 is online" << std::endl;
}
```

### Broadcasting to All Nodes

Execute jobs on all cluster nodes:

```cpp
auto nodes = client.get_cluster_nodes();
std::set<cluster_node> node_set(nodes.begin(), nodes.end());

auto target = broadcast_job_target::nodes(node_set);
auto broadcast = client.get_compute().submit_broadcast(target, descriptor, arg);

auto executions = broadcast.get_job_executions();
std::cout << "Broadcast to " << executions.size() << " nodes" << std::endl;
```

### Node Filtering

Filter nodes by criteria:

```cpp
auto nodes = client.get_cluster_nodes();

// Get nodes on specific host
std::vector<cluster_node> local_nodes;
std::copy_if(nodes.begin(), nodes.end(), std::back_inserter(local_nodes),
    [](const auto& node) {
        return node.get_address().host == "192.168.1.100";
    });

// Get nodes in port range
std::vector<cluster_node> dev_nodes;
std::copy_if(nodes.begin(), nodes.end(), std::back_inserter(dev_nodes),
    [](const auto& node) {
        return node.get_address().port >= 10800 && node.get_address().port < 10900;
    });
```

### Round-Robin Selection

Distribute work across nodes:

```cpp
auto nodes = client.get_cluster_nodes();
size_t current_index = 0;

for (const auto& task : tasks) {
    auto target = job_target::node(nodes[current_index]);
    compute.submit(target, descriptor, task);

    current_index = (current_index + 1) % nodes.size();
}
```

## Node Lifecycle

### Node Restart Impact

When a node restarts:

- Node ID changes to a new UUID
- Node name remains the same
- Network address typically remains the same

Use node names for stable node references across restarts.

### Topology Changes

The cluster topology may change between calls:

```cpp
// Initial topology
auto nodes1 = client.get_cluster_nodes();
size_t size1 = nodes1.size();

// Topology may change
std::this_thread::sleep_for(std::chrono::seconds(10));

// Updated topology
auto nodes2 = client.get_cluster_nodes();
size_t size2 = nodes2.size();

if (size2 != size1) {
    std::cout << "Topology changed: "
              << size1 << " -> " << size2 << " nodes" << std::endl;
}
```

Always retrieve fresh topology information before node-specific operations.

## Error Handling

### Network Errors

Handle connection failures:

```cpp
try {
    auto nodes = client.get_cluster_nodes();
} catch (const ignite_error& e) {
    std::cerr << "Failed to get cluster nodes: " << e.what_str() << std::endl;
}
```

With async operations:

```cpp
client.get_cluster_nodes_async([](ignite_result<std::vector<cluster_node>> result) {
    if (result.has_error()) {
        std::cerr << "Error: " << result.error().what_str() << std::endl;
    } else {
        auto nodes = std::move(result).value();
        // Use nodes
    }
});
```

### Empty Topology

Check for empty clusters:

```cpp
auto nodes = client.get_cluster_nodes();

if (nodes.empty()) {
    std::cerr << "Warning: No nodes available in cluster" << std::endl;
} else {
    // Proceed with operations
}
```

## Integration with Compute API

### Job Targeting

Use topology information for compute targeting:

```cpp
auto nodes = client.get_cluster_nodes();

// Target specific node
auto target = job_target::node(nodes[0]);

// Target any node from set
auto target = job_target::any_node(nodes);

// Broadcast to all nodes
std::set<cluster_node> node_set(nodes.begin(), nodes.end());
auto broadcast_target = broadcast_job_target::nodes(node_set);
```

### Node Affinity

Select nodes based on affinity:

```cpp
auto nodes = client.get_cluster_nodes();

// Find preferred nodes (example: local datacenter)
std::vector<cluster_node> preferred_nodes;
std::copy_if(nodes.begin(), nodes.end(), std::back_inserter(preferred_nodes),
    [](const auto& node) {
        return node.get_address().host.find("dc1") != std::string::npos;
    });

// Use preferred nodes for execution
if (!preferred_nodes.empty()) {
    auto target = job_target::any_node(preferred_nodes);
    compute.submit(target, descriptor, arg);
}
```

## Best Practices

### Cache Topology Information

Cache node information for short-lived operations:

```cpp
class compute_scheduler {
    std::vector<cluster_node> nodes_;
    std::chrono::steady_clock::time_point last_refresh_;
    std::chrono::seconds refresh_interval_{30};

public:
    void maybe_refresh_topology(ignite_client& client) {
        auto now = std::chrono::steady_clock::now();
        if (now - last_refresh_ > refresh_interval_) {
            nodes_ = client.get_cluster_nodes();
            last_refresh_ = now;
        }
    }

    std::vector<cluster_node> get_nodes() const {
        return nodes_;
    }
};
```

### Use Stable References

Prefer node names over IDs for persistent references:

```cpp
// Good: Use node name
std::string target_node_name = "my-node-01";
auto nodes = client.get_cluster_nodes();
auto it = std::find_if(nodes.begin(), nodes.end(),
    [&](const auto& n) { return n.get_name() == target_node_name; });

// Avoid: Using node ID (changes on restart)
uuid target_node_id = saved_id;  // May be stale after restart
```

### Handle Dynamic Topology

Account for nodes joining and leaving:

```cpp
void execute_with_fallback(ignite_client& client,
                           std::shared_ptr<job_descriptor> descriptor,
                           const binary_object& arg) {
    auto nodes = client.get_cluster_nodes();

    if (nodes.empty()) {
        throw ignite_error("No nodes available");
    }

    // Try first node
    try {
        auto target = job_target::node(nodes[0]);
        compute.submit(target, descriptor, arg);
    } catch (const ignite_error& e) {
        // Fallback to any available node
        auto target = job_target::any_node(nodes);
        compute.submit(target, descriptor, arg);
    }
}
```

## Reference

- [C++ API Documentation](https://ignite.apache.org/releases/ignite3/3.1.0/cppdoc/)
- [Compute API](./compute-api)
- [Client API](./client-api)
