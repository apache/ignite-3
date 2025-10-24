---
title: Network API
id: network-api
sidebar_position: 7
---

# Network API

The Network API provides information about cluster topology and active client connections. Use this API to discover cluster nodes, inspect connection status, and understand cluster composition.

## Key Concepts

Cluster nodes represent individual server instances in the Ignite cluster. Each node has a unique identifier and network address. The client maintains connections to cluster nodes and distributes operations across them.

### Node Identity

Nodes have two identifiers. The node ID changes after restart and uniquely identifies the current node instance. The node name (consistent ID) persists across restarts and identifies the node permanently.

### Connection Management

The client automatically manages connections to cluster nodes. Query active connections to understand which nodes the client is currently connected to and inspect connection properties like SSL configuration.

## Usage Examples

### Getting Cluster Nodes

```csharp
var client = await IgniteClient.StartAsync(configuration);

// Get all cluster nodes
var nodes = await client.GetClusterNodesAsync();

foreach (var node in nodes)
{
    Console.WriteLine($"Node: {node.Name}");
    Console.WriteLine($"  ID: {node.Id}");
    Console.WriteLine($"  Address: {node.Address}");
}
```

### Inspecting Active Connections

```csharp
var connections = client.GetConnections();

Console.WriteLine($"Active connections: {connections.Count}");

foreach (var conn in connections)
{
    Console.WriteLine($"Connected to: {conn.Node.Name}");
    Console.WriteLine($"  Node ID: {conn.Node.Id}");
    Console.WriteLine($"  Address: {conn.Node.Address}");

    if (conn.SslInfo != null)
    {
        Console.WriteLine($"  SSL: Enabled");
        Console.WriteLine($"  Protocol: {conn.SslInfo.SslProtocol}");
        Console.WriteLine($"  Cipher: {conn.SslInfo.NegotiatedCipherSuiteName}");
    }
}
```

### Finding Specific Nodes

```csharp
var nodes = await client.GetClusterNodesAsync();

// Find by name
var targetNode = nodes.FirstOrDefault(n => n.Name == "node-01");
if (targetNode != null)
{
    Console.WriteLine($"Found node: {targetNode.Name} at {targetNode.Address}");
}

// Find by ID
var nodeId = Guid.Parse("550e8400-e29b-41d4-a716-446655440000");
var nodeById = nodes.FirstOrDefault(n => n.Id == nodeId);
```

### Monitoring Connection Health

```csharp
var checkInterval = TimeSpan.FromSeconds(30);

while (true)
{
    var connections = client.GetConnections();

    if (connections.Count == 0)
    {
        Console.WriteLine("WARNING: No active connections!");
    }
    else
    {
        Console.WriteLine($"Connected to {connections.Count} nodes:");
        foreach (var conn in connections)
        {
            Console.WriteLine($"  - {conn.Node.Name}");
        }
    }

    await Task.Delay(checkInterval);
}
```

### Using Node Information for Job Targeting

```csharp
var nodes = await client.GetClusterNodesAsync();
var compute = client.Compute;

// Target specific node by name
var targetNode = nodes.FirstOrDefault(n => n.Name.StartsWith("compute"));
if (targetNode != null)
{
    var jobTarget = JobTarget.Node(targetNode);
    var execution = await compute.SubmitAsync(
        jobTarget, jobDescriptor, "input");
    var result = await execution.GetResultAsync();
}

// Target node by index (round-robin)
var nodeIndex = DateTime.UtcNow.Ticks % nodes.Count;
var selectedNode = nodes[(int)nodeIndex];
```

### Connection Status Check

```csharp
public async Task<bool> IsConnectedToCluster(IIgniteClient client)
{
    try
    {
        var connections = client.GetConnections();
        var nodes = await client.GetClusterNodesAsync();

        return connections.Count > 0 && nodes.Count > 0;
    }
    catch (Exception)
    {
        return false;
    }
}
```

### SSL Connection Information

```csharp
var connections = client.GetConnections();

foreach (var conn in connections)
{
    if (conn.SslInfo != null)
    {
        Console.WriteLine($"Node: {conn.Node.Name}");
        Console.WriteLine($"  SSL Protocol: {conn.SslInfo.SslProtocol}");
        Console.WriteLine($"  Cipher Suite: {conn.SslInfo.NegotiatedCipherSuiteName}");
        Console.WriteLine($"  Target Host: {conn.SslInfo.TargetHostName}");
        Console.WriteLine($"  Mutually Authenticated: {conn.SslInfo.IsMutuallyAuthenticated}");

        var localCert = conn.SslInfo.LocalCertificate;
        var remoteCert = conn.SslInfo.RemoteCertificate;

        if (localCert != null)
        {
            Console.WriteLine($"  Local Certificate: {localCert.Subject}");
        }

        if (remoteCert != null)
        {
            Console.WriteLine($"  Remote Certificate: {remoteCert.Subject}");
            Console.WriteLine($"  Valid Until: {remoteCert.GetExpirationDateString()}");
        }
    }
    else
    {
        Console.WriteLine($"Node: {conn.Node.Name} (unencrypted)");
    }
}
```

### Cluster Size Monitoring

```csharp
public class ClusterMonitor
{
    private readonly IIgniteClient _client;
    private int _lastKnownSize;

    public ClusterMonitor(IIgniteClient client)
    {
        _client = client;
    }

    public async Task MonitorAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var nodes = await _client.GetClusterNodesAsync();
            var currentSize = nodes.Count;

            if (currentSize != _lastKnownSize)
            {
                if (_lastKnownSize > 0)
                {
                    if (currentSize > _lastKnownSize)
                    {
                        Console.WriteLine($"Cluster grew: {_lastKnownSize} -> {currentSize} nodes");
                    }
                    else
                    {
                        Console.WriteLine($"Cluster shrunk: {_lastKnownSize} -> {currentSize} nodes");
                    }
                }

                _lastKnownSize = currentSize;
            }

            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
        }
    }
}
```

### Node Address Parsing

```csharp
var nodes = await client.GetClusterNodesAsync();

foreach (var node in nodes)
{
    var address = node.Address;

    if (address is IPEndPoint ipEndPoint)
    {
        Console.WriteLine($"Node: {node.Name}");
        Console.WriteLine($"  IP: {ipEndPoint.Address}");
        Console.WriteLine($"  Port: {ipEndPoint.Port}");
    }
    else if (address is DnsEndPoint dnsEndPoint)
    {
        Console.WriteLine($"Node: {node.Name}");
        Console.WriteLine($"  Host: {dnsEndPoint.Host}");
        Console.WriteLine($"  Port: {dnsEndPoint.Port}");
    }
}
```

## Reference

### IClusterNode Interface

Properties:

- **Id** - Unique node identifier (Guid) that changes after node restart
- **Name** - Consistent node name that persists across restarts
- **Address** - Network endpoint (IPEndPoint or DnsEndPoint)

The node ID is unique to the current node instance and changes when the node restarts. The node name remains consistent across restarts and serves as a stable identifier for the node.

### IConnectionInfo Interface

Properties:

- **Node** - The cluster node this connection targets
- **SslInfo** - SSL connection details (null if SSL not enabled)

Connection info describes an active client connection to a cluster node. The client may maintain multiple connections to different nodes simultaneously.

### ISslInfo Interface

Properties:

- **SslProtocol** - SSL/TLS protocol version (e.g., Tls12, Tls13)
- **NegotiatedCipherSuiteName** - Cipher suite negotiated for the connection
- **TargetHostName** - Server hostname used for certificate validation
- **IsMutuallyAuthenticated** - Whether both client and server are authenticated
- **LocalCertificate** - Client certificate (if provided)
- **RemoteCertificate** - Server certificate

SSL information is only available when SSL is configured through IgniteClientConfiguration.SslStreamFactory. When SSL is not enabled, IConnectionInfo.SslInfo returns null.

### IIgniteClient Methods

Node discovery:

- **GetClusterNodesAsync()** - Get all cluster nodes

Connection inspection:

- **GetConnections()** - Get active client connections to cluster nodes

### Best Practices

**Cache node lists** when possible. Cluster topology changes infrequently, so repeated calls to GetClusterNodesAsync may be unnecessary.

**Use node names for stable targeting**. Node IDs change on restart, but node names persist. Use names when you need consistent targeting across node restarts.

**Monitor connection count** to detect connectivity issues. A sudden drop in active connections may indicate network problems or node failures.

**Check SSL configuration** in production. Verify SSL is properly configured by inspecting ISslInfo properties to ensure connections are encrypted.

**Handle node changes gracefully**. Cluster topology can change as nodes join or leave. Design applications to adapt to topology changes without manual intervention.

**Use connection info for diagnostics**. Connection details help troubleshoot network issues, SSL problems, and load distribution across nodes.
