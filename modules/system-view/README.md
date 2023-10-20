# System view module

This module contains system views manager implementation.
The document provides some low-level implementation details that may be of interest to the maintainers of this module.
For information about using the system views API, see the `system-views-api` module documentation.

## System views manager

### Responsibility
1. Registers system views in the cluster.
2. Monitors topology changes and serves as a source of information about which nodes owns a particular system view.
3. Provides system view data sources for the query engine.

### Registration

Registration of system views occurs during node startup.
The system view cannot be registered after the system views manager is started.

System views registration in the cluster involve next steps, which are performed during node startup.

1. Components register system views using system views manager's `register` method.
2. System views manager starts.
   * Executes the `CreateSystemViewCommand` catalog command. This command adds non-existent system views into the catalog.
   * Collects a list of names of registered system views as a system node attribute.
3. Raft group service completes joining a node to the cluster by sending a `ClusterNodeMessage`. 
   with system attributes containing the names of locally residing system views.
4. Each node updates logical nodes in logical topology snapshot with received system attributes.  
   This way, any node in the cluster knows which nodes have which system views.

### Execution

From the query engine perspective the system view is an unmodifiable table that is located in `SYSTEM` schema.
System views don't support transactions. With each query, the sql engine requests data from the system view data provider, 
which does not have transaction integration.  That is, regardless of whether a transaction has started or not, each query 
to the system view may return a different set of data.

Cluster-wide view data can be obtained on any node so the view of this type has a single distribution.

Local node view data must be obtained on every node where view is registered, so it uses special identity-distribution, 
that calculates destinations based on a value of the row field.
