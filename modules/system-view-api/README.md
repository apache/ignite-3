# System view API module

System Views, also known as virtual tables, is a powerful tool that is meant to expose the system's state and provide real-time
insights into its various components.

This module provides an abstraction and core interfaces for the Ignite system views.

## Base components description

* `SystemViews` provides static factory methods for system view builders.
* `SystemView` is a base class for system view definitions.
  * `ClusterSystemView` represents cluster-wide view definition. This type of view is used to expose the data from a common source, 
    like distributed meta storage, table, or anything, so every node will provide the same copy of dataset.
    Use `SystemViews.clusterViewBuilder()` to create this type of view.
  * `NodeSystemView` represents node view definition. This type of view is used to expose data that is unique to a particular node, 
    and can be acquired only on the node itself. Note that a special column containing local node name always implicitly added to 
    the local view.  The name of this column must be set explicitly using the `nodeNameColumnAlias` builder method.
    Use `SystemViews.nodeViewBuilder()` to create this type of view.
* `SystemViewProvider` denotes a component that provides system views.
* `SystemViewManager` is responsible for views registration in the cluster.

## Basic concepts

* Component that want to expose system views must implement `SystemViewProvider` interface.
* All system views must be registered during node startup. The `IgniteImpl` constructor is currently used as a starting point to do this.
* All system views are reside in `SYSTEM` schema.
* From SQL perspective, a system view is a virtual immutable table.
* System views don't support transactions. That is, regardless of whether a transaction has started or not, each query 
  to the system view may return a different data set. This data set is provided by a specified system view data provider 
  that does not have integration with the transaction protocol.
* The view name and its columns are not case sensitive and must contain only alphanumeric characters with underscores and 
  begin with a letter. There is no way to set the name to be case sensitive.
* The system view named `SYSTEM_VIEWS` may be used to obtain a list of all system views registered in the cluster.

## System view registration example

To register a system view, you need to complete three steps.

### Step 1. Mark component as `SystemViewProvider`

Any component that wants to register system views must implement `SystemViewProvider` interface with single
`systemViews()` method, which simply returns a list of system views. The next step provides an example of the implementation of this method.

### Step 2. Build a view.
For example we want to add a view representing actual cluster topology.
Since this information can be obtained on any node in the cluster this should be a `ClusterSystemView`,
but to understand the difference between cluster-wide and local view, let's also add exactly the same local view (`NodeSystemView`). 
The only difference is that we have to set `nodeNameColumnAlias` for the local view.

Please note that we explicitly specify the generic type of the column. It is a good practice to detect the column
type mismatch at compile time.

```java
@Override
public List<SystemView<?>> systemViews() {
    SystemView<ClusterNode> clusterWideView = SystemViews.<ClusterNode>clusterViewBuilder()
            .name("TOPOLOGY_SNAPSHOT")
            .<String>addColumn("NAME", NativeTypes.STRING, ClusterNode::name)
            .<String>addColumn("HOST", NativeTypes.STRING, node -> node.address().host())
            .<Integer>addColumn("PORT", NativeTypes.INT32, node -> node.address().port())
            .dataProvider(SubscriptionUtils.fromIterable(() -> ignite.clusterNodes().iterator()))
            .build();

    SystemView<ClusterNode> localView = SystemViews.<ClusterNode>nodeViewBuilder()
            .name("TOPOLOGY_SNAPSHOT_LOCAL_VIEW")
            .nodeNameColumnAlias("VIEW_DATA_SOURCE")
            .<String>addColumn("NAME", NativeTypes.STRING, ClusterNode::name)
            .<String>addColumn("HOST", NativeTypes.STRING, node -> node.address().host())
            .<Integer>addColumn("PORT", NativeTypes.INT32, node -> node.address().port())
            .dataProvider(SubscriptionUtils.fromIterable(() -> ignite.iterator()))
            .build();
            
    return List.of(clusterWideView, localView);
}
```

### Step 3. Register your provider in views manager

Component `SystemViewManager` is responsible for views registration in the system.
Currently there is no automatic registration of system components views, so we need to do it manually.
```java
MyComponent component = new MyComponent();
systemViewManager.register(component);
```
Please note that this must be done before `systemViewManager` starts.

### Check the result

After these steps, we can start the cluster and start working with the system views we just created.

Note that the list of registered views can be obtained using the `SYSTEM_VIEWS` view.

```sql
select * from system.system_views order by id
```
| ID | SCHEMA | NAME                         | TYPE    |
|----|--------|------------------------------|---------|
| 3  | SYSTEM | SYSTEM_VIEWS                 | CLUSTER |
| 4  | SYSTEM | SYSTEM_VIEW_COLUMNS          | CLUSTER |
| 5  | SYSTEM | TOPOLOGY_SNAPSHOT            | CLUSTER |
| 6  | SYSTEM | TOPOLOGY_SNAPSHOT_LOCAL_VIEW | NODE    |

#### Check cluster-wide system view

```sql
select * from system.topology_snapshot
```
| host     | address       | port |
|----------|---------------|------|
| isvt_n_0 | 192.168.0.103 | 3344 |
| isvt_n_1 | 192.168.0.103 | 3345 |
| isvt_n_2 | 192.168.0.103 | 3346 |

For a cluster-wide system view, the data is retrieved on one of the nodes on which the view was registered.

#### Check local node system view

```sql
select * from system.topology_snapshot_local_view
```
| view_data_source | host      | address       | port |
|------------------|-----------|---------------|------|
| isvt_n_0         | isvt_n_0  | 192.168.0.103 | 3344 |
| isvt_n_0         | isvt_n_1  | 192.168.0.103 | 3345 |
| isvt_n_0         | isvt_n_2  | 192.168.0.103 | 3346 |
| isvt_n_2         | isvt_n_0  | 192.168.0.103 | 3344 |
| isvt_n_2         | isvt_n_1  | 192.168.0.103 | 3345 |
| isvt_n_2         | isvt_n_2  | 192.168.0.103 | 3346 |
| isvt_n_1         | isvt_n_0  | 192.168.0.103 | 3344 |
| isvt_n_1         | isvt_n_1  | 192.168.0.103 | 3345 |
| isvt_n_1         | isvt_n_2  | 192.168.0.103 | 3346 |

Since the local cluster includes 3 nodes, the data is duplicated three times.
Because for a local view, data is retrieved on **each node** where the view is registered.