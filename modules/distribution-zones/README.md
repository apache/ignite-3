This module provides implementations for the Distribution Zones module.

## Brief overview
Data partitioning in Apache Ignite is controlled by the affinity function that determines the mapping both between keys and partitions 
and partitions and nodes. Specifying an affinity function along with replica factor and partitions count sometimes is not enough, 
meaning that explicit fine grained tuning is required in order to control what data goes where. 
Distribution zones provides aforementioned configuration possibilities that eventually makes it possible to achieve following goals:

+ The ability to trigger data rebalance upon adding and removing cluster nodes.
+ The ability to delay rebalance until the topology stabilizes.
+ The ability to conveniently specify data distribution adjustment rules for tables.

## Threading
Every Ignite node has one scheduled thread pool executor and thread naming format: (`%{consistentId}%dst-zones-scheduler`)

This thread pool is responsible for the scheduling tasks that adds or removes nodes from zone's data nodes field in meta storage. 
This process is called data nodes' scale up or scale down. 
Every intent for scale up and scale down is scheduled in the thread pool with the timeout, that is presented in the configuration of
a zone. After timeout, new data nodes of a zone are propagated to the meta storage. 
If a new intent is appeared at the moment, when old intent has not been started yet, 
then old intent will be canceled and replaced with a new one.

## Access to the list of zones
To check the existing list of zones in the cluster, use zones' system view

```sql
SELECT * FROM SYSTEM.ZONES
```

