# Introduction
TBD

# Rebalance triggers
- Table configuration changes: number of partitions, number of replicas
- Logical topology changes: nodes appearing/dissappearing
- Distribution zone changes
- In future: uneven datanode load distribution (cpu, io, memory)

Different rebalance causes can have concurrent requests for rebalance and we must have the way linearize them and prepare optimal plan for received triggers.

Before diving into the abyss, let's review simple example to understand the main issues of a rebalance scope.

## Example of triggers' races

Let's take:
- Topology with 3 nodes A, B, C, D
- Table t1 with 1 partition p1 on (A, B) with replica factor 2

And imagine:
- t1 configuration updated with  replica factor 3 and rebalance for p1 started (A, B) -> (A, B, C)
- While rebalance is in progress, node C leave the logical topology, so, rebalance failed and retried again and again with the old task (A, B) -> (A, B, C), but in general new available configuration should be (A, B) -> (A, B, D). So, which component should be aware about this type of failure and can produce the new rebalance task according to new knowledges? In general this component can cancel all outdated rebalance tasks, which consists of leaved node. And as quicker it will do it, the more chances to stabilize the cluster before the new node failures, for example.

It looks like we need to have a single component, which prepare rebalance schedules and updates it, when needed. This coordinator must have enough power and vision to calculate the optimal cross-partions and even cross-table plans.

To be honest, it can be not the one component, but the group of distributed algrorithms around general distributed storage (like metastorage). But this approach will have exponent complexity with more and more triggers for rebalance. Current rebalance design shows us, that even for: 1 simple trigger (replication number changes) + cancellation + in-memory cluster needs; it is not easy to prepare the simple correct mechanism.

# Rebalance component
According to the previous chapter we need a single (but distiributed) component (let's name it RebalanceCoordinator) to coordinate the optimal schedule for all received rebalance triggers at the moment.

In general, different approaches available:
- Create the new one raft group and use it's leader as a RebalanceCoordinator
- Use one of the current groups: metastorage group, CMG group

**Subtotals: Rebalance coordinator must be the one per cluster, but distributed across some nodes component.**

## Choose the right one
- A separate RAFT group is an option, but it looks like it will bring additional cluster configuration complexity: Which nodes should be in this topology? How will users configure it and how to explain this?
- From the already exsisence groups, the CMG group looks suitable for this case both by name semantic and by the current functions:
    - It has the separate state machine, which we can use for storing the rebalance schedule
    - It already has requirements for proper user configuration
    - We can use it even for rebalance metastorage nodes

**Subtotals: RebalanceCoordinator can be a part of the current CMG leader.**

## Describe a data's flow between RebalanceCoordinator and partition groups
At first, we need to create as simple as possible data's flow between the RebalanceCoordinator and partition groups to handle the issues, which connected with:
- Partition's rebalance's fails
- Drop the rebalance process after leader re-election inside partition group (if rebalance process was in STAGE_CATCHING_UP)

The main idea of this flow - new rebalance tasks goes only from RebalanceCoordinator to partition group. At the same time, partition groups report about failes/completes and current progress (optional) to RebalanceCoordiator.

In the pseudo event-based oop-like language:
```
RebalanceCoordinator:
    onNewTrigger:
        calculateNewPlan()
        pushPartitionPlans()

    onNewParitionLeaderElectedReceived:
        pushCurrentPartitionTask()

    onErrorReceived:
        pushRetryIfNeeded() || pushRecalculatedPlan() || ... // we need to work out this cases further


PartitionRaftGroupListener:
    onLeaderElected:
        pushNewLeaderEventToRebalanceCoordinator()

    onReconfigurationComplete:
        pushResultsToRebalanceCoordinator()

    onError:
        pushErrorDetailsToRebalanceCoordinator()

PrimaryReplicaRebalanceListener:
    onNewPlanReceived:
        cancelCurrentTask()
        startNewRebalance()

```

So, in the new design - partition listeners aren't trying to prepare the new rebalance, make desisions about retries/cancelation and etc. Partition group just report about the results of rebalance to RebalanceCoordinator.

# RebalanceCoordinator operation details
This and all further aspects TBD
