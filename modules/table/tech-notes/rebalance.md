# How to read this doc
Every algorithm phase has the following main sections:
- Trigger - how current phase will be invoked
- Steps/Pseudocode - the main logical steps of the current phase
- Result (optional, if pseudocode provided) - events and system state changes, which this phase produces

# Rebalance algorithm
## Short algorithm description
  - Operations, which can trigger rebalance occurred:
    Write new baseline to metastore (effectively from 1 node in cluster)

    OR
    
    Write new replicas configuration number to table config (effectively from 1 node)
    
    OR
    
    Write new partition configuration number to table config (effectively from 1 node)
- Write new assignments intention to metastore (effectively from 1 node in cluster)
- Start new raft nodes. Initiate/update change peer request to raft group (effectively from 1 node per partition)
- Stop all redundant nodes. Change stable partition assignment to the new one and finish rebalance process.

## Operations, which can trigger rebalance
Three types of events can trigger the rebalance:
- API call of any special method like `org.apache.ignite.Ignite.setBaseline`, which will change baseline value in metastore (1 for all tables for now, but maybe it should be separate per table in future)
- Configuration change through `org.apache.ignite.configuration.schemas.table.TableChange.changeReplicas` produce metastore update event
- Configuration change through `org.apache.ignite.configuration.schemas.table.TableChange.changePartitions` produce metastore update event (IMPORTANT: this type of trigger has additional difficulties because of cross raft group data migration and it is out of scope of this document)

Result: So, one of three metastore keys will trigger rebalance:
```
<global>.baseline
<tableScope>.replicas
<tableScope>.partitions // out of scope
```
## Write new pending assignments
**Trigger**:
- Metastore event about change in `<global>.baseline`
- Metastore event about changes in `<tableScope>.replicas`

**Pseudocode**:
```
onBaselineEvent:
    for table in tableCfg.tables():
        for partition in table.partitions:
            <inline metastoreInvoke>
            
onReplicaNumberChange:
    with table as event.table:
        for partitoin in table.partitions:
            <inline metastoreInvoke>

metastoreInvoke: // atomic metastore call through multi-invoke api
    if empty(partition.change.trigger.revision) || partition.change.trigger.revision < event.revision:
        if empty(partition.assignments.pending) && partition.assignments.stable != calcPartAssighments():
            partition.assignments.pending = calcPartAssignments() 
            partition.change.trigger.revision = event.revision
        else:
            if partition.assignments.pending != calcPartAssignments
                partition.assignments.planned = calcPartAssignments()
                partition.change.trigger.revision = event.revision
            else
                remove(partition.assignments.planned)
    else:
        skip
```

## Start new raft nodes and initiate change peers
Trigger: Metastore event about new partition assignments received

Steps:
- On receive pending assignment updates every node, which listening the changes start all needed raft nodes
- After successful starts - check if current node is the leader of raft group and `changePeers(leaderTerm, peers)`. `changePeers` from old terms must be skipped.

Result:
- New needed raft nodes started
- Change peers state initiated/updated for every raft group

## When changePeers done inside the raft group - stop all redundant nodes
**Trigger**: When leader applied new Configuration with list of resulting peers `<applied peer>`, it calls `onRebalanceDone(<applied peers> -> closure)`

**Pseudocode**:
```
metastoreInvoke: \\ atomic
    partition.assignments.stable = appliedPeers
    if empty(partition.assignments.planned):
        partition.assignments.pending = empty
    else:
        partition.assignments.pending = partition.assignments.planned
```

Failover helpers (detailed failover scenarious must be developed in future)
- `onLeaderChanged()` - must be executed from the new leader when raft group changes the leader. Maybe we actually need to also check if a new lease is received - we need to investigate.
- `onChangePeersError()` - must be executed when any errors during changePeers occurred
- `onChangePeersCommitted(peers -> closure)` - must be executed with the list of new peers when changePeers has successfully done.

At the moment, this set of listeners seems to enough for restart rebalance and/or notify node's failover about fatal issues. Failover scenarios will be explained with more details during first phase of implementation.


## Cleanup redundant raft nodes
**Trigger**: Node receive update about partition stable assignments

**Steps**:
- Replace current raft client with new one, with appropriate peers
- Stop unneeded raft nodes

**Result**:
- Raft clients for new assignments refreshed
- Redundant raft nodes stopped

#Further optimisations:
# 'Smart' raft change peers:
Problem: current `changePeers` algorithm has the batch nature and catchup phase will be finished only when ALL peers of new topology ready. It can be a serious issue when user add chain of nodes and expects that nodes will be available asap. Any slow node will slow down whole rebalance process at all.


JRaft change peers algorithm must be improved to the following one:
- Leader must be available for adjusting the catchup list, if it is in the STAGE_CATCHING_UP stage when new change peers action called
- When node catchup done - leader must initiate 2-phase configuration applying for the current peers + done peer immediately, while over nodes is still catching up.
