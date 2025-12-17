---
id: disaster-recovery-system-groups
title: Disaster Recovery for System Groups
sidebar_label: System Groups Recovery
---

An Apache Ignite cluster includes two system RAFT groups, both of which are essential for the cluster's normal operation:

- [Cluster Management Group (CMG)](/3.1.0/configure-and-operate/operations/lifecycle#cluster-management-group)
- [Metastorage Group (MG)](/3.1.0/configure-and-operate/operations/lifecycle#cluster-metastorage-group)

You perform _disaster recovery_ operations on _system RAFT groups_ to recover permanent _majority loss_. When a system RAFT group loses majority, it becomes unavailable. When CMG is unavailable, the cluster itself remains available with limitations: it can still process most of the operations, but it cannot join new nodes, start/restart existing nodes, and start building new indexes. When MG is unavailable, the cluster becomes unusable; it cannot handle even GET/PUT/SQL requests.

:::note
Disaster recovery for [data partitions](/3.1.0/configure-and-operate/operations/disaster-recovery-partitions) is described in a separate page.
:::

You see that the majority has been lost in cluster logs in the console or in the [rotated log files](https://en.wikipedia.org/wiki/Log_rotation). When a RAFT group becomes unavailable, the logs would show something like `Send with retry timed out [retryCount = 11, groupId = cmg_group].` or `Send with retry timed out [retryCount = 11, groupId = metastorage_group].`.

An indicator that CMG is down is when a node does not start after a `restart` command. This is reflected in the log as `Local CMG state recovered, starting the CMG`, not followed by `Successfully joined the cluster`.

If a node tries to start when CMG is available, but MG is not, the log shows `Metastorage info on start` not followed by `Performing MetaStorage recovery`.

:::warning
Exercise caution when applying disaster recovery commands to system node groups. These commands might result in _split brain_ (split your cluster into two clusters). Use the recovery commands as the last resort, only if the majority for CMG/MG is lost permanently.
:::

## Cluster Management Group

If CMG loses majority:

1. Restart CMG nodes to restore the lost majority.
2. If the above fails, forcefully assign a new majority using the following [CLI command](/3.1.0/tools/cli-commands) (manually or via REST): `recovery cluster reset --url=<node-url> --cluster-management-group=<new-cmg-nodes>`.

The command is sent to the node indicated by the `--url` parameter, which must belong to the `new-cmg-nodes` RAFT group. This node becomes the _Repair Conductor_, and it initiates the `reset` procedure.

The above procedure might fail for the following reasons:

- Some of the nodes specified in `new-cmg-nodes` are not in the physical topology.
- The Repair Conductor does not have all the information it needs to start the procedure.

3. If some nodes were down or were unavailable due to a network partition (and hence did not participate in the repair):
   1. Start these nodes (or restore network connectivity and restart them).
   2. Migrate these nodes to the repaired cluster using the following [CLI command](/3.1.0/tools/cli-commands) (manually or via REST): `recovery cluster migrate --old-cluster-url=<url-of-old-cluster-node> --new-cluster-url=<url-of-new-cluster-node>`.

## Metastorage Group

If MG loses majority:

1. Restart MG nodes (or at least their RAFT nodes inside Apache Ignite nodes).
2. If the above fails:
   1. Make sure that every node that could be started had started and joined the cluster.
   2. Forcefully assign a new majority using the following [CLI command](/3.1.0/tools/cli-commands) (manually or via REST): `recovery cluster reset --url=<existing-node-url> [--cluster-management-group=<new-cmg-nodes>] --metastorage-replication-factor=N`.

`N` is the requested number of the voting RAFT nodes in the MG after repair. If you omit `--cluster-management-group`, the command takes the current CMG voting members set from the CMG leader; if CMG is not available, the command fails.

The command is sent to the node specified by `--url`. This node becomes the _Repair Conductor_, and it initiates the `reset` procedure.

If the Repair Conductor fails to repair MG, the procedure has to be repeated manually (there is no failover).

3. If some nodes were down or were unavailable due to a network partition (and hence did not participate in the repair):
   1. Start these nodes (or restore network connectivity and restart them).
   2. Migrate these nodes to the repaired cluster using the following [CLI command](/3.1.0/tools/cli-commands) (manually or via REST): `recovery cluster migrate --old-cluster-url=<url-of-old-cluster-node> --new-cluster-url=<url-of-new-cluster-node>`.

:::note
If one of the nodes has a revision in its metastorage that is not present on any of the nodes that had participated in the repair, this means that its metastorage has diverged from the MG leader's metastorage. Such a node will not be allowed to join the cluster. Its startup will fail with `MetastorageDivergedException (error code META-7)`. Remove the data from the above metastorage-divergent node and have it join the cluster as a blank node.
:::
