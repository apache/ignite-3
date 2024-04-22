/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.placementdriver;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.event.EventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;

/**
 * Service that provides an ability to await and retrieve primary replicas for replication groups.
 *
 * <p>Notes: If during recovery, the component needs to perform actions depending on whether the primary replica for some replication group
 * is a local node, then it needs to use {@link #getPrimaryReplica(ReplicationGroupId, HybridTimestamp)}. Then compare the local node with
 * {@link ReplicaMeta#getLeaseholder()} and {@link ReplicaMeta#getLeaseholderId()} and make sure that it has not yet expired by
 * {@link ReplicaMeta#getExpirationTime()}. And only then can we consider that the local node is the primary replica for the requested
 * replication group.</p>
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-20646 Consider using CLOCK_SKEW unaware await/getPrimaryReplica()
public interface PlacementDriver extends EventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters> {
    /**
     * Returns a future for the primary replica for the specified replication group whose expiration time (the right border of the
     * corresponding lease interval) is greater than or equal to the (timestamp passed as a parameter - CLOCK_SKEW).
     * Please pay attention that there are no restriction on the lease start time (left border),
     * it can either be less or greater than or equal to proposed timestamp.
     * Given method will await for an appropriate primary replica appearance if there's no already existing one. If the current lease
     * is held by a node that is already not in a cluster, the future will be completed after the lease is transferred to another node.
     *
     * @param groupId Replication group id.
     * @param timestamp CLOCK_SKEW aware timestamp reference value.
     * @param timeout How long to wait before completing exceptionally with a TimeoutException, in units of unit.
     * @param unit A TimeUnit determining how to interpret the timeout parameter.
     * @return Primary replica future.
     * @throws PrimaryReplicaAwaitTimeoutException If primary replica await timed out.
     * @throws PrimaryReplicaAwaitException If primary replica await failed with any other reason except timeout.
     */
    CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    );

    /**
     * Temporary solution for awaiting {@link ReplicaMeta}. Waits for
     * {@link ReplicaMeta} for {@link org.apache.ignite.internal.replicator.TablePartitionId}
     * based on the {@link ZonePartitionId#tableId()}.
     *
     * @param groupId Replication group id.
     * @param timestamp CLOCK_SKEW aware timestamp reference value.
     * @param timeout How long to wait before completing exceptionally with a TimeoutException, in units of unit.
     * @param unit A TimeUnit determining how to interpret the timeout parameter.
     * @return Primary replica future.
     * @throws PrimaryReplicaAwaitTimeoutException If primary replica await timed out.
     * @throws PrimaryReplicaAwaitException If primary replica await failed with any other reason except timeout.
     */
    @Deprecated
    default CompletableFuture<ReplicaMeta> awaitPrimaryReplicaForTable(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        return awaitPrimaryReplica(groupId, timestamp, timeout, unit);
    }

    /**
     * Same as {@link #awaitPrimaryReplica(ReplicationGroupId, HybridTimestamp, long, TimeUnit)} despite the fact that given method await
     * logic is bounded. It will wait for a primary replica for a reasonable period of time, and complete a future with null if a matching
     * lease isn't found. Generally speaking reasonable here means enough for distribution across cluster nodes.
     *
     * @param replicationGroupId Replication group id.
     * @param timestamp CLOCK_SKEW aware timestamp reference value.
     * @return Primary replica future.
     */
    CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp);

    /**
     * Returns a future that completes when all expiration event {@link PrimaryReplicaEvent#PRIMARY_REPLICA_EXPIRED} listeners of previous
     * primary complete.
     *
     * @param grpId Replication group id.
     * @return Future.
     */
    CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId);

    /**
     * Gets a cached lease by a zone replication group.
     *
     * @param grpId Replication group id.
     * @return Lease or {@code null}.
     */
    ReplicaMeta getLeaseMeta(ReplicationGroupId grpId);

    /**
     * Tries to update the lease in order to include the new subgroup. The set of groups will be added to the set of lease subgroups
     * ({@link ReplicaMeta#subgroups()}) for the specific lease determined by the zone id.
     * TODO: IGNITE-20362 When replicas are started by zone, the method is removed.
     *
     * @param zoneId Zone id.
     * @param enlistmentConsistencyToken Lease token.
     * @param subGrps Table ids.
     * @return Future to complete.
     */
    CompletableFuture<Void> addSubgroups(ZonePartitionId zoneId, Long enlistmentConsistencyToken, Set<ReplicationGroupId> subGrps);
}
