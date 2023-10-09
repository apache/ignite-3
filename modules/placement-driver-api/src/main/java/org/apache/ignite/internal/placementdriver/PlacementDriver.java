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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.event.EventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * Service that provides an ability to await and retrieve primary replicas for replication groups.
 */
public interface PlacementDriver extends EventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters> {
    /**
     * Returns a future for the primary replica for the specified replication group whose expiration time (the right border of the
     * corresponding lease interval) is greater than or equal to the timestamp passed as a parameter. Please pay attention that there are
     * no restriction on the lease start time (left border), it can either be less or greater than or equal to proposed timestamp.
     * Given method will await for an appropriate primary replica appearance if there's no already existing one.
     *
     * @param groupId Replication group id.
     * @param timestamp Timestamp reference value.
     * @param timeout How long to wait before completing exceptionally with a TimeoutException, in units of unit.
     * @param unit A TimeUnit determining how to interpret the timeout parameter.
     * @return Primary replica future.
     */
    CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    );

    /**
     * Same as {@link #awaitPrimaryReplica(ReplicationGroupId, HybridTimestamp, long, TimeUnit)} despite the fact that given method await
     * logic is bounded. It will wait for a primary replica for a reasonable period of time, and complete a future with null if a matching
     * lease isn't found. Generally speaking reasonable here means enough for distribution across cluster nodes.
     *
     * @param replicationGroupId Replication group id.
     * @param timestamp Timestamp reference value.
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
}
