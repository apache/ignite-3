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

package org.apache.ignite.internal.partition.replicator;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;

/**
 * Logic related to replica primacy checks.
 */
public class ReplicaPrimacyEngine {
    private final LeasePlacementDriver placementDriver;
    private final ClockService clockService;
    private final ReplicationGroupId replicationGroupId;
    private final InternalClusterNode localNode;

    /** Constructor. */
    public ReplicaPrimacyEngine(
            LeasePlacementDriver placementDriver,
            ClockService clockService,
            ReplicationGroupId replicationGroupId,
            InternalClusterNode localNode
    ) {
        this.placementDriver = placementDriver;
        this.clockService = clockService;
        this.replicationGroupId = replicationGroupId;
        this.localNode = localNode;
    }

    /**
     * Validates replica primacy.
     *
     * <ul>
     *     <li>For {@link PrimaryReplicaRequest}s, ensures that the primary replica is known, that it is hosted on this node,
     *     that it has not expired and the request was sent while the same replica was primary. If anything of the above is violated, the
     *     future is completed with a {@link PrimaryReplicaMissException}.</li>
     *     <li>For {@link ReadOnlyReplicaRequest}s and {@link ReplicaSafeTimeSyncRequest}s, detects whether this node is/was a primary
     *     at the corresponding timestamp.</li>
     * </ul>
     *
     * @param request Replica request.
     */
    public CompletableFuture<ReplicaPrimacy> validatePrimacy(ReplicaRequest request) {
        HybridTimestamp now = clockService.current();

        if (request instanceof PrimaryReplicaRequest) {
            PrimaryReplicaRequest primaryReplicaRequest = (PrimaryReplicaRequest) request;
            return ensureReplicaIsPrimary(primaryReplicaRequest, now);
        } else if (request instanceof ReadOnlyReplicaRequest) {
            return isLocalNodePrimaryReplicaAt(now);
        } else if (request instanceof ReplicaSafeTimeSyncRequest) {
            return isLocalNodePrimaryReplicaAt(now);
        } else {
            return completedFuture(ReplicaPrimacy.empty());
        }
    }

    private CompletableFuture<ReplicaPrimacy> ensureReplicaIsPrimary(
            PrimaryReplicaRequest primaryReplicaRequest,
            HybridTimestamp now
    ) {
        Long enlistmentConsistencyToken = primaryReplicaRequest.enlistmentConsistencyToken();

        Function<ReplicaMeta, ReplicaPrimacy> validateClo = primaryReplicaMeta -> validateReplicaPrimacy(
                now,
                primaryReplicaMeta,
                enlistmentConsistencyToken
        );

        ReplicaMeta meta = placementDriver.getCurrentPrimaryReplica(replicationGroupId, now);

        if (meta != null) {
            try {
                return completedFuture(validateClo.apply(meta));
            } catch (Exception e) {
                return failedFuture(e);
            }
        }

        return placementDriver.getPrimaryReplica(replicationGroupId, now).thenApply(validateClo);
    }

    private ReplicaPrimacy validateReplicaPrimacy(HybridTimestamp now, ReplicaMeta primaryReplicaMeta, Long enlistmentConsistencyToken) {
        if (primaryReplicaMeta == null) {
            throw new PrimaryReplicaMissException(
                    localNode.name(),
                    null,
                    localNode.id(),
                    null,
                    enlistmentConsistencyToken,
                    null,
                    null
            );
        }

        long currentEnlistmentConsistencyToken = primaryReplicaMeta.getStartTime().longValue();

        if (enlistmentConsistencyToken != currentEnlistmentConsistencyToken
                || clockService.before(primaryReplicaMeta.getExpirationTime(), now)
                || !isLocalPeer(primaryReplicaMeta.getLeaseholderId())
        ) {
            throw new PrimaryReplicaMissException(
                    localNode.name(),
                    primaryReplicaMeta.getLeaseholder(),
                    localNode.id(),
                    primaryReplicaMeta.getLeaseholderId(),
                    enlistmentConsistencyToken,
                    currentEnlistmentConsistencyToken,
                    null);
        }

        return ReplicaPrimacy.forPrimaryReplicaRequest(primaryReplicaMeta.getStartTime().longValue());
    }

    private CompletableFuture<ReplicaPrimacy> isLocalNodePrimaryReplicaAt(HybridTimestamp timestamp) {
        return placementDriver.getPrimaryReplica(replicationGroupId, timestamp)
                .thenApply(primaryReplica -> ReplicaPrimacy.forIsPrimary(
                        // TODO: https://issues.apache.org/jira/browse/IGNITE-24714 - should we also check lease expiration?
                        primaryReplica != null && isLocalPeer(primaryReplica.getLeaseholderId())
                ));
    }

    private boolean isLocalPeer(UUID nodeId) {
        return localNode.id().equals(nodeId);
    }

    /**
     * Checks whether the token still matches the current primary replica.
     *
     * @param suspectedEnlistmentConsistencyToken Enlistment consistency token that is going to be checked.
     */
    public boolean tokenStillMatchesPrimary(long suspectedEnlistmentConsistencyToken) {
        HybridTimestamp currentTime = clockService.current();

        ReplicaMeta meta = placementDriver.getCurrentPrimaryReplica(replicationGroupId, currentTime);

        return meta != null
                && isLocalPeer(meta.getLeaseholderId())
                && clockService.before(currentTime, meta.getExpirationTime())
                && suspectedEnlistmentConsistencyToken == meta.getStartTime().longValue();
    }
}
