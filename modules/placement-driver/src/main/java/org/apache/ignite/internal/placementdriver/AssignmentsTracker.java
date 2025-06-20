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

import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractTablePartitionId;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.extractZonePartitionId;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * The class tracks assignment of all replication groups.
 */
public class AssignmentsTracker implements AssignmentsPlacementDriver {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(AssignmentsTracker.class);

    // TODO Not sure whether it should be instantiated here or propagated from PDM.
    // TODO Use it on stop, etc.
    /** Busy lock to linearize service public API calls and service stop. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Meta storage manager. */
    private final MetaStorageManager msManager;

    private final FailureProcessor failureProcessor;

    private final NodeProperties nodeProperties;

    /** Map replication group id to stable assignment nodes. */
    private final Map<ReplicationGroupId, TokenizedAssignments> groupStableAssignments;

    /** Stable assignment Meta storage watch listener. */
    private final WatchListener stableAssignmentsListener;

    /** Map replication group id to pending assignment nodes. */
    private final Map<ReplicationGroupId, TokenizedAssignments> groupPendingAssignments;

    /** Pending assignment Meta storage watch listener. */
    private final WatchListener pendingAssignmentsListener;

    /**
     * The constructor.
     *
     * @param msManager Meta storage manager.
     * @param failureProcessor Failure processor.
     */
    public AssignmentsTracker(MetaStorageManager msManager, FailureProcessor failureProcessor, NodeProperties nodeProperties) {
        this.msManager = msManager;
        this.failureProcessor = failureProcessor;
        this.nodeProperties = nodeProperties;

        this.groupStableAssignments = new ConcurrentHashMap<>();
        this.stableAssignmentsListener = createStableAssignmentsListener();

        this.groupPendingAssignments = new ConcurrentHashMap<>();
        this.pendingAssignmentsListener = createPendingAssignmentsListener();
    }

    /**
     * Restores assignments form Vault and subscribers on further updates.
     */
    public void startTrack() {
        msManager.registerPrefixWatch(new ByteArray(pendingAssignmentsQueuePrefixBytes()), pendingAssignmentsListener);
        msManager.registerPrefixWatch(new ByteArray(stableAssignmentsPrefixBytes()), stableAssignmentsListener);

        msManager.recoveryFinishedFuture().thenAccept(recoveryRevisions -> {
            handleRecoveryAssignments(recoveryRevisions, pendingAssignmentsQueuePrefixBytes(), groupPendingAssignments,
                    bytes -> AssignmentsQueue.fromBytes(bytes).poll().nodes()
            );
            handleRecoveryAssignments(recoveryRevisions, stableAssignmentsPrefixBytes(), groupStableAssignments,
                    bytes -> Assignments.fromBytes(bytes).nodes()
            );
        }).whenComplete((res, ex) -> {
            if (ex != null) {
                failureProcessor.process(new FailureContext(ex, "Failed to start assignment tracker due to recovery error"));
            } else if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Assignment cache initialized for placement driver [stableAssignments=[{}], pendingAssignments=[{}]]",
                        prepareAssignmentsForLogging(groupStableAssignments),
                        prepareAssignmentsForLogging(groupPendingAssignments));
            }
        });
    }

    /**
     * Stops the tracker.
     */
    public void stopTrack() {
        msManager.unregisterWatch(pendingAssignmentsListener);
        msManager.unregisterWatch(stableAssignmentsListener);
    }

    @Override
    public CompletableFuture<List<TokenizedAssignments>> getAssignments(
            List<? extends ReplicationGroupId> replicationGroupIds,
            HybridTimestamp clusterTimeToAwait
    ) {
        return msManager
                .clusterTime()
                .waitFor(clusterTimeToAwait)
                .thenApply(ignored -> inBusyLock(busyLock, () -> {
                    Map<ReplicationGroupId, TokenizedAssignments> assignments = stableAssignments();

                    return replicationGroupIds.stream()
                            .map(assignments::get)
                            .collect(Collectors.toList());
                }));
    }

    /**
     * Gets stable assignments.
     *
     * @return Map replication group id to its stable assignments.
     */
    Map<ReplicationGroupId, TokenizedAssignments> stableAssignments() {
        return groupStableAssignments;
    }

    /**
     * Gets pending assignments.
     *
     * @return Map replication group id to its pending assignments.
     */
    Map<ReplicationGroupId, TokenizedAssignments> pendingAssignments() {
        return groupPendingAssignments;
    }

    private WatchListener createStableAssignmentsListener() {
        return event -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stable assignments update [revision={}, keys={}]", event.revision(), collectKeysFromEventAsString(event));
            }

            handleReceivedAssignments(event, stableAssignmentsPrefixBytes(), groupStableAssignments,
                    bytes -> Assignments.fromBytes(bytes).nodes()
            );

            return nullCompletedFuture();
        };
    }

    private WatchListener createPendingAssignmentsListener() {
        return event -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Pending assignments update [revision={}, keys={}]", event.revision(), collectKeysFromEventAsString(event));
            }

            handleReceivedAssignments(event, pendingAssignmentsQueuePrefixBytes(), groupPendingAssignments,
                    bytes -> AssignmentsQueue.fromBytes(bytes).poll().nodes()
            );

            return nullCompletedFuture();
        };
    }

    private void handleReceivedAssignments(
            WatchEvent event,
            byte[] assignmentsMetastoreKeyPrefix,
            Map<ReplicationGroupId, TokenizedAssignments> groupIdToAssignmentsMap,
            Function<byte[], Set<Assignment>> deserializer
    ) {
        for (EntryEvent evt : event.entryEvents()) {
            Entry entry = evt.newEntry();

            ReplicationGroupId grpId = extractReplicationGroupPartitionId(entry.key(), assignmentsMetastoreKeyPrefix);

            if (entry.tombstone()) {
                groupIdToAssignmentsMap.remove(grpId);
            } else {
                updateGroupAssignments(groupIdToAssignmentsMap, grpId, entry, deserializer);
            }
        }
    }

    private void handleRecoveryAssignments(
            Revisions recoveryRevisions,
            byte[] assignmentsMetastoreKeyPrefix,
            Map<ReplicationGroupId, TokenizedAssignments> groupIdToAssignmentsMap,
            Function<byte[], Set<Assignment>> deserializer
    ) {
        var prefix = new ByteArray(assignmentsMetastoreKeyPrefix);

        long revision = recoveryRevisions.revision();

        try (Cursor<Entry> cursor = msManager.prefixLocally(prefix, revision)) {
            for (Entry entry : cursor) {
                if (entry.tombstone()) {
                    continue;
                }

                ReplicationGroupId grpId = extractReplicationGroupPartitionId(entry.key(), assignmentsMetastoreKeyPrefix);

                updateGroupAssignments(groupIdToAssignmentsMap, grpId, entry, deserializer);
            }
        }
    }

    private static void updateGroupAssignments(
            Map<ReplicationGroupId, TokenizedAssignments> groupIdToAssignmentsMap,
            ReplicationGroupId grpId,
            Entry entry,
            Function<byte[], Set<Assignment>> deserializer
    ) {
        byte[] value = entry.value();

        // MetaStorage iterator should not return nulls as values.
        assert value != null;

        Set<Assignment> assignmentNodes = deserializer.apply(value);

        groupIdToAssignmentsMap.put(grpId, new TokenizedAssignmentsImpl(assignmentNodes, entry.revision()));
    }

    private static String collectKeysFromEventAsString(WatchEvent event) {
        return event.entryEvents().stream()
                .map(e -> new ByteArray(e.newEntry().key()).toString())
                .collect(Collectors.joining(","));
    }

    /**
     * Prepares assignments for logging using the following structure:
     * consistentId=[peers=[1_part_1, 1_part_2], learners=[2_part_0]].
     *
     * @param assignmentsMap assignments to be logged.
     * @return String representation of assignments.
     */
    private static String prepareAssignmentsForLogging(Map<ReplicationGroupId, TokenizedAssignments> assignmentsMap) {
        class NodeAssignments {
            private List<ReplicationGroupId> peers;
            private List<ReplicationGroupId> learners;

            private NodeAssignments() {
            }

            private void addReplicationGroupId(ReplicationGroupId replicationGroupId, boolean isPeer) {
                List<ReplicationGroupId> peersOrLearners;

                if (isPeer) {
                    if (peers == null) {
                        peers = new ArrayList<>();
                    }
                    peersOrLearners = peers;
                } else {
                    if (learners == null) {
                        learners = new ArrayList<>();
                    }
                    peersOrLearners = learners;
                }

                peersOrLearners.add(replicationGroupId);
            }

            private boolean arePeersEmpty() {
                return peers == null || peers.isEmpty();
            }

            private boolean areLearnersEmpty() {
                return learners == null || learners.isEmpty();
            }
        }

        Map<String, NodeAssignments> assignmentsToLog = new HashMap<>();

        for (Map.Entry<ReplicationGroupId, TokenizedAssignments> assignments : assignmentsMap.entrySet()) {
            for (Assignment assignment : assignments.getValue().nodes()) {
                assignmentsToLog
                        .computeIfAbsent(assignment.consistentId(), k -> new NodeAssignments())
                        .addReplicationGroupId(assignments.getKey(), assignment.isPeer());
            }
        }

        boolean first = true;
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, NodeAssignments> entry : assignmentsToLog.entrySet()) {
            NodeAssignments value = entry.getValue();

            if (value.arePeersEmpty() && value.areLearnersEmpty()) {
                continue;
            }

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(entry.getKey()).append("=[");
            if (!value.arePeersEmpty()) {
                sb.append("peers=").append(value.peers);
                if (!value.areLearnersEmpty()) {
                    sb.append(", ");
                }
            }
            if (!value.areLearnersEmpty()) {
                sb.append("learners=").append(value.learners);
            }
            sb.append(']');
        }

        return sb.toString();
    }

    private byte[] pendingAssignmentsQueuePrefixBytes() {
        return nodeProperties.colocationEnabled()
                ? ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES
                : PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES;
    }

    private byte[] stableAssignmentsPrefixBytes() {
        return nodeProperties.colocationEnabled() ? ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES : STABLE_ASSIGNMENTS_PREFIX_BYTES;
    }

    private ReplicationGroupId extractReplicationGroupPartitionId(byte[] key, byte[] prefix) {
        return nodeProperties.colocationEnabled() ? extractZonePartitionId(key, prefix) : extractTablePartitionId(key, prefix);
    }
}
