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

import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractTablePartitionId;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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
     * @param msManager Metastorage manager.
     */
    public AssignmentsTracker(MetaStorageManager msManager) {
        this.msManager = msManager;

        this.groupStableAssignments = new ConcurrentHashMap<>();
        this.stableAssignmentsListener = createStableAssignmentsListener();

        this.groupPendingAssignments = new ConcurrentHashMap<>();
        this.pendingAssignmentsListener = createPendingAssignmentsListener();
    }

    /**
     * Restores assignments form Vault and subscribers on further updates.
     */
    public void startTrack() {
        msManager.registerPrefixWatch(new ByteArray(PENDING_ASSIGNMENTS_PREFIX_BYTES), pendingAssignmentsListener);
        msManager.registerPrefixWatch(new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES), stableAssignmentsListener);

        msManager.recoveryFinishedFuture().thenAccept(recoveryRevisions -> {
            handleRecoveryAssignments(recoveryRevisions, PENDING_ASSIGNMENTS_PREFIX_BYTES, groupPendingAssignments);
            handleRecoveryAssignments(recoveryRevisions, STABLE_ASSIGNMENTS_PREFIX_BYTES, groupStableAssignments);
        }).whenComplete((res, ex) -> {
            if (ex != null) {
                LOG.error("Cannot do recovery", ex);
            }
        });

        LOG.info(
                "Assignment cache initialized for placement driver [groupStableAssignments={}, groupPendingAssignments={}]",
                groupStableAssignments,
                groupPendingAssignments
        );
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
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Stable assignments update [revision={}, keys={}]", event.revision(), collectKeysFromEventAsString(event));
                }

                handleReceivedAssignments(event, STABLE_ASSIGNMENTS_PREFIX_BYTES, groupStableAssignments);

                return nullCompletedFuture();
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }

    private WatchListener createPendingAssignmentsListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Pending assignments update [revision={}, keys={}]", event.revision(), collectKeysFromEventAsString(event));
                }

                handleReceivedAssignments(event, PENDING_ASSIGNMENTS_PREFIX_BYTES, groupPendingAssignments);

                return nullCompletedFuture();
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }

    private static void handleReceivedAssignments(
            WatchEvent event,
            byte[] assignmentsMetastoreKeyPrefix,
            Map<ReplicationGroupId, TokenizedAssignments> groupIdToAssignmentsMap
    ) {
        for (EntryEvent evt : event.entryEvents()) {
            Entry entry = evt.newEntry();

            ReplicationGroupId grpId = extractTablePartitionId(entry.key(), assignmentsMetastoreKeyPrefix);

            if (entry.tombstone()) {
                groupIdToAssignmentsMap.remove(grpId);
            } else {
                updateGroupAssignments(groupIdToAssignmentsMap, grpId, entry);
            }
        }
    }

    private void handleRecoveryAssignments(
            Revisions recoveryRevisions,
            byte[] assignmentsMetastoreKeyPrefix,
            Map<ReplicationGroupId, TokenizedAssignments> groupIdToAssignmentsMap
    ) {
        var prefix = new ByteArray(assignmentsMetastoreKeyPrefix);

        long revision = recoveryRevisions.revision();

        try (Cursor<Entry> cursor = msManager.prefixLocally(prefix, revision)) {
            for (Entry entry : cursor) {
                if (entry.tombstone()) {
                    continue;
                }

                ReplicationGroupId grpId = extractTablePartitionId(entry.key(), assignmentsMetastoreKeyPrefix);

                updateGroupAssignments(groupIdToAssignmentsMap, grpId, entry);
            }
        }
    }

    private static void updateGroupAssignments(
            Map<ReplicationGroupId, TokenizedAssignments> groupIdToAssignmentsMap,
            ReplicationGroupId grpId,
            Entry entry
    ) {
        byte[] value = entry.value();

        // MetaStorage iterator should not return nulls as values.
        assert value != null;

        Set<Assignment> assignmentNodes = Assignments.fromBytes(value).nodes();

        groupIdToAssignmentsMap.put(grpId, new TokenizedAssignmentsImpl(assignmentNodes, entry.revision()));
    }

    private static String collectKeysFromEventAsString(WatchEvent event) {
        return event.entryEvents().stream()
                .map(e -> new ByteArray(e.newEntry().key()).toString())
                .collect(Collectors.joining(","));
    }

}
