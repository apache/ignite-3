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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.StringUtils.incrementLastChar;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.affinity.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
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

    /** Map replication group id to assignment nodes. */
    private final Map<ReplicationGroupId, TokenizedAssignments> groupAssignments;

    /** Assignment Meta storage watch listener. */
    private final AssignmentsListener assignmentsListener;


    /**
     * The constructor.
     *
     * @param msManager Metastorage manager.
     */
    public AssignmentsTracker(MetaStorageManager msManager) {
        this.msManager = msManager;

        this.groupAssignments = new ConcurrentHashMap<>();
        this.assignmentsListener = new AssignmentsListener();
    }

    /**
     * Restores assignments form Vault and subscribers on further updates.
     */
    public void startTrack() {
        msManager.registerPrefixWatch(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX), assignmentsListener);

        msManager.recoveryFinishedFuture().thenAccept(recoveryRevision -> {
            try (Cursor<Entry> cursor = msManager.getLocally(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX),
                    ByteArray.fromString(incrementLastChar(STABLE_ASSIGNMENTS_PREFIX)), recoveryRevision);
            ) {
                for (Entry entry : cursor) {
                    if (entry.tombstone()) {
                        continue;
                    }

                    byte[] key = entry.key();
                    byte[] value = entry.value();

                    // MetaStorage iterator should not return nulls as values.
                    assert value != null;

                    String strKey = new String(key, StandardCharsets.UTF_8);

                    strKey = strKey.replace(STABLE_ASSIGNMENTS_PREFIX, "");

                    TablePartitionId grpId = TablePartitionId.fromString(strKey);

                    Set<Assignment> assignmentNodes = Assignments.fromBytes(entry.value()).nodes();

                    groupAssignments.put(grpId, new TokenizedAssignmentsImpl(assignmentNodes, entry.revision()));
                }
            }
        }).whenComplete((res, ex) -> {
            if (ex != null) {
                LOG.error("Cannot do recovery", ex);
            }
        });

        LOG.info("Assignment cache initialized for placement driver [groupAssignments={}]", groupAssignments);
    }

    /**
     * Stops the tracker.
     */
    public void stopTrack() {
        msManager.unregisterWatch(assignmentsListener);
    }

    @Override
    public CompletableFuture<TokenizedAssignments> getAssignments(
            ReplicationGroupId replicationGroupId,
            HybridTimestamp clusterTimeToAwait)
    {
        return msManager
                .clusterTime()
                .waitFor(clusterTimeToAwait)
                .thenApply(ignored -> inBusyLock(busyLock, () -> assignments().get(replicationGroupId)));
    }

    /**
     * Gets assignments.
     *
     * @return Map replication group id to its assignment.
     */
    public Map<ReplicationGroupId, TokenizedAssignments> assignments() {
        return groupAssignments;
    }

    /**
     * Meta storage assignments watch.
     */
    private class AssignmentsListener implements WatchListener {
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
            assert !event.entryEvents().stream().anyMatch(e -> e.newEntry().empty()) : "New assignments are empty";

            if (LOG.isDebugEnabled()) {
                LOG.debug("Assignment update [revision={}, keys={}]", event.revision(),
                        event.entryEvents().stream()
                                .map(e -> new ByteArray(e.newEntry().key()).toString())
                                .collect(Collectors.joining(",")));
            }

            for (EntryEvent evt : event.entryEvents()) {
                Entry entry = evt.newEntry();

                var replicationGrpId = TablePartitionId.fromString(
                        new String(entry.key(), StandardCharsets.UTF_8).replace(STABLE_ASSIGNMENTS_PREFIX, ""));

                if (entry.tombstone()) {
                    groupAssignments.remove(replicationGrpId);
                } else {
                    Set<Assignment> newAssignments = Assignments.fromBytes(entry.value()).nodes();
                    groupAssignments.put(replicationGrpId, new TokenizedAssignmentsImpl(newAssignments, entry.revision()));
                }
            }

            return nullCompletedFuture();
        }

        @Override
        public void onError(Throwable e) {
        }
    }
}
