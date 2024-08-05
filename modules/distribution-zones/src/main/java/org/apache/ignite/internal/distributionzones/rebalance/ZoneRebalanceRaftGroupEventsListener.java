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

package org.apache.ignite.internal.distributionzones.rebalance;

import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.switchAppendKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.switchReduceKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.union;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CollectionUtils.intersect;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftError;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.Status;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Listener for the raft group events, which must provide correct error handling of rebalance process and start new rebalance after the
 * current one finished.
 */
public class ZoneRebalanceRaftGroupEventsListener implements RaftGroupEventsListener {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ZoneRebalanceRaftGroupEventsListener.class);

    /** Number of retrying of the current rebalance in case of errors. */
    private static final int REBALANCE_RETRY_THRESHOLD = 10;

    /** Delay between unsuccessful trial of a rebalance and a new trial, ms. */
    private static final int REBALANCE_RETRY_DELAY_MS = 200;

    /** Success code for the MetaStorage switch append assignments change. */
    private static final int SWITCH_APPEND_SUCCESS = 1;

    /** Success code for the MetaStorage switch reduce assignments change. */
    private static final int SWITCH_REDUCE_SUCCESS = 2;

    /** Success code for the MetaStorage pending rebalance change. */
    private static final int SCHEDULE_PENDING_REBALANCE_SUCCESS = 3;

    /** Success code for the MetaStorage stable assignments change. */
    private static final int FINISH_REBALANCE_SUCCESS = 4;

    /** Failure code for the MetaStorage switch append assignments change. */
    private static final int SWITCH_APPEND_FAIL = -SWITCH_APPEND_SUCCESS;

    /** Failure code for the MetaStorage switch reduce assignments change. */
    private static final int SWITCH_REDUCE_FAIL = -SWITCH_REDUCE_SUCCESS;

    /** Failure code for the MetaStorage pending rebalance change. */
    private static final int SCHEDULE_PENDING_REBALANCE_FAIL = -SCHEDULE_PENDING_REBALANCE_SUCCESS;

    /** Failure code for the MetaStorage stable assignments change. */
    private static final int FINISH_REBALANCE_FAIL = -FINISH_REBALANCE_SUCCESS;

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    private final CatalogService catalogService;

    private final DistributionZoneManager distributionZoneManager;

    /** Unique table partition id. */
    private final ZonePartitionId zonePartitionId;

    /** Busy lock of parent component for synchronous stop. */
    private final IgniteSpinBusyLock busyLock;

    /** Executor for scheduling rebalance retries. */
    private final ScheduledExecutorService rebalanceScheduler;

    /** Performs reconfiguration of a Raft group of a partition. */
    private final PartitionMover partitionMover;

    /** Attempts to retry the current rebalance in case of errors. */
    private final AtomicInteger rebalanceAttempts = new AtomicInteger(0);

    /**
     * Constructs new listener.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param zonePartitionId Partition id.
     * @param busyLock Busy lock.
     * @param partitionMover Class that moves partition between nodes.
     * @param rebalanceScheduler Executor for scheduling rebalance retries.
     */
    public ZoneRebalanceRaftGroupEventsListener(
            MetaStorageManager metaStorageMgr,
            ZonePartitionId zonePartitionId,
            IgniteSpinBusyLock busyLock,
            PartitionMover partitionMover,
            ScheduledExecutorService rebalanceScheduler,
            CatalogService catalogService,
            DistributionZoneManager distributionZoneManager
    ) {
        this.metaStorageMgr = metaStorageMgr;
        this.zonePartitionId = zonePartitionId;
        this.busyLock = busyLock;
        this.partitionMover = partitionMover;
        this.rebalanceScheduler = rebalanceScheduler;
        this.distributionZoneManager = distributionZoneManager;
        this.catalogService = catalogService;
    }

    /** {@inheritDoc} */
    @Override
    public void onLeaderElected(long term) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            rebalanceScheduler.schedule(() -> {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    rebalanceAttempts.set(0);

                    byte[] pendingAssignmentsBytes = metaStorageMgr.get(pendingPartAssignmentsKey(zonePartitionId)).get().value();

                    if (pendingAssignmentsBytes != null) {
                        Set<Assignment> pendingAssignments = Assignments.fromBytes(pendingAssignmentsBytes).nodes();

                        var peers = new HashSet<String>();
                        var learners = new HashSet<String>();

                        for (Assignment assignment : pendingAssignments) {
                            if (assignment.isPeer()) {
                                peers.add(assignment.consistentId());
                            } else {
                                learners.add(assignment.consistentId());
                            }
                        }

                        LOG.info(
                                "New leader elected. Going to apply new configuration [zonePartitionId={}, peers={}, learners={}]",
                                zonePartitionId, peers, learners
                        );

                        PeersAndLearners peersAndLearners = PeersAndLearners.fromConsistentIds(peers, learners);

                        partitionMover.movePartition(peersAndLearners, term).get();
                    }
                } catch (Exception e) {
                    // TODO: IGNITE-14693
                    LOG.warn("Unable to start rebalance [tablePartitionId, term={}]", e, zonePartitionId, term);
                } finally {
                    busyLock.leaveBusy();
                }
            }, 0, TimeUnit.MILLISECONDS);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onNewPeersConfigurationApplied(PeersAndLearners configuration) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            rebalanceScheduler.schedule(() -> {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    Set<Assignment> stable = createAssignments(configuration);

                    doStableKeySwitch(
                            stable,
                            zonePartitionId,
                            metaStorageMgr,
                            catalogService,
                            distributionZoneManager
                    );
                } finally {
                    busyLock.leaveBusy();
                }
            }, 0, TimeUnit.MILLISECONDS);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onReconfigurationError(Status status, PeersAndLearners configuration, long term) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            assert status != null;

            if (status.equals(Status.LEADER_STEPPED_DOWN)) {
                // Leader stepped down, so we are expecting RebalanceRaftGroupEventsListener.onLeaderElected to be called on a new leader.
                LOG.info("Leader stepped down during rebalance [partId={}]", zonePartitionId);

                return;
            }

            RaftError raftError = status.error();

            assert raftError == RaftError.ECATCHUP : "According to the JRaft protocol, " + RaftError.ECATCHUP
                    + " is expected, got " + raftError;

            LOG.debug("Error occurred during rebalance [partId={}]", zonePartitionId);

            if (rebalanceAttempts.incrementAndGet() < REBALANCE_RETRY_THRESHOLD) {
                scheduleChangePeersAndLearners(configuration, term);
            } else {
                LOG.info("Number of retries for rebalance exceeded the threshold [partId={}, threshold={}]", zonePartitionId,
                        REBALANCE_RETRY_THRESHOLD);

                // TODO: currently we just retry intent to change peers according to the rebalance infinitely, until new leader is elected,
                // TODO: but rebalance cancel mechanism should be implemented. https://issues.apache.org/jira/browse/IGNITE-19087
                scheduleChangePeersAndLearners(configuration, term);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Schedules changing peers according to the current rebalance.
     *
     * @param peersAndLearners Peers and learners.
     * @param term Current known leader term.
     */
    private void scheduleChangePeersAndLearners(PeersAndLearners peersAndLearners, long term) {
        rebalanceScheduler.schedule(() -> {
            if (!busyLock.enterBusy()) {
                return;
            }

            LOG.info("Going to retry rebalance [attemptNo={}, partId={}]", rebalanceAttempts.get(), zonePartitionId);

            try {
                partitionMover.movePartition(peersAndLearners, term).join();
            } finally {
                busyLock.leaveBusy();
            }
        }, REBALANCE_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Updates stable value with the new applied assignment.
     */
    static void doStableKeySwitch(
            Set<Assignment> stableFromRaft,
            ZonePartitionId zonePartitionId,
            MetaStorageManager metaStorageMgr,
            CatalogService catalogService,
            DistributionZoneManager distributionZoneManager
    ) {
        try {
            ByteArray pendingPartAssignmentsKey = pendingPartAssignmentsKey(zonePartitionId);
            ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(zonePartitionId);
            ByteArray plannedPartAssignmentsKey = plannedPartAssignmentsKey(zonePartitionId);
            ByteArray switchReduceKey = switchReduceKey(zonePartitionId);
            ByteArray switchAppendKey = switchAppendKey(zonePartitionId);
            // TODO: https://issues.apache.org/jira/browse/IGNITE-17592 Remove synchronous wait
            Map<ByteArray, Entry> values = metaStorageMgr.getAll(
                    Set.of(
                            plannedPartAssignmentsKey,
                            pendingPartAssignmentsKey,
                            stablePartAssignmentsKey,
                            switchReduceKey,
                            switchAppendKey
                    )
            ).get();

            // TODO: IGNITE-22680 Find a better way to retrieve the catalog version.
            int catalogVersion = catalogService.latestCatalogVersion();

            Set<Assignment> calculatedAssignments = calculateZoneAssignments(
                    zonePartitionId,
                    catalogService,
                    distributionZoneManager,
                    catalogVersion
            ).get();

            Entry stableEntry = values.get(stablePartAssignmentsKey);
            Entry pendingEntry = values.get(pendingPartAssignmentsKey);
            Entry plannedEntry = values.get(plannedPartAssignmentsKey);
            Entry switchReduceEntry = values.get(switchReduceKey);
            Entry switchAppendEntry = values.get(switchAppendKey);

            Set<Assignment> retrievedStable = readAssignments(stableEntry).nodes();
            Set<Assignment> retrievedSwitchReduce = readAssignments(switchReduceEntry).nodes();
            Set<Assignment> retrievedSwitchAppend = readAssignments(switchAppendEntry).nodes();
            Set<Assignment> retrievedPending = readAssignments(pendingEntry).nodes();

            if (!retrievedPending.equals(stableFromRaft)) {
                return;
            }

            // Were reduced
            Set<Assignment> reducedNodes = difference(retrievedSwitchReduce, stableFromRaft);

            // Were added
            Set<Assignment> addedNodes = difference(stableFromRaft, retrievedStable);

            // For further reduction
            Set<Assignment> calculatedSwitchReduce = difference(retrievedSwitchReduce, reducedNodes);

            // For further addition
            Set<Assignment> calculatedSwitchAppend = union(retrievedSwitchAppend, reducedNodes);
            calculatedSwitchAppend = difference(calculatedSwitchAppend, addedNodes);
            calculatedSwitchAppend = intersect(calculatedAssignments, calculatedSwitchAppend);

            Set<Assignment> calculatedPendingReduction = difference(stableFromRaft, retrievedSwitchReduce);

            Set<Assignment> calculatedPendingAddition = union(stableFromRaft, reducedNodes);
            calculatedPendingAddition = intersect(calculatedAssignments, calculatedPendingAddition);

            // eq(revision(assignments.stable), retrievedAssignmentsStable.revision)
            SimpleCondition con1 = stableEntry.empty()
                    ? notExists(stablePartAssignmentsKey) :
                    revision(stablePartAssignmentsKey).eq(stableEntry.revision());

            // eq(revision(assignments.pending), retrievedAssignmentsPending.revision)
            SimpleCondition con2 = revision(pendingPartAssignmentsKey).eq(pendingEntry.revision());

            // eq(revision(assignments.switch.reduce), retrievedAssignmentsSwitchReduce.revision)
            SimpleCondition con3 = switchReduceEntry.empty()
                    ? notExists(switchReduceKey) : revision(switchReduceKey).eq(switchReduceEntry.revision());

            // eq(revision(assignments.switch.append), retrievedAssignmentsSwitchAppend.revision)
            SimpleCondition con4 = switchAppendEntry.empty()
                    ? notExists(switchAppendKey) : revision(switchAppendKey).eq(switchAppendEntry.revision());

            // All conditions combined with AND operator.
            Condition retryPreconditions = and(con1, and(con2, and(con3, con4)));

            Update successCase;
            Update failCase;

            byte[] stableFromRaftByteArray = Assignments.toBytes(stableFromRaft);
            byte[] additionByteArray = Assignments.toBytes(calculatedPendingAddition);
            byte[] reductionByteArray = Assignments.toBytes(calculatedPendingReduction);
            byte[] switchReduceByteArray = Assignments.toBytes(calculatedSwitchReduce);
            byte[] switchAppendByteArray = Assignments.toBytes(calculatedSwitchAppend);

            if (!calculatedSwitchAppend.isEmpty()) {
                successCase = ops(
                        put(stablePartAssignmentsKey, stableFromRaftByteArray),
                        put(pendingPartAssignmentsKey, additionByteArray),
                        put(switchReduceKey, switchReduceByteArray),
                        put(switchAppendKey, switchAppendByteArray)
                ).yield(SWITCH_APPEND_SUCCESS);
                failCase = ops().yield(SWITCH_APPEND_FAIL);
            } else if (!calculatedSwitchReduce.isEmpty()) {
                successCase = ops(
                        put(stablePartAssignmentsKey, stableFromRaftByteArray),
                        put(pendingPartAssignmentsKey, reductionByteArray),
                        put(switchReduceKey, switchReduceByteArray),
                        put(switchAppendKey, switchAppendByteArray)
                ).yield(SWITCH_REDUCE_SUCCESS);
                failCase = ops().yield(SWITCH_REDUCE_FAIL);
            } else {
                Condition con5;
                if (plannedEntry.value() != null) {
                    // eq(revision(partition.assignments.planned), plannedEntry.revision)
                    con5 = revision(plannedPartAssignmentsKey).eq(plannedEntry.revision());

                    successCase = ops(
                            put(stablePartAssignmentsKey, stableFromRaftByteArray),
                            put(pendingPartAssignmentsKey, plannedEntry.value()),
                            remove(plannedPartAssignmentsKey)
                    ).yield(SCHEDULE_PENDING_REBALANCE_SUCCESS);

                    failCase = ops().yield(SCHEDULE_PENDING_REBALANCE_FAIL);
                } else {
                    // notExists(partition.assignments.planned)
                    con5 = notExists(plannedPartAssignmentsKey);

                    successCase = ops(
                            put(stablePartAssignmentsKey, stableFromRaftByteArray),
                            remove(pendingPartAssignmentsKey)
                    ).yield(FINISH_REBALANCE_SUCCESS);

                    failCase = ops().yield(FINISH_REBALANCE_FAIL);
                }

                retryPreconditions = and(retryPreconditions, con5);
            }

            // TODO: https://issues.apache.org/jira/browse/IGNITE-17592 Remove synchronous wait
            int res = metaStorageMgr.invoke(iif(retryPreconditions, successCase, failCase)).get().getAsInt();

            if (res < 0) {
                switch (res) {
                    case SWITCH_APPEND_FAIL:
                        LOG.info("Rebalance keys changed while trying to update rebalance pending addition information. "
                                        + "Going to retry [zonePartitionId={}, appliedPeers={}]",
                                zonePartitionId, stableFromRaft
                        );
                        break;
                    case SWITCH_REDUCE_FAIL:
                        LOG.info("Rebalance keys changed while trying to update rebalance pending reduce information. "
                                        + "Going to retry [zonePartitionId={}, appliedPeers={}]",
                                zonePartitionId, stableFromRaft
                        );
                        break;
                    case SCHEDULE_PENDING_REBALANCE_FAIL:
                    case FINISH_REBALANCE_FAIL:
                        LOG.info("Rebalance keys changed while trying to update rebalance information. "
                                        + "Going to retry [zonePartitionId={}, appliedPeers={}]",
                                zonePartitionId, stableFromRaft
                        );
                        break;
                    default:
                        assert false : res;
                        break;
                }

                doStableKeySwitch(
                        stableFromRaft,
                        zonePartitionId,
                        metaStorageMgr,
                        catalogService,
                        distributionZoneManager
                );

                return;
            }

            switch (res) {
                case SWITCH_APPEND_SUCCESS:
                    LOG.info("Rebalance finished. Going to schedule next rebalance with addition"
                                    + " [zonePartitionId={}, appliedPeers={}, plannedPeers={}]",
                            zonePartitionId, stableFromRaft, calculatedPendingAddition
                    );
                    break;
                case SWITCH_REDUCE_SUCCESS:
                    LOG.info("Rebalance finished. Going to schedule next rebalance with reduction"
                                    + " [zonePartitionId={}, appliedPeers={}, plannedPeers={}]",
                            zonePartitionId, stableFromRaft, calculatedPendingReduction
                    );
                    break;
                case SCHEDULE_PENDING_REBALANCE_SUCCESS:
                    LOG.info(
                            "Rebalance finished. Going to schedule next rebalance [zonePartitionId={}, appliedPeers={}, plannedPeers={}]",
                            zonePartitionId, stableFromRaft, Assignments.fromBytes(plannedEntry.value()).nodes()
                    );
                    break;
                case FINISH_REBALANCE_SUCCESS:
                    LOG.info("Rebalance finished [zonePartitionId={}, appliedPeers={}]", zonePartitionId, stableFromRaft);
                    break;

                default:
                    assert false : res;
                    break;
            }

        } catch (InterruptedException | ExecutionException e) {
            // TODO: IGNITE-14693
            LOG.warn("Unable to commit partition configuration to metastore: " + zonePartitionId, e);
        }
    }

    /**
     * Creates a set of assignments from the given set of peers and learners.
     */
    private static Set<Assignment> createAssignments(PeersAndLearners configuration) {
        Stream<Assignment> newAssignments = Stream.concat(
                configuration.peers().stream().map(peer -> Assignment.forPeer(peer.consistentId())),
                configuration.learners().stream().map(peer -> Assignment.forLearner(peer.consistentId()))
        );

        return newAssignments.collect(Collectors.toSet());
    }

    /**
     * Reads a set of assignments from a MetaStorage entry.
     *
     * @param entry MetaStorage entry.
     * @return Set of cluster assignments.
     */
    private static Assignments readAssignments(Entry entry) {
        byte[] value = entry.value();

        return value == null ? Assignments.EMPTY : Assignments.fromBytes(value);
    }

    /**
     * Handles assignments switch reduce changed updating pending assignments if there is no rebalancing in progress.
     * If there is rebalancing in progress, then new assignments will be applied when rebalance finishes.
     *
     * @param metaStorageMgr MetaStorage manager.
     * @param dataNodes Data nodes.
     * @param replicas Replicas count.
     * @param partId Partition's raft group id.
     * @param event Assignments switch reduce change event.
     * @return Completable future that signifies the completion of this operation.
     */
    public static CompletableFuture<Void> handleReduceChanged(MetaStorageManager metaStorageMgr, Collection<String> dataNodes,
            int replicas, ZonePartitionId partId, WatchEvent event) {
        Entry entry = event.entryEvent().newEntry();
        byte[] eventData = entry.value();

        assert eventData != null : "Null event data for " + partId;

        Assignments switchReduce = Assignments.fromBytes(eventData);

        if (switchReduce.isEmpty()) {
            return nullCompletedFuture();
        }

        Set<Assignment> assignments = AffinityUtils.calculateAssignmentForPartition(dataNodes, partId.partitionId(), replicas);

        ByteArray pendingKey = pendingPartAssignmentsKey(partId);

        Set<Assignment> pendingAssignments = difference(assignments, switchReduce.nodes());

        byte[] pendingByteArray = Assignments.toBytes(pendingAssignments);
        byte[] assignmentsByteArray = Assignments.toBytes(assignments);

        ByteArray changeTriggerKey = ZoneRebalanceUtil.pendingChangeTriggerKey(partId);
        byte[] rev = ByteUtils.longToBytesKeepingOrder(entry.revision());

        // Here is what happens in the MetaStorage:
        // if ((notExists(changeTriggerKey) || value(changeTriggerKey) < revision) && (notExists(pendingKey) && notExists(stableKey)) {
        //     put(pendingKey, pending)
        //     put(stableKey, assignments)
        //     put(changeTriggerKey, revision)
        // } else if ((notExists(changeTriggerKey) || value(changeTriggerKey) < revision) && (notExists(pendingKey))) {
        //     put(pendingKey, pending)
        //     put(changeTriggerKey, revision)
        // }

        Iif resultingOperation = iif(
                and(
                        or(notExists(changeTriggerKey), value(changeTriggerKey).lt(rev)),
                        and(notExists(pendingKey), (notExists(stablePartAssignmentsKey(partId))))
                ),
                ops(
                        put(pendingKey, pendingByteArray),
                        put(stablePartAssignmentsKey(partId), assignmentsByteArray),
                        put(changeTriggerKey, rev)
                ).yield(),
                iif(
                        and(
                                or(notExists(changeTriggerKey), value(changeTriggerKey).lt(rev)),
                                notExists(pendingKey)
                        ),
                        ops(
                                put(pendingKey, pendingByteArray),
                                put(changeTriggerKey, rev)
                        ).yield(),
                        ops().yield()
                )
        );

        return metaStorageMgr.invoke(resultingOperation).thenApply(unused -> null);
    }

    private static CompletableFuture<Set<Assignment>> calculateZoneAssignments(
            ZonePartitionId zonePartitionId,
            CatalogService catalogService,
            DistributionZoneManager distributionZoneManager,
            int catalogVersion
    ) {
        CatalogZoneDescriptor zoneDescriptor = catalogService.zone(zonePartitionId.zoneId(), catalogVersion);

        int zoneId = zonePartitionId.zoneId();

        return distributionZoneManager.dataNodes(
                zoneDescriptor.updateToken(),
                catalogVersion,
                zoneId
        ).thenApply(dataNodes ->
                AffinityUtils.calculateAssignmentForPartition(
                        dataNodes,
                        zonePartitionId.partitionId(),
                        zoneDescriptor.replicas()
                )
        );
    }
}
