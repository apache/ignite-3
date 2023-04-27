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

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.utils.RebalanceUtil.intersect;
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.subtract;
import static org.apache.ignite.internal.utils.RebalanceUtil.switchAppendKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.switchReduceKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.union;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftError;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.Status;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.table.distributed.PartitionMover;
import org.apache.ignite.internal.table.distributed.replicator.ZoneReplicaGroupId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ByteArray;

/**
 * Listener for the raft group events, which must provide correct error handling of rebalance process
 * and start new rebalance after the current one finished.
 */
public class RebalanceRaftGroupEventsListener implements RaftGroupEventsListener {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RebalanceRaftGroupEventsListener.class);

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

    /** Table configuration instance. */
    private final TableConfiguration tblConfiguration;

    private final ZoneReplicaGroupId zoneReplicaGroupId;

    /** Partition number. */
    private final int partNum;

    /** Busy lock of parent component for synchronous stop. */
    private final IgniteSpinBusyLock busyLock;

    /** Executor for scheduling rebalance retries. */
    private final ScheduledExecutorService rebalanceScheduler;

    /** Performs reconfiguration of a Raft group of a partition. */
    private final PartitionMover partitionMover;

    /** Attempts to retry the current rebalance in case of errors. */
    private final AtomicInteger rebalanceAttempts =  new AtomicInteger(0);

    /** Function that calculates assignments for table's partition. */
    private final BiFunction<TableConfiguration, Integer, Set<Assignment>> calculateAssignmentsFn;

    /**
     * Constructs new listener.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param tblConfiguration Table configuration.
     * @param partNum Partition number.
     * @param busyLock Busy lock.
     * @param partitionMover Class that moves partition between nodes.
     * @param calculateAssignmentsFn Function that calculates assignments for table's partition.
     * @param rebalanceScheduler Executor for scheduling rebalance retries.
     */
    public RebalanceRaftGroupEventsListener(
            MetaStorageManager metaStorageMgr,
            TableConfiguration tblConfiguration,
            int partNum,
            IgniteSpinBusyLock busyLock,
            PartitionMover partitionMover,
            BiFunction<TableConfiguration, Integer, Set<Assignment>> calculateAssignmentsFn,
            ScheduledExecutorService rebalanceScheduler) {
        this.metaStorageMgr = metaStorageMgr;
        this.tblConfiguration = tblConfiguration;
        // TODO: KKK we need to receive the right zone key here. Eventual alter zone can ruin the whole idea.
        this.zoneReplicaGroupId = new ZoneReplicaGroupId(tblConfiguration.zoneId().value(), partNum);
        this.partNum = partNum;
        this.busyLock = busyLock;
        this.partitionMover = partitionMover;
        this.calculateAssignmentsFn = calculateAssignmentsFn;
        this.rebalanceScheduler = rebalanceScheduler;
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

                    byte[] pendingAssignmentsBytes = metaStorageMgr.get(pendingPartAssignmentsKey(zoneReplicaGroupId)).get().value();

                    if (pendingAssignmentsBytes != null) {
                        Set<Assignment> pendingAssignments = ByteUtils.fromBytes(pendingAssignmentsBytes);

                        var peers = new HashSet<String>();
                        var learners = new HashSet<String>();

                        for (Assignment assignment : pendingAssignments) {
                            if (assignment.isPeer()) {
                                peers.add(assignment.consistentId());
                            } else {
                                learners.add(assignment.consistentId());
                            }
                        }

                        LOG.info("New leader elected. Going to apply new configuration "
                                        + "[group={}, partition={}, table={}, peers={}, learners={}]",
                                zoneReplicaGroupId, partNum, tblConfiguration.name().value(), peers, learners);

                        PeersAndLearners peersAndLearners = PeersAndLearners.fromConsistentIds(peers, learners);

                        partitionMover.movePartition(peersAndLearners, term).get();
                    }
                } catch (Exception e) {
                    // TODO: IGNITE-14693
                    LOG.warn("Unable to start rebalance [partition={}, table={}, term={}]",
                            e, partNum, tblConfiguration.name().value(), term);
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
                    doOnNewPeersConfigurationApplied(configuration);
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
                LOG.info("Leader stepped down during rebalance [partId={}]", zoneReplicaGroupId);

                return;
            }

            RaftError raftError = status.error();

            assert raftError == RaftError.ECATCHUP : "According to the JRaft protocol, " + RaftError.ECATCHUP
                    + " is expected, got " + raftError;

            LOG.debug("Error occurred during rebalance [partId={}]", zoneReplicaGroupId);

            if (rebalanceAttempts.incrementAndGet() < REBALANCE_RETRY_THRESHOLD) {
                scheduleChangePeers(configuration, term);
            } else {
                LOG.info("Number of retries for rebalance exceeded the threshold [partId={}, threshold={}]", zoneReplicaGroupId,
                        REBALANCE_RETRY_THRESHOLD);

                // TODO: currently we just retry intent to change peers according to the rebalance infinitely, until new leader is elected,
                // TODO: but rebalance cancel mechanism should be implemented. https://issues.apache.org/jira/browse/IGNITE-19087
                scheduleChangePeers(configuration, term);
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
    private void scheduleChangePeers(PeersAndLearners peersAndLearners, long term) {
        rebalanceScheduler.schedule(() -> {
            if (!busyLock.enterBusy()) {
                return;
            }

            LOG.info("Going to retry rebalance [attemptNo={}, partId={}]", rebalanceAttempts.get(), zoneReplicaGroupId);

            try {
                partitionMover.movePartition(peersAndLearners, term).join();
            } finally {
                busyLock.leaveBusy();
            }
        }, REBALANCE_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Implementation of {@link RebalanceRaftGroupEventsListener#onNewPeersConfigurationApplied}.
     */
    private void doOnNewPeersConfigurationApplied(PeersAndLearners configuration) {
        try {
            ByteArray pendingPartAssignmentsKey = pendingPartAssignmentsKey(zoneReplicaGroupId);
            ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(zoneReplicaGroupId);
            ByteArray plannedPartAssignmentsKey = plannedPartAssignmentsKey(zoneReplicaGroupId);
            ByteArray switchReduceKey = switchReduceKey(zoneReplicaGroupId);
            ByteArray switchAppendKey = switchAppendKey(zoneReplicaGroupId);

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

            Entry stableEntry = values.get(stablePartAssignmentsKey);
            Entry pendingEntry = values.get(pendingPartAssignmentsKey);
            Entry plannedEntry = values.get(plannedPartAssignmentsKey);
            Entry switchReduceEntry = values.get(switchReduceKey);
            Entry switchAppendEntry = values.get(switchAppendKey);

            Set<Assignment> retrievedStable = readAssignments(stableEntry);
            Set<Assignment> retrievedSwitchReduce = readAssignments(switchReduceEntry);
            Set<Assignment> retrievedSwitchAppend = readAssignments(switchAppendEntry);

            Set<Assignment> calculatedAssignments = calculateAssignmentsFn.apply(tblConfiguration, partNum);

            Set<Assignment> stable = createAssignments(configuration);

            // Were reduced
            Set<Assignment> reducedNodes = subtract(retrievedSwitchReduce, stable);

            // Were added
            Set<Assignment> addedNodes = subtract(stable, retrievedStable);

            // For further reduction
            Set<Assignment> calculatedSwitchReduce = subtract(retrievedSwitchReduce, reducedNodes);

            // For further addition
            Set<Assignment> calculatedSwitchAppend = union(retrievedSwitchAppend, reducedNodes);
            calculatedSwitchAppend = subtract(calculatedSwitchAppend, addedNodes);
            calculatedSwitchAppend = intersect(calculatedAssignments, calculatedSwitchAppend);

            Set<Assignment> calculatedPendingReduction = subtract(stable, retrievedSwitchReduce);

            Set<Assignment> calculatedPendingAddition = union(stable, reducedNodes);
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

            // TODO: https://issues.apache.org/jira/browse/IGNITE-17592 Remove synchronous wait
            tblConfiguration.change(ch -> {
                List<Set<Assignment>> assignments = ByteUtils.fromBytes(((ExtendedTableChange) ch).assignments());
                assignments.set(partNum, stable);
                ((ExtendedTableChange) ch).changeAssignments(ByteUtils.toBytes(assignments));
            }).get(10, TimeUnit.SECONDS);

            Update successCase;
            Update failCase;

            byte[] stableByteArray = ByteUtils.toBytes(stable);
            byte[] additionByteArray = ByteUtils.toBytes(calculatedPendingAddition);
            byte[] reductionByteArray = ByteUtils.toBytes(calculatedPendingReduction);
            byte[] switchReduceByteArray = ByteUtils.toBytes(calculatedSwitchReduce);
            byte[] switchAppendByteArray = ByteUtils.toBytes(calculatedSwitchAppend);

            if (!calculatedSwitchAppend.isEmpty()) {
                successCase = ops(
                        put(stablePartAssignmentsKey, stableByteArray),
                        put(pendingPartAssignmentsKey, additionByteArray),
                        put(switchReduceKey, switchReduceByteArray),
                        put(switchAppendKey, switchAppendByteArray)
                ).yield(SWITCH_APPEND_SUCCESS);
                failCase = ops().yield(SWITCH_APPEND_FAIL);
            } else if (!calculatedSwitchReduce.isEmpty()) {
                successCase = ops(
                        put(stablePartAssignmentsKey, stableByteArray),
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
                            put(stablePartAssignmentsKey, ByteUtils.toBytes(stable)),
                            put(pendingPartAssignmentsKey, plannedEntry.value()),
                            remove(plannedPartAssignmentsKey)
                    ).yield(SCHEDULE_PENDING_REBALANCE_SUCCESS);

                    failCase = ops().yield(SCHEDULE_PENDING_REBALANCE_FAIL);
                } else {
                    // notExists(partition.assignments.planned)
                    con5 = notExists(plannedPartAssignmentsKey);

                    successCase = ops(
                            put(stablePartAssignmentsKey, ByteUtils.toBytes(stable)),
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
                        LOG.info("Rebalance keys changed while trying to update rebalance pending addition information. Going to retry"
                                + " [partition={}, table={}, appliedPeers={}]", partNum, tblConfiguration.name(), stable);
                        break;
                    case SWITCH_REDUCE_FAIL:
                        LOG.info("Rebalance keys changed while trying to update rebalance pending reduce information. Going to retry"
                                + " [partition={}, table={}, appliedPeers={}]", partNum, tblConfiguration.name(), stable);
                        break;
                    case SCHEDULE_PENDING_REBALANCE_FAIL:
                    case FINISH_REBALANCE_FAIL:
                        LOG.info("Rebalance keys changed while trying to update rebalance information. Going to retry"
                                + " [partition={}, table={}, appliedPeers={}]", partNum, tblConfiguration.name(), stable);
                        break;
                    default:
                        assert false : res;
                        break;
                }

                doOnNewPeersConfigurationApplied(configuration);
                return;
            }

            switch (res) {
                case SWITCH_APPEND_SUCCESS:
                    LOG.info("Rebalance finished. Going to schedule next rebalance with addition"
                            + " [partition={}, table={}, appliedPeers={}, plannedPeers={}]",
                            partNum, tblConfiguration.name().value(), stable, calculatedPendingAddition);
                    break;
                case SWITCH_REDUCE_SUCCESS:
                    LOG.info("Rebalance finished. Going to schedule next rebalance with reduction"
                            + " [partition={}, table={}, appliedPeers={}, plannedPeers={}]",
                            partNum, tblConfiguration.name().value(), stable, calculatedPendingReduction);
                    break;
                case SCHEDULE_PENDING_REBALANCE_SUCCESS:
                    LOG.info("Rebalance finished. Going to schedule next rebalance [partition={}, table={}, appliedPeers={}, "
                            + "plannedPeers={}]",
                            partNum, tblConfiguration.name().value(), stable, ByteUtils.fromBytes(plannedEntry.value()));
                    break;
                case FINISH_REBALANCE_SUCCESS:
                    LOG.info("Rebalance finished [partition={}, table={}, appliedPeers={}]",
                            partNum, tblConfiguration.name().value(), stable);
                    break;
                default:
                    assert false : res;
                    break;
            }

            rebalanceAttempts.set(0);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            // TODO: IGNITE-14693
            LOG.warn("Unable to commit partition configuration to metastore [table = {}, partition = {}]",
                    e, tblConfiguration.name(), partNum);
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
    private static Set<Assignment> readAssignments(Entry entry) {
        byte[] value = entry.value();

        return value == null ? Set.of() : ByteUtils.fromBytes(value);
    }
}
