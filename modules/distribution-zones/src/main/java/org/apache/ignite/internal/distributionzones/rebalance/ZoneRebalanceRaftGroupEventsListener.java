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

import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.assignmentsChainKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
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
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.partitiondistribution.PendingAssignmentsCalculator.pendingAssignmentsCalculator;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CollectionUtils.intersect;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsChain;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftError;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.Status;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.TestOnly;

/**
 * Listener for the raft group events, which must provide correct error handling of rebalance process and start new rebalance after the
 * current one finished.
 */
public class ZoneRebalanceRaftGroupEventsListener implements RaftGroupEventsListener {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ZoneRebalanceRaftGroupEventsListener.class);

    /** Number of retrying of the current rebalance in case of errors. */
    private static final int REBALANCE_RETRY_THRESHOLD = 10;

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

    private final FailureProcessor failureProcessor;

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

    /** Configuration of rebalance retries delay. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> retryDelayConfiguration;

    /** Function that calculates assignments for zone's partition. */
    private final BiFunction<ZonePartitionId, Long, CompletableFuture<Set<Assignment>>> calculateAssignmentsFn;

    /**
     * Constructs new listener.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param zonePartitionId Partition id.
     * @param busyLock Busy lock.
     * @param partitionMover Class that moves partition between nodes.
     * @param rebalanceScheduler Executor for scheduling rebalance retries.
     * @param calculateAssignmentsFn Function that calculates assignments for zone's partition.
     * @param retryDelayConfiguration Configuration property for rebalance retries delay.
     */
    public ZoneRebalanceRaftGroupEventsListener(
            MetaStorageManager metaStorageMgr,
            FailureProcessor failureProcessor,
            ZonePartitionId zonePartitionId,
            IgniteSpinBusyLock busyLock,
            PartitionMover partitionMover,
            ScheduledExecutorService rebalanceScheduler,
            BiFunction<ZonePartitionId, Long, CompletableFuture<Set<Assignment>>> calculateAssignmentsFn,
            SystemDistributedConfigurationPropertyHolder<Integer> retryDelayConfiguration
    ) {
        this.metaStorageMgr = metaStorageMgr;
        this.failureProcessor = failureProcessor;
        this.zonePartitionId = zonePartitionId;
        this.busyLock = busyLock;
        this.partitionMover = partitionMover;
        this.rebalanceScheduler = rebalanceScheduler;
        this.calculateAssignmentsFn = calculateAssignmentsFn;
        this.retryDelayConfiguration = retryDelayConfiguration;
    }

    /** {@inheritDoc} */
    @Override
    public void onLeaderElected(
            long term,
            long configurationTerm,
            long configurationIndex,
            PeersAndLearners configuration,
            long sequenceToken
    ) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            rebalanceScheduler.execute(() -> {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    rebalanceAttempts.set(0);

                    // TODO https://issues.apache.org/jira/browse/IGNITE-23633
                    // First of all, it's required to reread pending assignments and recheck whether it's still needed to perform the
                    // rebalance. Worth mentioning that one of legitimate cases of metaStorageMgr.get() timeout is MG unavailability
                    // in that cases it's required to retry the request. However it's important to handle local node stopping intent,
                    // meaning that busyLock should be handled properly with thought of a throttle to provide the ability for node to stop.

                    // It's required to read pending assignments from MS leader instead of local MS in order not to catch-up stale pending
                    // ones:
                    // Let's say that node A has processed configurations С1 and C2 and therefore moved the raft to configuration C2 for
                    // partition P1.
                    // Node B was elected as partition P1 leader, however locally Node B is a bit outdated within MS timeline, thus it has
                    // C1 as pending assignments. If it'll propose C1 as "new" configuration and then fall, raft will stuck on old
                    // configuration and won't do further progress.
                    Entry entry = metaStorageMgr.get(pendingPartAssignmentsQueueKey(zonePartitionId)).get();
                    byte[] pendingAssignmentsBytes = entry.value();

                    if (pendingAssignmentsBytes != null) {
                        Set<Assignment> pendingAssignments = AssignmentsQueue.fromBytes(pendingAssignmentsBytes).poll().nodes();

                        // The race is possible between doStableSwitch processing on the node colocated with former leader
                        // and onLeaderElected of the new leader:
                        // 1. Node A that was colocated with former leader successfully moved raft from C0 to C1, however did not receive
                        // onNewPeersConfigurationApplied yet and thus didn't do doStableSwitch.
                        // 2. New node B occurred to be collocated with new leader and thus received onLeaderElected, checked pending
                        // assignments in meta storage and retried changePeersAndLearnersAsync(C1).
                        // 3. At the very same moment Node A performs doStableSwitch, meaning that it switches assignments pending from
                        // C1 to C2.
                        // 4.Node B gets meta storage notification about new pending C2 and sends changePeersAndLearnersAsync(C2)
                        // changePeersAndLearnersAsync(C1) and changePeersAndLearnersAsync(C2) from Node B may reorder.
                        // In order to eliminate this we may check raft configuration on leader election and if it matches the one in
                        // current global pending assignments call doStableSwitch instead of changePeersAndLearners since raft already on
                        // required configuration.
                        if (PeersAndLearners.fromAssignments(pendingAssignments).equals(configuration)) {
                            doStableKeySwitchWithExceptionHandling(
                                    pendingAssignments,
                                    zonePartitionId,
                                    configurationTerm,
                                    configurationIndex,
                                    calculateAssignmentsFn
                            );
                        } else {

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

                            partitionMover.movePartition(peersAndLearners, term, entry.revision())
                                    .whenComplete((unused, ex) -> {
                                        // TODO https://issues.apache.org/jira/browse/IGNITE-23633 remove !hasCause(ex, TimeoutException.class)
                                        if (ex != null && !hasCause(ex, NodeStoppingException.class) && !hasCause(ex,
                                                TimeoutException.class)) {
                                            String errorMessage = String.format(
                                                    "Unable to start rebalance [zonePartitionId=%s, term=%s]",
                                                    zonePartitionId,
                                                    term
                                            );
                                            failureProcessor.process(new FailureContext(ex, errorMessage));
                                        }
                                    });
                        }
                    }
                } catch (Exception e) {
                    // TODO: IGNITE-14693
                    // TODO https://issues.apache.org/jira/browse/IGNITE-23633 remove !hasCause(e, TimeoutException.class)
                    if (!hasCause(e, NodeStoppingException.class) && !hasCause(e, TimeoutException.class)) {
                        String errorMessage = String.format(
                                "Unable to start rebalance [zonePartitionId=%s, term=%s]",
                                zonePartitionId,
                                term
                        );
                        failureProcessor.process(new FailureContext(e, errorMessage));
                    }
                } finally {
                    busyLock.leaveBusy();
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onNewPeersConfigurationApplied(PeersAndLearners configuration, long term, long index) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            Set<Assignment> stable = createAssignments(configuration);

            rebalanceScheduler.execute(() -> {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    doStableKeySwitchWithExceptionHandling(stable, zonePartitionId, term, index, calculateAssignmentsFn);
                } finally {
                    busyLock.leaveBusy();
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onReconfigurationError(Status status, PeersAndLearners configuration, long term, long sequenceToken) {
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
                scheduleChangePeersAndLearners(configuration, term, sequenceToken);
            } else {
                LOG.info("Number of retries for rebalance exceeded the threshold [partId={}, threshold={}]", zonePartitionId,
                        REBALANCE_RETRY_THRESHOLD);

                // TODO: currently we just retry intent to change peers according to the rebalance infinitely, until new leader is elected,
                // TODO: but rebalance cancel mechanism should be implemented. https://issues.apache.org/jira/browse/IGNITE-19087
                scheduleChangePeersAndLearners(configuration, term, sequenceToken);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns current retry delay.
     *
     * @return Current retry delay
     */
    @TestOnly
    public int currentRetryDelay() {
        return retryDelayConfiguration.currentValue();
    }

    /**
     * Schedules changing peers according to the current rebalance.
     *
     * @param peersAndLearners Peers and learners.
     * @param term Current known leader term.
     */
    private void scheduleChangePeersAndLearners(PeersAndLearners peersAndLearners, long term, long revision) {
        rebalanceScheduler.schedule(() -> {
            if (!busyLock.enterBusy()) {
                return;
            }

            LOG.info("Going to retry rebalance [attemptNo={}, partId={}]", rebalanceAttempts.get(), zonePartitionId);

            try {
                partitionMover.movePartition(peersAndLearners, term, revision)
                        .whenComplete((unused, ex) -> {
                            if (ex != null && !hasCause(ex, NodeStoppingException.class)) {
                                String errorMessage = String.format("Failure while moving partition [partId=%s]", zonePartitionId);
                                failureProcessor.process(new FailureContext(ex, errorMessage));
                            }
                        });
            } finally {
                busyLock.leaveBusy();
            }
        }, retryDelayConfiguration.currentValue(), TimeUnit.MILLISECONDS);
    }

    /**
     * Updates stable value with the new applied assignment.
     */
    private void doStableKeySwitchWithExceptionHandling(
            Set<Assignment> stableFromRaft,
            ZonePartitionId zonePartitionId,
            long configurationTerm,
            long configurationIndex,
            BiFunction<ZonePartitionId, Long, CompletableFuture<Set<Assignment>>> calculateAssignmentsFn
    ) {
        doStableKeySwitch(
                stableFromRaft,
                zonePartitionId,
                configurationTerm,
                configurationIndex,
                calculateAssignmentsFn
        ).whenComplete((res, ex) -> {
            // TODO: IGNITE-14693
            if (ex != null && !hasCause(ex, NodeStoppingException.class)) {
                if (hasCause(ex, TimeoutException.class)) {
                    // TODO: https://issues.apache.org/jira/browse/IGNITE-25276 - handle this timeout properly.
                    LOG.error("Unable to commit partition configuration to metastore: {}", ex, zonePartitionId);
                } else {
                    String errorMessage = String.format("Unable to commit partition configuration to metastore: %s", zonePartitionId);
                    failureProcessor.process(new FailureContext(ex, errorMessage));
                }
            }
        });
    }

    private CompletableFuture<Void> doStableKeySwitch(
            Set<Assignment> stableFromRaft,
            ZonePartitionId zonePartitionId,
            long configurationTerm,
            long configurationIndex,
            BiFunction<ZonePartitionId, Long, CompletableFuture<Set<Assignment>>> calculateAssignmentsFn
    ) {
        ByteArray pendingPartAssignmentsKey = pendingPartAssignmentsQueueKey(zonePartitionId);
        ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(zonePartitionId);
        ByteArray plannedPartAssignmentsKey = plannedPartAssignmentsKey(zonePartitionId);
        ByteArray switchReduceKey = switchReduceKey(zonePartitionId);
        ByteArray switchAppendKey = switchAppendKey(zonePartitionId);
        ByteArray assignmentsChainKey = assignmentsChainKey(zonePartitionId);

        Set<ByteArray> keysToGet = Set.of(
                plannedPartAssignmentsKey,
                pendingPartAssignmentsKey,
                stablePartAssignmentsKey,
                switchReduceKey,
                switchAppendKey,
                assignmentsChainKey
        );
        return metaStorageMgr.getAll(keysToGet).thenCompose(values -> {
            Entry stableEntry = values.get(stablePartAssignmentsKey);
            Entry pendingEntry = values.get(pendingPartAssignmentsKey);
            Entry plannedEntry = values.get(plannedPartAssignmentsKey);
            Entry switchReduceEntry = values.get(switchReduceKey);
            Entry switchAppendEntry = values.get(switchAppendKey);
            Entry assignmentsChainEntry = values.get(assignmentsChainKey);

            AssignmentsQueue pendingAssignmentsQueue = AssignmentsQueue.fromBytes(pendingEntry.value());

            if (pendingAssignmentsQueue != null && pendingAssignmentsQueue.size() > 1) {

                if (pendingAssignmentsQueue.peekFirst().nodes().equals(stableFromRaft)) {
                    pendingAssignmentsQueue.poll(); // remove, first element was already applied to the configuration by pending listeners
                }

                Assignments stable = Assignments.of(stableFromRaft, pendingAssignmentsQueue.peekFirst().timestamp());
                pendingAssignmentsQueue = pendingAssignmentsCalculator()
                        .stable(stable)
                        .target(pendingAssignmentsQueue.peekLast())
                        .toQueue();

                final AssignmentsQueue pendingAssignmentsQueueFinal = pendingAssignmentsQueue;
                return metaStorageMgr.invoke(iif(
                        revision(pendingPartAssignmentsKey).eq(pendingEntry.revision()),
                        ops(put(pendingPartAssignmentsKey, pendingAssignmentsQueue.toBytes())).yield(true),
                        ops().yield(false)
                )).thenCompose(statementResult -> {
                    boolean updated = statementResult.getAsBoolean();

                    if (updated) {
                        LOG.info("Pending assignments queue polled and updated [zonePartitionId={}, pendingQueue={}]",
                                zonePartitionId, pendingAssignmentsQueueFinal);
                        return nullCompletedFuture(); // quit and wait for new configuration iteration
                    } else {
                        LOG.info("Pending assignments queue update retry [zonePartitionId={}, pendingQueue={}]",
                                zonePartitionId, pendingAssignmentsQueueFinal);
                        return doStableKeySwitch(
                                stableFromRaft,
                                zonePartitionId,
                                configurationTerm,
                                configurationIndex,
                                calculateAssignmentsFn
                        );
                    }
                });
            }

            Set<Assignment> retrievedStable = readAssignments(stableEntry).nodes();
            Set<Assignment> retrievedSwitchReduce = readAssignments(switchReduceEntry).nodes();
            Set<Assignment> retrievedSwitchAppend = readAssignments(switchAppendEntry).nodes();

            Assignments pendingAssignments = pendingAssignmentsQueue == null
                    ? Assignments.EMPTY
                    : pendingAssignmentsQueue.poll();

            Set<Assignment> retrievedPending = pendingAssignments.nodes();

            if (!retrievedPending.equals(stableFromRaft)) {
                return nullCompletedFuture();
            }

            // We wait for catalog metadata to be applied up to the provided timestamp, so it should be safe to use the timestamp.
            return calculateAssignmentsFn.apply(zonePartitionId, pendingAssignments.timestamp()).thenCompose(calculatedAssignments -> {
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

                long catalogTimestamp = pendingAssignments.timestamp();

                Assignments newStableAssignments = Assignments.of(stableFromRaft, catalogTimestamp);

                Operation assignmentChainChangeOp = handleAssignmentsChainChange(
                        assignmentsChainKey,
                        assignmentsChainEntry,
                        pendingAssignments,
                        newStableAssignments,
                        configurationTerm,
                        configurationIndex
                );

                Update successCase;
                Update failCase;

                byte[] stableFromRaftByteArray = newStableAssignments.toBytes();
                byte[] additionByteArray = AssignmentsQueue.toBytes(Assignments.of(calculatedPendingAddition, catalogTimestamp));
                byte[] reductionByteArray = AssignmentsQueue.toBytes(Assignments.of(calculatedPendingReduction, catalogTimestamp));
                byte[] switchReduceByteArray = Assignments.toBytes(calculatedSwitchReduce, catalogTimestamp);
                byte[] switchAppendByteArray = Assignments.toBytes(calculatedSwitchAppend, catalogTimestamp);

                if (!calculatedSwitchAppend.isEmpty()) {
                    successCase = ops(
                            put(stablePartAssignmentsKey, stableFromRaftByteArray),
                            put(pendingPartAssignmentsKey, additionByteArray),
                            put(switchReduceKey, switchReduceByteArray),
                            put(switchAppendKey, switchAppendByteArray),
                            assignmentChainChangeOp
                    ).yield(SWITCH_APPEND_SUCCESS);
                    failCase = ops().yield(SWITCH_APPEND_FAIL);
                } else if (!calculatedSwitchReduce.isEmpty()) {
                    successCase = ops(
                            put(stablePartAssignmentsKey, stableFromRaftByteArray),
                            put(pendingPartAssignmentsKey, reductionByteArray),
                            put(switchReduceKey, switchReduceByteArray),
                            put(switchAppendKey, switchAppendByteArray),
                            assignmentChainChangeOp
                    ).yield(SWITCH_REDUCE_SUCCESS);
                    failCase = ops().yield(SWITCH_REDUCE_FAIL);
                } else {
                    Condition con5;
                    if (plannedEntry.value() != null) {
                        // eq(revision(partition.assignments.planned), plannedEntry.revision)
                        con5 = revision(plannedPartAssignmentsKey).eq(plannedEntry.revision());

                        AssignmentsQueue partAssignmentsPendingQueue = pendingAssignmentsCalculator()
                                .stable(newStableAssignments)
                                .target(Assignments.fromBytes(plannedEntry.value()))
                                .toQueue();

                        successCase = ops(
                                put(stablePartAssignmentsKey, stableFromRaftByteArray),
                                put(pendingPartAssignmentsKey, partAssignmentsPendingQueue.toBytes()),
                                remove(plannedPartAssignmentsKey),
                                assignmentChainChangeOp
                        ).yield(SCHEDULE_PENDING_REBALANCE_SUCCESS);

                        failCase = ops().yield(SCHEDULE_PENDING_REBALANCE_FAIL);
                    } else {
                        // notExists(partition.assignments.planned)
                        con5 = notExists(plannedPartAssignmentsKey);

                        successCase = ops(
                                put(stablePartAssignmentsKey, stableFromRaftByteArray),
                                remove(pendingPartAssignmentsKey),
                                assignmentChainChangeOp
                        ).yield(FINISH_REBALANCE_SUCCESS);

                        failCase = ops().yield(FINISH_REBALANCE_FAIL);
                    }

                    retryPreconditions = and(retryPreconditions, con5);
                }

                Set<Assignment> finalCalculatedPendingAddition = calculatedPendingAddition;
                return metaStorageMgr.invoke(iif(retryPreconditions, successCase, failCase)).thenCompose(statementResult -> {
                    int res = statementResult.getAsInt();

                    if (res < 0) {
                        logSwitchFailure(res, stableFromRaft, zonePartitionId);

                        return doStableKeySwitch(
                                stableFromRaft,
                                zonePartitionId,
                                configurationTerm,
                                configurationIndex,
                                calculateAssignmentsFn
                        );
                    } else {
                        logSwitchSuccess(
                                res,
                                stableFromRaft,
                                zonePartitionId,
                                finalCalculatedPendingAddition,
                                calculatedPendingReduction,
                                plannedEntry
                        );
                        return nullCompletedFuture();
                    }
                });
            });
        });
    }

    private static void logSwitchFailure(int res, Set<Assignment> stableFromRaft, ZonePartitionId zonePartitionId) {
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
    }

    private static void logSwitchSuccess(
            int res,
            Set<Assignment> stableFromRaft,
            ZonePartitionId zonePartitionId,
            Set<Assignment> calculatedPendingAddition,
            Set<Assignment> calculatedPendingReduction,
            Entry plannedEntry
    ) {
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
     * @param partitions Partitions count.
     * @param replicas Replicas count.
     * @param consensusGroupSize Number of nodes in a consensus group.
     * @param partId Partition's raft group id.
     * @param event Assignments switch reduce change event.
     * @return Completable future that signifies the completion of this operation.
     */
    public static CompletableFuture<Void> handleReduceChanged(
            MetaStorageManager metaStorageMgr,
            Collection<String> dataNodes,
            int partitions,
            int replicas,
            int consensusGroupSize,
            ZonePartitionId partId,
            WatchEvent event,
            long assignmentsTimestamp
    ) {
        Entry entry = event.entryEvent().newEntry();
        byte[] eventData = entry.value();

        assert eventData != null : "Null event data for " + partId;

        Assignments switchReduce = Assignments.fromBytes(eventData);

        if (switchReduce.isEmpty()) {
            return nullCompletedFuture();
        }

        Set<Assignment> assignments = calculateAssignmentForPartition(
                dataNodes,
                partId.partitionId(),
                partitions,
                replicas,
                consensusGroupSize
        );

        ByteArray pendingKey = pendingPartAssignmentsQueueKey(partId);

        Set<Assignment> pendingAssignments = difference(assignments, switchReduce.nodes());

        byte[] pendingByteArray = AssignmentsQueue.toBytes(Assignments.of(pendingAssignments, assignmentsTimestamp));
        byte[] assignmentsByteArray = Assignments.toBytes(assignments, assignmentsTimestamp);

        ByteArray changeTriggerKey = ZoneRebalanceUtil.pendingChangeTriggerKey(partId);
        byte[] timestamp = ByteUtils.longToBytesKeepingOrder(entry.timestamp().longValue());

        // Here is what happens in the MetaStorage:
        // if ((notExists(changeTriggerKey) || value(changeTriggerKey) < revision) && (notExists(pendingKey) && notExists(stableKey)) {
        //     put(pendingKey, pending)
        //     put(stableKey, assignments)
        //     put(changeTriggerKey, revision)
        // } else if ((notExists(changeTriggerKey) || value(changeTriggerKey) < revision) && (notExists(pendingKey))) {
        //     put(pendingKey, pending)
        //     put(changeTriggerKey, revision)
        // }

        Condition changeTimestampDontExistOrLessThan = or(notExists(changeTriggerKey), value(changeTriggerKey).lt(timestamp));

        Iif resultingOperation = iif(
                and(
                        changeTimestampDontExistOrLessThan,
                        and(notExists(pendingKey), (notExists(stablePartAssignmentsKey(partId))))
                ),
                ops(
                        put(pendingKey, pendingByteArray),
                        put(stablePartAssignmentsKey(partId), assignmentsByteArray),
                        put(changeTriggerKey, timestamp)
                ).yield(),
                iif(
                        and(
                                changeTimestampDontExistOrLessThan,
                                notExists(pendingKey)
                        ),
                        ops(
                                put(pendingKey, pendingByteArray),
                                put(changeTriggerKey, timestamp)
                        ).yield(),
                        ops().yield()
                )
        );

        return metaStorageMgr.invoke(resultingOperation).thenApply(unused -> null);
    }

    private static Operation handleAssignmentsChainChange(
            ByteArray assignmentsChainKey,
            Entry assignmentsChainEntry,
            Assignments pendingAssignments,
            Assignments stableAssignments,
            long configurationTerm,
            long configurationIndex
    ) {
        // We write this key only in HA mode. See TableManager.writeTableAssignmentsToMetastore.
        if (assignmentsChainEntry.value() != null) {
            AssignmentsChain updatedAssignmentsChain = updateAssignmentsChain(
                    AssignmentsChain.fromBytes(assignmentsChainEntry.value()),
                    stableAssignments,
                    pendingAssignments,
                    configurationTerm,
                    configurationIndex
            );
            return put(assignmentsChainKey, updatedAssignmentsChain.toBytes());
        } else {
            return Operations.noop();
        }
    }

    private static AssignmentsChain updateAssignmentsChain(
            AssignmentsChain assignmentsChain,
            Assignments newStable,
            Assignments pendingAssignments,
            long configurationTerm,
            long configurationIndex
    ) {
        assert assignmentsChain != null : "Assignments chain cannot be null in HA mode.";

        assert assignmentsChain.size() > 0 : "Assignments chain cannot be empty on stable switch.";

        /*
            This method covers the following case:

            stable = [A,B,C,D,E,F,G]
            lost A B C D

            stable = [E] pending = [E F G]
            stable = [E F G]

            E F lost
            stable = [G]

            on node G restart
            ms.chain = [A,B,C,D,E,F,G] -> [E,F,G] -> [G]

          on doStableKeySwitch
              if !pending.force && !pending.secondPhaseOfReset
                ms.chain = pending/stable // [A,B,C,D,E,F,G]
              else if !pending.force && pending.secondPhaseOfReset
                ms.chain.last = stable // [A,B,C,D,E,F,G] -> [E] => [A,B,C,D,E,F,G] -> [E F G]
              else
                ms.chain = ms.chain + pending/stable // [A,B,C,D,E,F,G] => [A,B,C,D,E,F,G] -> [E]
        */
        if (!pendingAssignments.force() && !pendingAssignments.fromReset()) {
            return AssignmentsChain.of(configurationTerm, configurationIndex, newStable);
        } else if (!pendingAssignments.force() && pendingAssignments.fromReset()) {
            assignmentsChain.replaceLast(newStable, configurationTerm, configurationIndex);
        } else {
            assignmentsChain.addLast(newStable, configurationTerm, configurationIndex);
        }

        return assignmentsChain;
    }
}
