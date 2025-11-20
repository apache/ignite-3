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

import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.assignmentsChainKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.switchAppendKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.switchReduceKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.union;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.partitiondistribution.PendingAssignmentsCalculator.pendingAssignmentsCalculator;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CollectionUtils.intersect;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

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
import org.apache.ignite.internal.metastorage.dsl.Condition;
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
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.TestOnly;

/**
 * Listener for the raft group events, which must provide correct error handling of rebalance process
 * and start new rebalance after the current one finished.
 * TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this class and use {@link ZoneRebalanceRaftGroupEventsListener} instead
 *  after switching to zone-based replication.
 */
public class RebalanceRaftGroupEventsListener implements RaftGroupEventsListener {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RebalanceRaftGroupEventsListener.class);

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
    private final TablePartitionId tablePartitionId;

    /** Busy lock of parent component for synchronous stop. */
    private final IgniteSpinBusyLock busyLock;

    /** Executor for scheduling rebalance retries. */
    private final ScheduledExecutorService rebalanceScheduler;

    /** Performs reconfiguration of a Raft group of a partition. */
    private final PartitionMover partitionMover;

    /** Attempts to retry the current rebalance in case of errors. */
    private final AtomicInteger rebalanceAttempts =  new AtomicInteger(0);

    /** Configuration of rebalance retries delay. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> retryDelayConfiguration;

    /** Function that calculates assignments for table's partition. */
    private final BiFunction<TablePartitionId, Long, CompletableFuture<Set<Assignment>>> calculateAssignmentsFn;

    /**
     * Constructs new listener.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param failureProcessor Failure processor.
     * @param tablePartitionId Partition id.
     * @param busyLock Busy lock.
     * @param partitionMover Class that moves partition between nodes.
     * @param calculateAssignmentsFn Function that calculates assignments for table's partition.
     * @param rebalanceScheduler Executor for scheduling rebalance retries.
     * @param retryDelayConfiguration Configuration property for rebalance retries delay.
     */
    public RebalanceRaftGroupEventsListener(
            MetaStorageManager metaStorageMgr,
            FailureProcessor failureProcessor,
            TablePartitionId tablePartitionId,
            IgniteSpinBusyLock busyLock,
            PartitionMover partitionMover,
            BiFunction<TablePartitionId, Long, CompletableFuture<Set<Assignment>>> calculateAssignmentsFn,
            ScheduledExecutorService rebalanceScheduler,
            SystemDistributedConfigurationPropertyHolder<Integer> retryDelayConfiguration
    ) {
        this.metaStorageMgr = metaStorageMgr;
        this.failureProcessor = failureProcessor;
        this.tablePartitionId = tablePartitionId;
        this.busyLock = busyLock;
        this.partitionMover = partitionMover;
        this.calculateAssignmentsFn = calculateAssignmentsFn;
        this.rebalanceScheduler = rebalanceScheduler;
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
       // no-op
    }

    /** {@inheritDoc} */
    @Override
    public void onNewPeersConfigurationApplied(PeersAndLearners configuration, long term, long index, long sequenceToken) {
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
                    doStableKeySwitchWithExceptionHandling(stable, tablePartitionId, term, index, calculateAssignmentsFn);
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
                LOG.info("Leader stepped down during rebalance [partId={}]", tablePartitionId);

                return;
            }

            RaftError raftError = status.error();

            assert raftError == RaftError.ECATCHUP : "According to the JRaft protocol, " + RaftError.ECATCHUP
                    + " is expected, got " + raftError;

            LOG.debug("Error occurred during rebalance [partId={}]", tablePartitionId);

            if (rebalanceAttempts.incrementAndGet() < REBALANCE_RETRY_THRESHOLD) {
                scheduleChangePeersAndLearners(configuration, term, sequenceToken);
            } else {
                LOG.info("Number of retries for rebalance exceeded the threshold [partId={}, threshold={}]", tablePartitionId,
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
    private void scheduleChangePeersAndLearners(PeersAndLearners peersAndLearners, long term, long sequenceToken) {
        rebalanceScheduler.schedule(() -> {
            if (!busyLock.enterBusy()) {
                return;
            }

            LOG.info("Going to retry rebalance [attemptNo={}, partId={}]", rebalanceAttempts.get(), tablePartitionId);

            try {
                partitionMover.movePartition(peersAndLearners, term, sequenceToken)
                        .whenComplete((unused, ex) -> {
                            if (ex != null && !hasCause(ex, NodeStoppingException.class)) {
                                String errorMessage = String.format("Failure while moving partition [partId=%s]", tablePartitionId);
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
            TablePartitionId tablePartitionId,
            long configurationTerm,
            long configurationIndex,
            BiFunction<TablePartitionId, Long, CompletableFuture<Set<Assignment>>> calculateAssignmentsFn
    ) {
        doStableKeySwitch(
                stableFromRaft,
                tablePartitionId,
                configurationTerm,
                configurationIndex,
                calculateAssignmentsFn
        ).whenComplete((res, ex) -> {
            // TODO: IGNITE-14693
            if (ex != null && !hasCause(ex, NodeStoppingException.class)) {
                if (hasCause(ex, TimeoutException.class)) {
                    // TODO: https://issues.apache.org/jira/browse/IGNITE-25276 - handle this timeout properly.
                    LOG.error("Unable to commit partition configuration to metastore: {}", ex, tablePartitionId);
                } else {
                    String errorMessage = String.format("Unable to commit partition configuration to metastore: %s", tablePartitionId);
                    failureProcessor.process(new FailureContext(ex, errorMessage));
                }
            }
        });
    }

    private CompletableFuture<Void> doStableKeySwitch(
            Set<Assignment> stableFromRaft,
            TablePartitionId tablePartitionId,
            long configurationTerm,
            long configurationIndex,
            BiFunction<TablePartitionId, Long, CompletableFuture<Set<Assignment>>> calculateAssignmentsFn
    ) {
        ByteArray pendingPartAssignmentsKey = pendingPartAssignmentsQueueKey(tablePartitionId);
        ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(tablePartitionId);
        ByteArray plannedPartAssignmentsKey = plannedPartAssignmentsKey(tablePartitionId);
        ByteArray switchReduceKey = switchReduceKey(tablePartitionId);
        ByteArray switchAppendKey = switchAppendKey(tablePartitionId);
        ByteArray assignmentsChainKey = assignmentsChainKey(tablePartitionId);

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
                        LOG.info("Pending assignments queue polled and updated [tablePartitionId={}, pendingQueue={}]",
                                tablePartitionId, pendingAssignmentsQueueFinal);
                        // quit and wait for new configuration switch iteration
                        return nullCompletedFuture();
                    } else {
                        LOG.info("Pending assignments queue update retry [tablePartitionId={}, pendingQueue={}]",
                                tablePartitionId, pendingAssignmentsQueueFinal
                        );

                        return doStableKeySwitch(
                                stableFromRaft,
                                tablePartitionId,
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
            return calculateAssignmentsFn.apply(tablePartitionId, pendingAssignments.timestamp()).thenCompose(calculatedAssignments -> {
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
                        logSwitchFailure(res, stableFromRaft, tablePartitionId);

                        return doStableKeySwitch(
                                stableFromRaft,
                                tablePartitionId,
                                configurationTerm,
                                configurationIndex,
                                calculateAssignmentsFn
                        );
                    } else {
                        logSwitchSuccess(
                                res,
                                stableFromRaft,
                                tablePartitionId,
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

    private static void logSwitchFailure(int res, Set<Assignment> stableFromRaft, TablePartitionId tablePartitionId) {
        switch (res) {
            case SWITCH_APPEND_FAIL:
                LOG.info("Rebalance keys changed while trying to update rebalance pending addition information. "
                                + "Going to retry [tablePartitionID={}, appliedPeers={}]",
                        tablePartitionId, stableFromRaft
                );
                break;
            case SWITCH_REDUCE_FAIL:
                LOG.info("Rebalance keys changed while trying to update rebalance pending reduce information. "
                                + "Going to retry [tablePartitionID={}, appliedPeers={}]",
                        tablePartitionId, stableFromRaft
                );
                break;
            case SCHEDULE_PENDING_REBALANCE_FAIL:
            case FINISH_REBALANCE_FAIL:
                LOG.info("Rebalance keys changed while trying to update rebalance information. "
                                + "Going to retry [tablePartitionId={}, appliedPeers={}]",
                        tablePartitionId, stableFromRaft
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
            TablePartitionId tablePartitionId,
            Set<Assignment> calculatedPendingAddition,
            Set<Assignment> calculatedPendingReduction,
            Entry plannedEntry
    ) {
        switch (res) {
            case SWITCH_APPEND_SUCCESS:
                LOG.info("Rebalance finished. Going to schedule next rebalance with addition"
                                + " [tablePartitionId={}, appliedPeers={}, plannedPeers={}]",
                        tablePartitionId, stableFromRaft, calculatedPendingAddition
                );
                break;
            case SWITCH_REDUCE_SUCCESS:
                LOG.info("Rebalance finished. Going to schedule next rebalance with reduction"
                                + " [tablePartitionId={}, appliedPeers={}, plannedPeers={}]",
                        tablePartitionId, stableFromRaft, calculatedPendingReduction
                );
                break;
            case SCHEDULE_PENDING_REBALANCE_SUCCESS:
                LOG.info(
                        "Rebalance finished. Going to schedule next rebalance [tablePartitionId={}, appliedPeers={}, plannedPeers={}]",
                        tablePartitionId, stableFromRaft, Assignments.fromBytes(plannedEntry.value()).nodes()
                );
                break;
            case FINISH_REBALANCE_SUCCESS:
                LOG.info("Rebalance finished [tablePartitionId={}, appliedPeers={}]", tablePartitionId, stableFromRaft);
                break;

            default:
                assert false : res;
                break;
        }
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
}
