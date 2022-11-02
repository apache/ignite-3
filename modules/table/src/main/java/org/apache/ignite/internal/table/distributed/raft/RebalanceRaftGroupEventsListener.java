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

import static org.apache.ignite.internal.metastorage.client.CompoundCondition.and;
import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.revision;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;
import static org.apache.ignite.internal.utils.RebalanceUtil.intersect;
import static org.apache.ignite.internal.utils.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.readClusterNodes;
import static org.apache.ignite.internal.utils.RebalanceUtil.resolveClusterNodes;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.subtract;
import static org.apache.ignite.internal.utils.RebalanceUtil.switchAppendKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.switchReduceKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.union;
import static org.apache.ignite.raft.jraft.core.NodeImpl.LEADER_STEPPED_DOWN;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Condition;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.SimpleCondition;
import org.apache.ignite.internal.metastorage.client.Update;
import org.apache.ignite.internal.raft.server.RaftGroupEventsListener;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;

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
    public static final int REBALANCE_RETRY_DELAY_MS = 200;

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

    /** Unique partition id. */
    private final TablePartitionId partId;

    /** Partition number. */
    private final int partNum;

    /** Busy lock of parent component for synchronous stop. */
    private final IgniteSpinBusyLock busyLock;

    /** Executor for scheduling rebalance retries. */
    private final ScheduledExecutorService rebalanceScheduler;

    /** Function that performs a reconfiguration of a raft group of a partition. */
    private final BiFunction<List<Peer>, Long, CompletableFuture<Void>> movePartitionFn;

    /** Attempts to retry the current rebalance in case of errors. */
    private final AtomicInteger rebalanceAttempts =  new AtomicInteger(0);

    /** Function that calculates assignments for table's partition. */
    private final BiFunction<TableConfiguration, Integer, Set<ClusterNode>> calculateAssignmentsFn;

    /**
     * Constructs new listener.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param tblConfiguration Table configuration.
     * @param partId Partition id.
     * @param partNum Partition number.
     * @param busyLock Busy lock.
     * @param movePartitionFn Function that moves partition between nodes.
     * @param calculateAssignmentsFn Function that calculates assignments for table's partition.
     * @param rebalanceScheduler Executor for scheduling rebalance retries.
     */
    public RebalanceRaftGroupEventsListener(
            MetaStorageManager metaStorageMgr,
            TableConfiguration tblConfiguration,
            TablePartitionId partId,
            int partNum,
            IgniteSpinBusyLock busyLock,
            BiFunction<List<Peer>, Long, CompletableFuture<Void>> movePartitionFn,
            BiFunction<TableConfiguration, Integer, Set<ClusterNode>> calculateAssignmentsFn,
            ScheduledExecutorService rebalanceScheduler) {
        this.metaStorageMgr = metaStorageMgr;
        this.tblConfiguration = tblConfiguration;
        this.partId = partId;
        this.partNum = partNum;
        this.busyLock = busyLock;
        this.movePartitionFn = movePartitionFn;
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

                    Entry pendingEntry = metaStorageMgr.get(pendingPartAssignmentsKey(partId)).get();

                    if (!pendingEntry.empty()) {
                        Set<ClusterNode> pendingNodes = ByteUtils.fromBytes(pendingEntry.value());

                        LOG.info("New leader elected. Going to reconfigure peers [group={}, partition={}, table={}, peers={}]",
                                partId, partNum, tblConfiguration.name().value(), pendingNodes);

                        movePartitionFn.apply(clusterNodesToPeers(pendingNodes), term).join();
                    }
                } catch (InterruptedException | ExecutionException e) {
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
    public void onNewPeersConfigurationApplied(List<PeerId> peers) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            rebalanceScheduler.schedule(() -> {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    doOnNewPeersConfigurationApplied(peers);
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
    public void onReconfigurationError(Status status, List<PeerId> peers, long term) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            assert status != null;

            if (status.equals(LEADER_STEPPED_DOWN)) {
                // Leader stepped down, so we are expecting RebalanceRaftGroupEventsListener.onLeaderElected to be called on a new leader.
                LOG.info("Leader stepped down during rebalance [partId={}]", partId);

                return;
            }

            RaftError raftError = status.getRaftError();

            assert raftError == RaftError.ECATCHUP : "According to the JRaft protocol, " + RaftError.ECATCHUP
                    + " is expected, got " + raftError;

            LOG.debug("Error occurred during rebalance [partId={}]", partId);

            if (rebalanceAttempts.incrementAndGet() < REBALANCE_RETRY_THRESHOLD) {
                scheduleChangePeers(peers, term);
            } else {
                LOG.info("Number of retries for rebalance exceeded the threshold [partId={}, threshold={}]", partId,
                        REBALANCE_RETRY_THRESHOLD);

                // TODO: currently we just retry intent to change peers according to the rebalance infinitely, until new leader is elected,
                // TODO: but rebalance cancel mechanism should be implemented. https://issues.apache.org/jira/browse/IGNITE-17056
                scheduleChangePeers(peers, term);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Schedules changing peers according to the current rebalance.
     *
     * @param peers Peers to change configuration for a raft group.
     * @param term Current known leader term.
     */
    private void scheduleChangePeers(List<PeerId> peers, long term) {
        rebalanceScheduler.schedule(() -> {
            if (!busyLock.enterBusy()) {
                return;
            }

            LOG.info("Going to retry rebalance [attemptNo={}, partId={}]", rebalanceAttempts.get(), partId);

            try {
                movePartitionFn.apply(peerIdsToPeers(peers), term).join();
            } finally {
                busyLock.leaveBusy();
            }
        }, REBALANCE_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Implementation of {@link RebalanceRaftGroupEventsListener#onNewPeersConfigurationApplied(List)}.
     *
     * @param peers Peers
     */
    private void doOnNewPeersConfigurationApplied(List<PeerId> peers) {
        try {
            ByteArray pendingPartAssignmentsKey = pendingPartAssignmentsKey(partId);
            ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(partId);
            ByteArray plannedPartAssignmentsKey = plannedPartAssignmentsKey(partId);
            ByteArray switchReduceKey = switchReduceKey(partId);
            ByteArray switchAppendKey = switchAppendKey(partId);

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

            Set<ClusterNode> calculatedAssignments = calculateAssignmentsFn.apply(tblConfiguration, partNum);

            Set<ClusterNode> stable = resolveClusterNodes(peers, pendingEntry.value(), stableEntry.value());

            Set<ClusterNode> retrievedSwitchReduce = readClusterNodes(switchReduceEntry);
            Set<ClusterNode> retrievedSwitchAppend = readClusterNodes(switchAppendEntry);
            Set<ClusterNode> retrievedStable = readClusterNodes(stableEntry);

            // Were reduced
            Set<ClusterNode> reducedNodes = subtract(retrievedSwitchReduce, stable);

            // Were added
            Set<ClusterNode> addedNodes = subtract(stable, retrievedStable);

            // For further reduction
            Set<ClusterNode> calculatedSwitchReduce = subtract(retrievedSwitchReduce, reducedNodes);

            // For further addition
            Set<ClusterNode> calculatedSwitchAppend = union(retrievedSwitchAppend, reducedNodes);
            calculatedSwitchAppend = subtract(calculatedSwitchAppend, addedNodes);
            calculatedSwitchAppend = intersect(calculatedAssignments, calculatedSwitchAppend);

            Set<ClusterNode> calculatedPendingReduction = subtract(stable, retrievedSwitchReduce);

            Set<ClusterNode> calculatedPendingAddition = union(stable, reducedNodes);
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
                List<Set<ClusterNode>> assignments = ByteUtils.fromBytes(((ExtendedTableChange) ch).assignments());
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
            int res = metaStorageMgr.invoke(If.iif(retryPreconditions, successCase, failCase)).get().getAsInt();

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

                doOnNewPeersConfigurationApplied(peers);
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
     * Transforms list of cluster nodes to the list of peers.
     *
     * @param nodes List of cluster nodes to transform.
     * @return List of transformed peers.
     */
    private static List<Peer> clusterNodesToPeers(Set<ClusterNode> nodes) {
        List<Peer> peers = new ArrayList<>(nodes.size());

        for (ClusterNode node : nodes) {
            peers.add(new Peer(node.name()));
        }

        return peers;
    }

    /**
     * Transforms list of peerIds to list of peers.
     *
     * @param peerIds List of peerIds to transform.
     * @return List of transformed peers.
     */
    private static List<Peer> peerIdsToPeers(List<PeerId> peerIds) {
        List<Peer> peers = new ArrayList<>(peerIds.size());

        for (PeerId peerId : peerIds) {
            peers.add(new Peer(peerId.getConsistentId()));
        }

        return peers;
    }
}
