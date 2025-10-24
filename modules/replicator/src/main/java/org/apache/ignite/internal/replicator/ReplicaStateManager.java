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

package org.apache.ignite.internal.replicator;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicaManager.WeakReplicaStopReason;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates logic related to replica state transitions.
 */
class ReplicaStateManager {
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaStateManager.class);

    private final Map<ReplicationGroupId, ReplicaStateContext> replicaContexts = new ConcurrentHashMap<>();

    private final Executor replicaStartStopExecutor;

    private final PlacementDriver placementDriver;

    private final ReplicaManager replicaManager;

    private final FailureProcessor failureProcessor;

    private volatile UUID localNodeId;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    ReplicaStateManager(
            Executor replicaStartStopExecutor,
            PlacementDriver placementDriver,
            ReplicaManager replicaManager,
            FailureProcessor failureProcessor
    ) {
        this.replicaStartStopExecutor = replicaStartStopExecutor;
        this.placementDriver = placementDriver;
        this.replicaManager = replicaManager;
        this.failureProcessor = failureProcessor;
    }

    void start(UUID localNodeId) {
        this.localNodeId = localNodeId;
        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, this::onPrimaryElected);
        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, this::onPrimaryExpired);
    }

    void stop() {
        busyLock.block();
    }

    private CompletableFuture<Boolean> onPrimaryElected(PrimaryReplicaEventParameters parameters) {
        // Busy lock guarding because on node stop we shouldn't handle primary replica events anymore.
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            ReplicationGroupId replicationGroupId = parameters.groupId();
            ReplicaStateContext context = getContext(replicationGroupId);

            synchronized (context) {
                if (localNodeId.equals(parameters.leaseholderId())) {
                    assert context.replicaState != ReplicaState.STOPPED
                            : "Unexpected primary replica state STOPPED [groupId=" + replicationGroupId
                            + ", leaseStartTime=" + parameters.startTime() + ", reservedForPrimary=" + context.reservedForPrimary
                            + ", contextLeaseStartTime=" + context.leaseStartTime + "].";
                } else if (context.reservedForPrimary) {
                    context.assertReservation(replicationGroupId);

                    // Unreserve if another replica was elected as primary, only if its lease start time is greater,
                    // otherwise it means that event is too late relatively to lease negotiation start and should be ignored.
                    if (parameters.startTime().compareTo(context.leaseStartTime) > 0) {
                        context.releaseReservation();
                    }
                }
            }

            return falseCompletedFuture();
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Boolean> onPrimaryExpired(PrimaryReplicaEventParameters parameters) {
        // Busy lock guarding because on node stop we shouldn't handle primary replica events anymore.
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            if (localNodeId.equals(parameters.leaseholderId())) {
                ReplicaStateContext context = replicaContexts.get(parameters.groupId());

                if (context != null) {
                    synchronized (context) {
                        if (context.reservedForPrimary) {
                            context.assertReservation(parameters.groupId());

                            // Unreserve if primary replica expired, only if its lease start time is equal to reservation time,
                            // otherwise it means that event is too late relatively to lease negotiation start and should be ignored.
                            if (parameters.startTime().equals(context.leaseStartTime)) {
                                context.releaseReservation();
                            }
                        }
                    }
                }
            }

            return falseCompletedFuture();
        } finally {
            busyLock.leaveBusy();
        }
    }

    private ReplicaStateContext getContext(ReplicationGroupId groupId) {
        return replicaContexts.computeIfAbsent(groupId,
                // Treat the absence in the map as STOPPED.
                k -> new ReplicaStateContext(ReplicaState.STOPPED, nullCompletedFuture()));
    }

    /**
     * Can possibly start replica if it's not running or is stopping.
     *
     * @param groupId Group id.
     * @param startOperation Replica start operation.
     * @param forcedAssignments Assignments to reset forcibly, if needed. Assignments reset is only available when replica is
     *         started.
     * @return Completable future, the result means whether the replica was started.
     */
    CompletableFuture<Boolean> weakStartReplica(
            ReplicationGroupId groupId,
            Supplier<CompletableFuture<Boolean>> startOperation,
            @Nullable Assignments forcedAssignments,
            long revision
    ) {
        ReplicaStateContext context = getContext(groupId);

        synchronized (context) {
            ReplicaState state = context.replicaState;

            LOG.debug("Weak replica start [grp={}, state={}, future={}].", groupId, state, context.previousOperationFuture);

            if (state == ReplicaState.STOPPED || state == ReplicaState.STOPPING) {
                return startReplica(groupId, context, startOperation);
            } else if (state == ReplicaState.ASSIGNED) {
                if (forcedAssignments != null) {
                    assert forcedAssignments.force() :
                            format("Unexpected assignments to force [assignments={}, groupId={}].", forcedAssignments, groupId);

                    replicaManager.resetPeers(groupId, fromAssignments(forcedAssignments.nodes()), revision);
                }

                // Telling the caller that the replica is started.
                return trueCompletedFuture();
            } else if (state == ReplicaState.PRIMARY_ONLY) {
                context.replicaState = ReplicaState.ASSIGNED;

                LOG.debug("Weak replica start complete [state={}].", context.replicaState);

                return trueCompletedFuture();
            } else if (state == ReplicaState.RESTART_PLANNED) {
                throw new AssertionError("Replica start cannot begin before stop on replica restart is completed [groupId="
                        + groupId + "].");
            } // else no-op.

            throw new AssertionError("Replica start cannot begin while the replica is being started [groupId=" + groupId + "].");
        }
    }

    private CompletableFuture<Boolean> startReplica(
            ReplicationGroupId groupId,
            ReplicaStateContext context,
            Supplier<CompletableFuture<Boolean>> startOperation
    ) {
        context.replicaState = ReplicaState.STARTING;
        context.previousOperationFuture = context.previousOperationFuture
                .handleAsync((v, e) -> startOperation.get(), replicaStartStopExecutor)
                .thenCompose(startOperationFuture -> startOperationFuture.thenApply(partitionStarted -> {
                    synchronized (context) {
                        if (partitionStarted) {
                            context.replicaState = ReplicaState.ASSIGNED;
                        } else {
                            context.replicaState = ReplicaState.STOPPED;
                            replicaContexts.remove(groupId);
                        }
                    }

                    LOG.debug("Weak replica start complete [state={}, partitionStarted={}].", context.replicaState, partitionStarted);

                    return partitionStarted;
                }))
                .whenComplete((res, ex) -> {
                    if (ex != null && !hasCause(ex, NodeStoppingException.class, TransientReplicaStartException.class)) {
                        failureProcessor.process(new FailureContext(ex, String.format("Replica start failed [groupId=%s]", groupId)));
                    }
                });

        return context.previousOperationFuture;
    }

    /**
     * Can possibly stop replica if it is running or starting, and is not a primary replica. Relies on the given reason. If the reason is
     * {@link WeakReplicaStopReason#EXCLUDED_FROM_ASSIGNMENTS} then the replica can be not stopped if it is still a primary. If the reason
     * is {@link WeakReplicaStopReason#PRIMARY_EXPIRED} then the replica is stopped only if its state is {@link ReplicaState#PRIMARY_ONLY},
     * because this assumes that it was excluded from assignments before.
     *
     * @param groupId Group id.
     * @param reason Reason to stop replica.
     * @param stopOperation Replica stop operation.
     * @return Completable future, the result means whether the replica was stopped.
     */
    CompletableFuture<Void> weakStopReplica(
            ReplicationGroupId groupId,
            WeakReplicaStopReason reason,
            Supplier<CompletableFuture<Void>> stopOperation
    ) {
        ReplicaStateContext context = getContext(groupId);

        synchronized (context) {
            ReplicaState state = context.replicaState;

            LOG.debug("Weak replica stop [grpId={}, state={}, reason={}, reservedForPrimary={}, future={}].", groupId, state,
                    reason, context.reservedForPrimary, context.previousOperationFuture);

            if (reason == WeakReplicaStopReason.EXCLUDED_FROM_ASSIGNMENTS) {
                if (state == ReplicaState.ASSIGNED) {
                    if (context.reservedForPrimary) {
                        context.replicaState = ReplicaState.PRIMARY_ONLY;
                        // Intentionally do not return future here: it can freeze the handling of assignment changes.
                        planDeferredReplicaStop(groupId, context, stopOperation);
                    } else {
                        return stopReplica(groupId, context, stopOperation);
                    }
                } else if (state == ReplicaState.STARTING) {
                    return stopReplica(groupId, context, stopOperation);
                } else if (state == ReplicaState.STOPPED) {
                    // We need to stop replica and destroy storages anyway, because they can be already created.
                    return stopReplica(groupId, context, stopOperation);
                } // else: no-op.
            } else if (reason == WeakReplicaStopReason.RESTART) {
                // Explicit restart: always stop.
                if (context.reservedForPrimary) {
                    // If is primary, turning off the primary first.
                    context.replicaState = ReplicaState.RESTART_PLANNED;

                    return replicaManager.stopLeaseProlongation(groupId, null)
                            .thenCompose(unused -> planDeferredReplicaStop(groupId, context, stopOperation));
                } else {
                    return stopReplica(groupId, context, stopOperation);
                }
            } else {
                assert reason == WeakReplicaStopReason.PRIMARY_EXPIRED : "Unknown replica stop reason: " + reason;

                if (state == ReplicaState.PRIMARY_ONLY) {
                    return stopReplica(groupId, context, stopOperation);
                } // else: no-op.
            }
            // State #RESTART_PLANNED is also no-op because replica will be stopped within deferred operation.

            LOG.debug("Weak replica stop (sync part) complete [grpId={}, state={}].", groupId, context.replicaState);

            return nullCompletedFuture();
        }
    }

    /**
     * Should be called within {@code synchronized (context)}.
     *
     * @param groupId  Group id.
     * @param context Replica state context.
     * @param stopOperation Replica stop operation.
     * @return Completable future that completes when the replica is stopped.
     */
    private CompletableFuture<Void> stopReplica(
            ReplicationGroupId groupId,
            ReplicaStateContext context,
            Supplier<CompletableFuture<Void>> stopOperation
    ) {
        // These is some probability that the replica would be reserved after the previous lease is expired and before this method
        // is called, so reservation state needs to be checked again.
        if (context.reservedForPrimary) {
            return replicaManager.stopLeaseProlongation(groupId, null)
                    .thenCompose(unused -> planDeferredReplicaStop(groupId, context, stopOperation));
        }

        context.replicaState = ReplicaState.STOPPING;
        context.previousOperationFuture = context.previousOperationFuture
                .handleAsync((v, e) -> stopOperation.get(), replicaStartStopExecutor)
                .thenCompose(stopOperationFuture -> stopOperationFuture.thenApply(v -> {
                    synchronized (context) {
                        context.replicaState = ReplicaState.STOPPED;
                    }

                    LOG.debug("Weak replica stop complete [grpId={}, state={}].", groupId, context.replicaState);

                    return true;
                }))
                .whenComplete((res, ex) -> {
                    if (ex != null && !hasCause(ex, NodeStoppingException.class)) {
                        failureProcessor.process(new FailureContext(ex, String.format("Replica stop failed [groupId=%s]", groupId)));
                    }
                });

        return context.previousOperationFuture.thenApply(v -> null);
    }

    private CompletableFuture<Void> planDeferredReplicaStop(
            ReplicationGroupId groupId,
            ReplicaStateContext context,
            Supplier<CompletableFuture<Void>> deferredStopOperation
    ) {
        synchronized (context) {
            LOG.debug("Planning deferred replica stop [groupId={}, reservedForPrimary={}].", groupId, context.reservedForPrimary);

            // No parallel actions affected this, continue.
            if (context.reservedForPrimary) {
                context.deferredStopReadyFuture = new CompletableFuture<>();
            } else {
                // context.unreserve() has been called in parallel, no need to wait.
                context.deferredStopReadyFuture = nullCompletedFuture();
            }

            return context.deferredStopReadyFuture
                    .thenCompose(unused -> {
                        synchronized (context) {
                            return stopReplica(groupId, context, deferredStopOperation);
                        }
                    });
        }
    }

    /**
     * Reserve replica as primary.
     *
     * @param groupId Group id.
     */
    void reserveReplica(ReplicationGroupId groupId, HybridTimestamp leaseStartTime) {
        ReplicaStateContext context = getContext(groupId);

        synchronized (context) {
            LOG.debug("Trying to reserve replica [groupId={}, leaseStartTime={}, replicaState={}, reservedForPrimary={}].",
                    groupId, leaseStartTime, context.replicaState, context.reservedForPrimary);

            ReplicaState state = context.replicaState;

            if (state == ReplicaState.STOPPING || state == ReplicaState.STOPPED) {
                if (state == ReplicaState.STOPPED) {
                    replicaContexts.remove(groupId);
                }

                if (context.reservedForPrimary) {
                    throw new AssertionError("Unexpected replica reservation with " + state + " state [groupId=" + groupId + "].");
                }
            } else if (state == ReplicaState.RESTART_PLANNED) {
                // Explicitly release reservation if the replica is going to restart, otherwise placement driver will always try to reserve
                // it again before the replica stopping begins.
                context.releaseReservation();
            } else {
                context.reserve(groupId, leaseStartTime);
            }

            if (!context.reservedForPrimary) {
                throw new ReplicaReservationFailedException(format(
                        "Replica reservation failed [groupId={}, leaseStartTime={}, currentReplicaState={}].",
                        groupId,
                        leaseStartTime,
                        state
                ));
            }
        }
    }

    /**
     * Replica lifecycle states.
     * <br>
     * Transitions:
     * <br>
     * On {@link #weakStartReplica(ReplicationGroupId, Supplier, Assignments, long)} (this assumes that the replica is included into
     * assignments):
     * <ul>
     *     <li>if {@link #ASSIGNED}: next state is {@link #ASSIGNED};</li>
     *     <li>if {@link #PRIMARY_ONLY}: next state is {@link #ASSIGNED};</li>
     *     <li>if {@link #STOPPED} or {@link #STOPPING}: next state is {@link #STARTING}, replica is started after stop operation
     *         completes;</li>
     *     <li>if {@link #RESTART_PLANNED}: produces {@link AssertionError} because replica should be stopped first;</li>
     *     <li>if {@link #STARTING}: produces {@link AssertionError}.</li>
     * </ul>
     * On {@link #weakStopReplica(ReplicationGroupId, WeakReplicaStopReason, Supplier)} the next state also depends on given
     * {@link WeakReplicaStopReason}:
     * <ul>
     *     <li>if {@link WeakReplicaStopReason#EXCLUDED_FROM_ASSIGNMENTS}:
     *     <ul>
     *         <li>if {@link #ASSIGNED}: when {@link ReplicaStateContext#reservedForPrimary} is {@code true} then the next state
     *             is {@link #PRIMARY_ONLY}, otherwise the replica is stopped, the next state is {@link #STOPPING};</li>
     *         <li>if {@link #PRIMARY_ONLY} or {@link #STOPPING}: no-op.</li>
     *         <li>if {@link #RESTART_PLANNED} no-op, because replica will be stopped within deferred operation;</li>
     *         <li>if {@link #STARTING}: replica is stopped, the next state is {@link #STOPPING};</li>
     *         <li>if {@link #STOPPED}: replica is stopped.</li>
     *     </ul>
     *     </li>
     *     <li>if {@link WeakReplicaStopReason#PRIMARY_EXPIRED}:
     *     <ul>
     *         <li>if {@link #PRIMARY_ONLY} replica is stopped, the next state is {@link #STOPPING}. Otherwise no-op.</li>
     *     </ul>
     *     </li>
     *     <li>if {@link WeakReplicaStopReason#RESTART}: this is explicit manual replica restart for disaster recovery purposes,
     *         replica is stopped. It happens immediately if it's <b>not</b> reserved as primary, the next state is {@link #STOPPING}. But
     *         if if is reserved as primary, it asks the lease placement driver to stop the prolongation of lease, and is transferred
     *         to the state {@link #RESTART_PLANNED}. When the lease is expired, the replica is stopped and transferred to
     *         {@link #STOPPING} state.</li>
     * </ul>
     */
    private enum ReplicaState {
        /** Replica is starting. */
        STARTING,

        /**
         * Local node, where the replica is located, is included into the union of stable and pending assignments. The replica can be either
         * primary or non-primary. Assumes that the replica is started.
         */
        ASSIGNED,

        /**
         * Local node is excluded from the union of stable and pending assignments but the replica is a primary replica and hence can't be
         * stopped. Assumes that the replica is started.
         */
        PRIMARY_ONLY,

        /**
         * Replica is going to be restarted, this state means that it is primary and needs to wait for lease expiration first. After lease
         * is expired, replica is stopped and transferred to {@link ReplicaState#STOPPING} state.
         */
        RESTART_PLANNED,

        /** Replica is stopping. */
        STOPPING,

        /** Replica is stopped. */
        STOPPED
    }

    private static class ReplicaStateContext {
        /** Replica state. */
        ReplicaState replicaState;

        /**
         * Future of the previous operation, to linearize the starts and stops of replica. The result of the future is whether the operation
         * was actually performed (for example, partition start operation can not start replica or raft node locally).
         */
        CompletableFuture<Boolean> previousOperationFuture;

        /**
         * Whether the replica is reserved to serve as a primary even if it is not included into assignments. If it is {@code} true, then
         * {@link #weakStopReplica(ReplicationGroupId, WeakReplicaStopReason, Supplier)} transfers {@link ReplicaState#ASSIGNED} to
         * {@link ReplicaState#PRIMARY_ONLY} instead of {@link ReplicaState#STOPPING}. Replica is reserved when it is in progress of lease
         * negotiation and stays reserved when it's primary. The negotiation moves this flag to {@code true}. Primary replica expiration or
         * the election of different node as a leaseholder moves this flag to {@code false}.
         */
        boolean reservedForPrimary;

        /**
         * Lease start time of the lease this replica is reserved for, not {@code null} if {@link #reservedForPrimary} is {@code true}.
         */
        @Nullable
        HybridTimestamp leaseStartTime;

        /**
         * Future that should be complete when the deferred stop operation is ready to begin. Deferred stop operation is the stop of replica
         * that was reserved for becoming primary, and needs to be stopped.
         */
        @Nullable
        CompletableFuture<Void> deferredStopReadyFuture;

        ReplicaStateContext(ReplicaState replicaState, CompletableFuture<Boolean> previousOperationFuture) {
            this.replicaState = replicaState;
            this.previousOperationFuture = previousOperationFuture;
        }

        void reserve(ReplicationGroupId groupId, HybridTimestamp leaseStartTime) {
            if (reservedForPrimary && this.leaseStartTime != null && leaseStartTime.compareTo(this.leaseStartTime) < 0) {
                // Newer lease may reserve this replica when it's already reserved by older one: this means than older one is no longer
                // valid and most likely has not been negotiated and is discarded. By the same reason we shouldn't process the attempt
                // of reservation by older lease, which is not likely and means reordering of message handling.
                throw new ReplicaReservationFailedException(format("Replica reservation failed: newer lease has already reserved this "
                                + "replica [groupId={}, requestedLeaseStartTime={}, newerLeaseStartTime={}].", groupId, leaseStartTime,
                        this.leaseStartTime));
            }

            LOG.debug("Reserving replica [groupId={}, leaseStartTime={}].", groupId, leaseStartTime);

            this.leaseStartTime = leaseStartTime;
            this.reservedForPrimary = true;
        }

        void releaseReservation() {
            reservedForPrimary = false;
            leaseStartTime = null;

            // Unblock the deferred replica stop, if needed.
            if (deferredStopReadyFuture != null
                    && (replicaState == ReplicaState.PRIMARY_ONLY || replicaState == ReplicaState.RESTART_PLANNED)) {
                deferredStopReadyFuture.complete(null);
                deferredStopReadyFuture = null;
            }
        }

        void assertReservation(ReplicationGroupId groupId) {
            assert reservedForPrimary : "Replica is elected as primary but not reserved [groupId="
                    + groupId + ", leaseStartTime=" + leaseStartTime + "].";
            assert leaseStartTime != null : "Replica is reserved but lease start time is null [groupId="
                    + groupId + ", leaseStartTime=" + leaseStartTime + "].";
        }
    }
}
