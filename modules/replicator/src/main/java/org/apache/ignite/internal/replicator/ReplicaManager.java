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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.AFTER_REPLICA_STARTED;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.BEFORE_REPLICA_STOPPED;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.thread.ThreadOperation.TX_STATE_STORAGE_ACCESS;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.exception.ExpectedReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicaIsAlreadyStartedException;
import org.apache.ignite.internal.replicator.exception.ReplicaStoppingException;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.AwaitReplicaRequest;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReadOnlyDirectReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.replicator.message.WaitReplicaStateMessage;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.thread.ThreadAttributes;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Replica manager maintains {@link Replica} instances on an Ignite node.
 *
 * <p>Manager allows starting, stopping, getting a {@link Replica} by its unique id.
 *
 * <p>Only a single instance of the class exists in Ignite node.
 */
public class ReplicaManager extends AbstractEventProducer<LocalReplicaEvent, LocalReplicaEventParameters> implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaManager.class);

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Meta storage node names. */
    private final CompletableFuture<Set<String>> msNodes = new CompletableFuture<>();

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Cluster group manager. */
    private final ClusterManagementGroupManager cmgMgr;

    /** Replica message handler. */
    private final NetworkMessageHandler handler;

    /** Message handler for placement driver messages. */
    private final NetworkMessageHandler placementDriverMessageHandler;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    private final LongSupplier idleSafeTimePropagationPeriodMsSupplier;

    /** Replicas. */
    private final ConcurrentHashMap<ReplicationGroupId, CompletableFuture<Replica>> replicas = new ConcurrentHashMap<>();

    private final ClockService clockService;

    private final ConcurrentHashMap<ZonePartitionId, Set<ReplicationGroupId>> zonePartIdToTablePartId = new ConcurrentHashMap<>();

    /** Scheduled executor for idle safe time sync. */
    private final ScheduledExecutorService scheduledIdleSafeTimeSyncExecutor;

    private final ScheduledExecutorService scheduledTableLeaseUpdateExecutor;

    private final Executor requestsExecutor;

    private final FailureProcessor failureProcessor;

    /** Set of message groups to handler as replica requests. */
    private final Set<Class<?>> messageGroupsToHandle;

    private String localNodeId;

    /**
     * Constructor for a replica service.
     *
     * @param nodeName Node name.
     * @param clusterNetSvc Cluster network service.
     * @param cmgMgr Cluster group manager.
     * @param clockService Clock service.
     * @param messageGroupsToHandle Message handlers.
     * @param placementDriver A placement driver.
     */
    @TestOnly
    public ReplicaManager(
            String nodeName,
            ClusterService clusterNetSvc,
            ClusterManagementGroupManager cmgMgr,
            ClockService clockService,
            Set<Class<?>> messageGroupsToHandle,
            PlacementDriver placementDriver,
            Executor requestsExecutor,
            FailureProcessor failureProcessor
    ) {
        this(
                nodeName,
                clusterNetSvc,
                cmgMgr,
                clockService,
                messageGroupsToHandle,
                placementDriver,
                requestsExecutor,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                failureProcessor
        );
    }

    /**
     * Constructor for a replica service.
     *
     * @param nodeName Node name.
     * @param clusterNetSvc Cluster network service.
     * @param cmgMgr Cluster group manager.
     * @param clockService Clock service.
     * @param messageGroupsToHandle Message handlers.
     * @param placementDriver A placement driver.
     * @param requestsExecutor Executor that will be used to execute requests by replicas.
     * @param idleSafeTimePropagationPeriodMsSupplier Used to get idle safe time propagation period in ms.
     */
    public ReplicaManager(
            String nodeName,
            ClusterService clusterNetSvc,
            ClusterManagementGroupManager cmgMgr,
            ClockService clockService,
            Set<Class<?>> messageGroupsToHandle,
            PlacementDriver placementDriver,
            Executor requestsExecutor,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier,
            FailureProcessor failureProcessor
    ) {
        this.clusterNetSvc = clusterNetSvc;
        this.cmgMgr = cmgMgr;
        this.clockService = clockService;
        this.messageGroupsToHandle = messageGroupsToHandle;
        this.handler = this::onReplicaMessageReceived;
        this.placementDriverMessageHandler = this::onPlacementDriverMessageReceived;
        this.placementDriver = placementDriver;
        this.requestsExecutor = requestsExecutor;
        this.idleSafeTimePropagationPeriodMsSupplier = idleSafeTimePropagationPeriodMsSupplier;
        this.failureProcessor = failureProcessor;

        scheduledIdleSafeTimeSyncExecutor = Executors.newScheduledThreadPool(
                1,
                NamedThreadFactory.create(nodeName, "scheduled-idle-safe-time-sync-thread", LOG)
        );

        scheduledTableLeaseUpdateExecutor = Executors.newScheduledThreadPool(
                1,
                NamedThreadFactory.create(nodeName, "scheduled-table-lease-update-thread", LOG)
        );
    }

    private void onReplicaMessageReceived(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
        if (!(message instanceof ReplicaRequest)) {
            return;
        }

        assert correlationId != null;

        ReplicaRequest request = (ReplicaRequest) message;

        // If the request actually came from the network, we are already in the correct thread that has permissions to do storage reads
        // and writes.
        // But if this is a local call (in the same Ignite instance), we might still be in a thread that does not have those permissions.
        if (shouldSwitchToRequestsExecutor()) {
            requestsExecutor.execute(() -> handleReplicaRequest(request, sender, correlationId));
        } else {
            handleReplicaRequest(request, sender, correlationId);
        }
    }

    private static boolean shouldSwitchToRequestsExecutor() {
        if (Thread.currentThread() instanceof ThreadAttributes) {
            ThreadAttributes thread = (ThreadAttributes) Thread.currentThread();
            return !thread.allows(STORAGE_READ) || !thread.allows(STORAGE_WRITE) || !thread.allows(TX_STATE_STORAGE_ACCESS);
        } else {
            if (PublicApiThreading.executingSyncPublicApi()) {
                // It's a user thread, it executes a sync public API call, so it can do anything, no switch is needed.
                return false;
            }
            if (PublicApiThreading.executingAsyncPublicApi()) {
                // It's a user thread, it executes an async public API call, so it cannot do anything, a switch is needed.
                return true;
            }

            // It's something else: either a JRE thread or an Ignite thread not marked with ThreadAttributes. As we are not sure,
            // let's switch: false negative can produce assertion errors.
            return true;
        }
    }

    private void handleReplicaRequest(ReplicaRequest request, ClusterNode sender, @Nullable Long correlationId) {
        if (!busyLock.enterBusy()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Failed to process replica request (the node is stopping) [request={}].", request);
            }

            return;
        }

        String senderConsistentId = sender.name();

        try {
            // Notify the sender that the Replica is created and ready to process requests.
            if (request instanceof AwaitReplicaRequest) {
                replicas.compute(request.groupId(), (replicationGroupId, replicaFut) -> {
                    if (replicaFut == null) {
                        replicaFut = new CompletableFuture<>();
                    }

                    if (!replicaFut.isDone()) {
                        replicaFut.whenComplete((createdReplica, ex) -> {
                            if (ex != null) {
                                clusterNetSvc.messagingService().respond(
                                        senderConsistentId,
                                        REPLICA_MESSAGES_FACTORY
                                                .errorReplicaResponse()
                                                .throwable(ex)
                                                .build(),
                                        correlationId);
                            } else {
                                sendAwaitReplicaResponse(senderConsistentId, correlationId);
                            }
                        });
                    } else {
                        sendAwaitReplicaResponse(senderConsistentId, correlationId);
                    }
                    return replicaFut;
                });

                return;
            }

            CompletableFuture<Replica> replicaFut = replicas.get(request.groupId());

            HybridTimestamp requestTimestamp = extractTimestamp(request);

            if (replicaFut == null || !replicaFut.isDone()) {
                sendReplicaUnavailableErrorResponse(senderConsistentId, correlationId, request.groupId(), requestTimestamp);

                return;
            }

            if (requestTimestamp != null) {
                clockService.updateClock(requestTimestamp);
            }

            boolean sendTimestamp = request instanceof TimestampAware || request instanceof ReadOnlyDirectReplicaRequest;

            // replicaFut is always completed here.
            Replica replica = replicaFut.join();

            String senderId = sender.id();

            CompletableFuture<ReplicaResult> resFut = replica.processRequest(request, senderId);

            resFut.whenComplete((res, ex) -> {
                NetworkMessage msg;

                if (ex == null) {
                    msg = prepareReplicaResponse(sendTimestamp, res.result());
                } else {
                    if (indicatesUnexpectedProblem(ex)) {
                        LOG.warn("Failed to process replica request [request={}].", ex, request);
                    } else {
                        LOG.debug("Failed to process replica request [request={}].", ex, request);
                    }

                    msg = prepareReplicaErrorResponse(sendTimestamp, ex);
                }

                clusterNetSvc.messagingService().respond(senderConsistentId, msg, correlationId);

                if (request instanceof PrimaryReplicaRequest && isConnectivityRelatedException(ex)) {
                    stopLeaseProlongation(request.groupId(), null);
                }

                if (ex == null && res.replicationFuture() != null) {
                    res.replicationFuture().whenComplete((res0, ex0) -> {
                        NetworkMessage msg0;

                        LOG.debug("Sending delayed response for replica request [request={}]", request);

                        if (ex0 == null) {
                            msg0 = prepareReplicaResponse(sendTimestamp, res0);
                        } else {
                            LOG.warn("Failed to process delayed response [request={}]", ex0, request);

                            msg0 = prepareReplicaErrorResponse(sendTimestamp, ex0);
                        }

                        // Using strong send here is important to avoid a reordering with a normal response.
                        clusterNetSvc.messagingService().send(senderConsistentId, ChannelType.DEFAULT, msg0);
                    });
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static boolean indicatesUnexpectedProblem(Throwable ex) {
        return !(unwrapCause(ex) instanceof ExpectedReplicationException);
    }

    /**
     * Checks this exception is caused of timeout or connectivity issue.
     *
     * @param ex An exception
     * @return True if this exception has thrown due to timeout or connection problem, false otherwise.
     */
    private static boolean isConnectivityRelatedException(@Nullable Throwable ex) {
        if (ex instanceof ExecutionException || ex instanceof CompletionException) {
            ex = ex.getCause();
        }

        return ex instanceof TimeoutException || ex instanceof IOException;
    }

    private void onPlacementDriverMessageReceived(NetworkMessage msg0, ClusterNode sender, @Nullable Long correlationId) {
        if (!(msg0 instanceof PlacementDriverReplicaMessage)) {
            return;
        }

        String senderConsistentId = sender.name();

        assert correlationId != null;

        var msg = (LeaseGrantedMessage) msg0;

        if (!busyLock.enterBusy()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Failed to process placement driver message (the node is stopping) [msg={}].", msg);
            }

            return;
        }

        try {
            Set<ReplicationGroupId> replicationGroupIds = zonePartIdToTablePartId.get((ZonePartitionId) msg.groupId());

            CompletableFuture<LeaseGrantedMessageResponse>[] futures = new CompletableFuture[replicationGroupIds.size()];

            int i = 0;

            for (ReplicationGroupId grpId : replicationGroupIds) {
                CompletableFuture<Replica> replicaFut = replicas.computeIfAbsent(grpId, k -> new CompletableFuture<>());
                futures[i++] = replicaFut.thenCompose(replica -> replica.processPlacementDriverMessage(msg));
            }

            allOf(futures).whenComplete((responses, ex) -> {
                if (ex == null) {
                    boolean accepted = responses.stream().allMatch(LeaseGrantedMessageResponse::accepted);

                    assert !msg.force() || accepted : "We do not give a replica possibility to decline a forced request.";

                    String redirect = accepted ? null :
                            responses.stream().filter(leaseGranResp -> !leaseGranResp.accepted()).findAny().get().redirectProposal();

                    LeaseGrantedMessageResponse response = PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                            .appliedGroups(replicationGroupIds == null ? Collections.emptySet() : replicationGroupIds)
                            .redirectProposal(redirect)
                            .accepted(accepted)
                            .build();

                    clusterNetSvc.messagingService().respond(senderConsistentId, response, correlationId);
                } else if (!(unwrapCause(ex) instanceof NodeStoppingException)) {
                    LOG.error("Failed to process placement driver message [msg={}].", ex, msg);
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Sends stop lease prolongation message to all participants of placement driver group.
     *
     * @param groupId Replication group id.
     * @param redirectNodeId Node consistent id to redirect.
     */
    private void stopLeaseProlongation(ReplicationGroupId groupId, @Nullable String redirectNodeId) {
        LOG.info("The replica does not meet the requirements for the leaseholder [groupId={}, redirectNodeId={}]", groupId, redirectNodeId);

        msNodes.thenAccept(nodeIds -> {
            for (String nodeId : nodeIds) {
                ClusterNode node = clusterNetSvc.topologyService().getByConsistentId(nodeId);

                if (node != null) {
                    // TODO: IGNITE-19441 Stop lease prolongation message might be sent several
                    clusterNetSvc.messagingService().send(node, PLACEMENT_DRIVER_MESSAGES_FACTORY.stopLeaseProlongationMessage()
                            .groupId(groupId)
                            .redirectProposal(redirectNodeId)
                            .build());
                }
            }
        });
    }

    /**
     * Starts a replica. If a replica with the same partition id already exists, the method throws an exception.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @param raftClient Topology aware Raft client.
     * @param storageIndexTracker Storage index tracker.
     * @throws NodeStoppingException If node is stopping.
     * @throws ReplicaIsAlreadyStartedException Is thrown when a replica with the same replication group id has already been
     *         started.
     */
    public CompletableFuture<Replica> startReplica(
            ReplicationGroupId replicaGrpId,
            ZonePartitionId zonePartitionId,
            ReplicaListener listener,
            TopologyAwareRaftGroupService raftClient,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return startReplicaInternal(replicaGrpId, zonePartitionId, listener, raftClient, storageIndexTracker);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for starting a replica.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @param raftClient Topology aware Raft client.
     * @param storageIndexTracker Storage index tracker.
     */
    private CompletableFuture<Replica> startReplicaInternal(
            ReplicationGroupId replicaGrpId,
            ZonePartitionId zonePartitionId,
            ReplicaListener listener,
            TopologyAwareRaftGroupService raftClient,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker
    ) {
        LOG.info("Replica is about to start [replicationGroupId={}].", replicaGrpId);

        ClusterNode localNode = clusterNetSvc.topologyService().localMember();

        Replica newReplica = new Replica(
                replicaGrpId,
                zonePartitionId,
                listener,
                storageIndexTracker,
                raftClient,
                localNode,
                requestsExecutor,
                placementDriver,
                clockService
        );

        CompletableFuture<Replica> replicaFuture = replicas.compute(replicaGrpId, (k, existingReplicaFuture) -> {
            zonePartIdToTablePartId.compute(zonePartitionId, (key, tablePartIds) -> {
                if (tablePartIds == null) {
                    tablePartIds = new HashSet<>();
                }

                tablePartIds.add(replicaGrpId);

                return tablePartIds;
            });

            if (existingReplicaFuture == null || existingReplicaFuture.isDone()) {
                assert existingReplicaFuture == null || isCompletedSuccessfully(existingReplicaFuture);
                LOG.info("Replica is started [replicationGroupId={}].", replicaGrpId);

                return completedFuture(newReplica);
            } else {
                existingReplicaFuture.complete(newReplica);
                LOG.info("Replica is started, existing replica waiter was completed [replicationGroupId={}].", replicaGrpId);

                return existingReplicaFuture;
            }
        });

        var eventParams = new LocalReplicaEventParameters(replicaGrpId);

        return fireEvent(AFTER_REPLICA_STARTED, eventParams)
                .exceptionally(e -> {
                    LOG.error("Error when notifying about AFTER_REPLICA_STARTED event.", e);

                    return null;
                })
                .thenCompose(v -> replicaFuture);
    }

    /**
     * Stops a replica by the partition group id.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is found and closed, false otherwise.
     * @throws NodeStoppingException If the node is stopping.
     */
    public CompletableFuture<Boolean> stopReplica(ReplicationGroupId replicaGrpId) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return stopReplicaInternal(replicaGrpId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for stopping a replica.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is found and closed, false otherwise.
     */
    private CompletableFuture<Boolean> stopReplicaInternal(ReplicationGroupId replicaGrpId) {
        var isRemovedFuture = new CompletableFuture<Boolean>();

        var eventParams = new LocalReplicaEventParameters(replicaGrpId);

        fireEvent(BEFORE_REPLICA_STOPPED, eventParams).whenComplete((v, e) -> {
            if (e != null) {
                LOG.error("Error when notifying about BEFORE_REPLICA_STOPPED event.", e);
            }

            if (!busyLock.enterBusy()) {
                isRemovedFuture.completeExceptionally(new NodeStoppingException());

                return;
            }

            try {
                replicas.compute(replicaGrpId, (grpId, replicaFuture) -> {
                    if (replicaFuture == null) {
                        isRemovedFuture.complete(false);
                    } else if (!replicaFuture.isDone()) {
                        ClusterNode localMember = clusterNetSvc.topologyService().localMember();

                        replicaFuture.completeExceptionally(new ReplicaStoppingException(grpId, localMember));

                        isRemovedFuture.complete(true);
                    } else if (!isCompletedSuccessfully(replicaFuture)) {
                        isRemovedFuture.complete(true);
                    } else {
                        replicaFuture
                                .thenCompose(Replica::shutdown)
                                .whenComplete((notUsed, throwable) -> {
                                    if (throwable != null) {
                                        LOG.error("Failed to stop replica [replicaGrpId={}].", throwable, grpId);
                                    }

                                    isRemovedFuture.complete(throwable == null);
                                });
                    }

                    zonePartIdToTablePartId.forEach((zonePartId, tblPartIds) -> {
                        tblPartIds.remove(replicaGrpId);
                    });

                    return null;
                });
            } finally {
                busyLock.leaveBusy();
            }
        });

        return isRemovedFuture;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> start() {
        ExecutorChooser<NetworkMessage> replicaMessagesExecutorChooser = message -> requestsExecutor;

        clusterNetSvc.messagingService().addMessageHandler(ReplicaMessageGroup.class, replicaMessagesExecutorChooser, handler);
        clusterNetSvc.messagingService().addMessageHandler(PlacementDriverMessageGroup.class, placementDriverMessageHandler);
        messageGroupsToHandle.forEach(
                mg -> clusterNetSvc.messagingService().addMessageHandler(mg, replicaMessagesExecutorChooser, handler)
        );
        scheduledIdleSafeTimeSyncExecutor.scheduleAtFixedRate(
                this::idleSafeTimeSync,
                0,
                idleSafeTimePropagationPeriodMsSupplier.getAsLong(),
                TimeUnit.MILLISECONDS
        );

        scheduledTableLeaseUpdateExecutor.scheduleAtFixedRate(() -> {
            if (!busyLock.enterBusy()) {
                return;
            }

            try {
                updateTableGroupsInternal();
            } finally {
                busyLock.leaveBusy();
            }
        }, 0, 1, TimeUnit.SECONDS);

        cmgMgr.metaStorageNodes().whenComplete((nodes, e) -> {
            if (e != null) {
                msNodes.completeExceptionally(e);
            } else {
                msNodes.complete(nodes);
            }
        });

        localNodeId = clusterNetSvc.topologyService().localMember().id();

        return nullCompletedFuture();
    }

    /**
     * Updates list of replication groups for each distributed zone.
     */
    private void updateTableGroupsInternal() {
        for (Entry<ZonePartitionId, Set<ReplicationGroupId>> entry : zonePartIdToTablePartId.entrySet()) {
            ZonePartitionId repGrp = entry.getKey();

            ReplicaMeta meta = placementDriver.getLeaseMeta(repGrp);

            if (meta != null) {
                HashSet<ReplicationGroupId> diff = new HashSet<>(entry.getValue());
                diff.removeAll(meta.subgroups());

                if (meta.getLeaseholderId().equals(localNodeId) && !diff.isEmpty()) {
                    LOG.info("New subgroups are found for existing lease [repGrp={}, subGroups={}]", repGrp, diff);

                    try {
                        placementDriver.addSubgroups(repGrp, meta.getStartTime().longValue(), diff)
                                .thenCompose(unused -> {
                                    ArrayList<CompletableFuture<?>> requestToReplicas = new ArrayList<>();

                                                for (ReplicationGroupId partId : diff) {
                                                    WaitReplicaStateMessage req = REPLICA_MESSAGES_FACTORY.waitReplicaStateMessage()
                                                            .enlistmentConsistencyToken(meta.getStartTime().longValue())
                                                            .groupId(partId)
                                                            // TODO: discuss this timeout
                                                            .timeout(10)
                                                            .build();

                                        CompletableFuture<Replica> replicaFut = replicas.get(repGrp);

                                        if (replicaFut != null) {
                                            requestToReplicas.add(replicaFut.thenCompose(
                                                    replica -> replica.processRequest(req, localNodeId)));
                                        }
                                    }

                                    return allOf(requestToReplicas.toArray(CompletableFuture[]::new));
                                }).get(10, TimeUnit.SECONDS);
                    } catch (Exception ex) {
                        LOG.error(
                                "Failed to add new subgroups to the replication group [repGrp={}, subGroups={}]",
                                ex,
                                repGrp,
                                diff
                        );
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        shutdownAndAwaitTermination(scheduledIdleSafeTimeSyncExecutor, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(scheduledTableLeaseUpdateExecutor, 10, TimeUnit.SECONDS);

        assert replicas.values().stream().noneMatch(CompletableFuture::isDone)
                : "There are replicas alive [replicas="
                + replicas.entrySet().stream().filter(e -> e.getValue().isDone()).map(Entry::getKey).collect(toSet()) + ']';

        for (CompletableFuture<Replica> replicaFuture : replicas.values()) {
            replicaFuture.completeExceptionally(new NodeStoppingException());
        }
    }

    /**
     * Extract a hybrid timestamp from timestamp aware request or return null.
     */
    private static @Nullable HybridTimestamp extractTimestamp(ReplicaRequest request) {
        if (request instanceof TimestampAware) {
            return ((TimestampAware) request).timestamp();
        } else {
            return null;
        }
    }

    /**
     * Sends replica unavailable error response.
     */
    private void sendReplicaUnavailableErrorResponse(
            String senderConsistentId,
            long correlationId,
            ReplicationGroupId groupId,
            @Nullable HybridTimestamp requestTimestamp
    ) {
        if (requestTimestamp != null) {
            clusterNetSvc.messagingService().respond(
                    senderConsistentId,
                    REPLICA_MESSAGES_FACTORY
                            .errorTimestampAwareReplicaResponse()
                            .throwable(
                                    new ReplicaUnavailableException(
                                            groupId,
                                            clusterNetSvc.topologyService().localMember())
                            )
                            .timestampLong(clockService.updateClock(requestTimestamp).longValue())
                            .build(),
                    correlationId);
        } else {
            clusterNetSvc.messagingService().respond(
                    senderConsistentId,
                    REPLICA_MESSAGES_FACTORY
                            .errorReplicaResponse()
                            .throwable(
                                    new ReplicaUnavailableException(
                                            groupId,
                                            clusterNetSvc.topologyService().localMember())
                            )
                            .build(),
                    correlationId);
        }
    }

    /**
     * Sends await replica response.
     */
    private void sendAwaitReplicaResponse(String senderConsistentId, long correlationId) {
        clusterNetSvc.messagingService().respond(
                senderConsistentId,
                REPLICA_MESSAGES_FACTORY
                        .awaitReplicaResponse()
                        .build(),
                correlationId);
    }

    /**
     * Prepares replica response.
     */
    private NetworkMessage prepareReplicaResponse(boolean sendTimestamp, Object result) {
        if (sendTimestamp) {
            return REPLICA_MESSAGES_FACTORY
                    .timestampAwareReplicaResponse()
                    .result(result)
                    .timestampLong(clockService.nowLong())
                    .build();
        } else {
            return REPLICA_MESSAGES_FACTORY
                    .replicaResponse()
                    .result(result)
                    .build();
        }
    }

    /**
     * Prepares replica error response.
     */
    private NetworkMessage prepareReplicaErrorResponse(boolean sendTimestamp, Throwable ex) {
        if (sendTimestamp) {
            return REPLICA_MESSAGES_FACTORY
                    .errorTimestampAwareReplicaResponse()
                    .throwable(ex)
                    .timestampLong(clockService.nowLong())
                    .build();
        } else {
            return REPLICA_MESSAGES_FACTORY
                    .errorReplicaResponse()
                    .throwable(ex)
                    .build();
        }
    }

    /**
     * Idle safe time sync for replicas.
     */
    private void idleSafeTimeSync() {
        for (Entry<ReplicationGroupId, CompletableFuture<Replica>> entry : replicas.entrySet()) {
            try {
                sendSafeTimeSyncIfReplicaReady(entry.getValue());
            } catch (Exception | AssertionError e) {
                LOG.warn("Error while trying to send a safe time sync request [groupId={}]", e, entry.getKey());
            } catch (Error e) {
                LOG.error("Error while trying to send a safe time sync request [groupId={}]", e, entry.getKey());

                failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
        }
    }

    private void sendSafeTimeSyncIfReplicaReady(CompletableFuture<Replica> replicaFuture) {
        if (isCompletedSuccessfully(replicaFuture)) {
            Replica replica = replicaFuture.join();

            ReplicaSafeTimeSyncRequest req = REPLICA_MESSAGES_FACTORY.replicaSafeTimeSyncRequest()
                    .groupId(replica.groupId())
                    .build();

            replica.processRequest(req, localNodeId);
        }
    }

    /**
     * Check if replica is started.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is started.
     */
    public boolean isReplicaStarted(ReplicationGroupId replicaGrpId) {
        CompletableFuture<Replica> replicaFuture = replicas.get(replicaGrpId);
        return replicaFuture != null && isCompletedSuccessfully(replicaFuture);
    }

    /**
     * Check if replica was touched by an any actor. Touched here means either replica creation or replica waiter registration.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica was touched.
     */
    @TestOnly
    public boolean isReplicaTouched(ReplicationGroupId replicaGrpId) {
        return replicas.containsKey(replicaGrpId);
    }

    /**
     * Returns started replication groups.
     *
     * @return Set of started replication groups.
     */
    @TestOnly
    public Set<ReplicationGroupId> startedGroups() {
        return replicas.entrySet().stream()
                .filter(entry -> isCompletedSuccessfully(entry.getValue()))
                .map(Entry::getKey)
                .collect(toSet());
    }
}
