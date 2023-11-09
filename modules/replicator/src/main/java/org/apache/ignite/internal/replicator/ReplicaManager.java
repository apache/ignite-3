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
import static org.apache.ignite.internal.Kludges.IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS_PROPERTY;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
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
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ChannelType;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Replica manager maintains {@link Replica} instances on an Ignite node.
 * Manager allows starting, stopping, getting a {@link Replica} by its unique id.
 * Only a single instance of the class exists in Ignite node.
 * This class allow to start/stop/get a replica.
 */
public class ReplicaManager implements IgniteComponent {
    /** Default Idle safe time propagation period for tests. */
    public static final int DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS = 1000;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaManager.class);

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean metaStorageNodes = new AtomicBoolean();

    /** Meta storage service. */
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

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /** Scheduled executor for idle safe time sync. */
    private final ScheduledExecutorService scheduledIdleSafeTimeSyncExecutor;

    /** Set of message groups to handler as replica requests. */
    private final Set<Class<?>> messageGroupsToHandle;

    /** Executor. */
    // TODO: IGNITE-20063 Maybe get rid of it
    private final ExecutorService executor;

    private String localNodeId;

    /**
     * Constructor for a replica service.
     *
     * @param nodeName Node name.
     * @param clusterNetSvc Cluster network service.
     * @param cmgMgr Cluster group manager.
     * @param clock A hybrid logical clock.
     * @param messageGroupsToHandle Message handlers.
     * @param placementDriver A placement driver.
     */
    @TestOnly
    public ReplicaManager(
            String nodeName,
            ClusterService clusterNetSvc,
            ClusterManagementGroupManager cmgMgr,
            HybridClock clock,
            Set<Class<?>> messageGroupsToHandle,
            PlacementDriver placementDriver
    ) {
        this(
                nodeName,
                clusterNetSvc,
                cmgMgr,
                clock,
                messageGroupsToHandle,
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
        );
    }

    /**
     * Constructor for a replica service.
     *
     * @param nodeName Node name.
     * @param clusterNetSvc Cluster network service.
     * @param cmgMgr Cluster group manager.
     * @param clock A hybrid logical clock.
     * @param messageGroupsToHandle Message handlers.
     * @param placementDriver A placement driver.
     * @param idleSafeTimePropagationPeriodMsSupplier Used to get idle safe time propagation period in ms.
     */
    public ReplicaManager(
            String nodeName,
            ClusterService clusterNetSvc,
            ClusterManagementGroupManager cmgMgr,
            HybridClock clock,
            Set<Class<?>> messageGroupsToHandle,
            PlacementDriver placementDriver,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier
    ) {
        this.clusterNetSvc = clusterNetSvc;
        this.cmgMgr = cmgMgr;
        this.clock = clock;
        this.messageGroupsToHandle = messageGroupsToHandle;
        this.handler = this::onReplicaMessageReceived;
        this.placementDriverMessageHandler = this::onPlacementDriverMessageReceived;
        this.placementDriver = placementDriver;
        this.idleSafeTimePropagationPeriodMsSupplier = idleSafeTimePropagationPeriodMsSupplier;

        scheduledIdleSafeTimeSyncExecutor = Executors.newScheduledThreadPool(
                1,
                NamedThreadFactory.create(nodeName, "scheduled-idle-safe-time-sync-thread", LOG)
        );

        int threadCount = Runtime.getRuntime().availableProcessors();

        executor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(nodeName, "replica", LOG)
        );
    }

    private void onReplicaMessageReceived(NetworkMessage message, String senderConsistentId, @Nullable Long correlationId) {
        if (!(message instanceof ReplicaRequest)) {
            return;
        }

        ReplicaRequest request = (ReplicaRequest) message;

        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

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
                                        createdReplica.ready().thenAccept(unused ->
                                                IgniteUtils.inBusyLock(
                                                        busyLock,
                                                        () -> sendAwaitReplicaResponse(senderConsistentId, correlationId)
                                                )
                                        );
                                    }
                                }
                        );
                    } else {
                        sendAwaitReplicaResponse(senderConsistentId, correlationId);
                    }
                    return replicaFut;
                });

                return;
            }

            CompletableFuture<Replica> replicaFut = replicas.get(request.groupId());

            HybridTimestamp requestTimestamp = extractTimestamp(request);

            if (replicaFut == null || !replicaFut.isDone() || !replicaFut.join().ready().isDone()) {
                sendReplicaUnavailableErrorResponse(senderConsistentId, correlationId, request.groupId(), requestTimestamp);

                return;
            }

            if (requestTimestamp != null) {
                clock.update(requestTimestamp);
            }

            boolean sendTimestamp = request instanceof TimestampAware || request instanceof ReadOnlyDirectReplicaRequest;

            // replicaFut is always completed here.
            Replica replica = replicaFut.join();

            // TODO IGNITE-20296 Id of the node should come along with the message itself.
            String senderId = clusterNetSvc.topologyService().getByConsistentId(senderConsistentId).id();

            CompletableFuture<ReplicaResult> resFut = replica.processRequest(request, senderId);

            resFut.handle((res, ex) -> {
                NetworkMessage msg;

                if (ex == null) {
                    msg = prepareReplicaResponse(sendTimestamp, res.result());
                } else {
                    if (indicatesUnexpectedProblem(ex)) {
                        LOG.warn("Failed to process replica request [request={}]", ex, request);
                    } else {
                        LOG.debug("Failed to process replica request [request={}]", ex, request);
                    }

                    msg = prepareReplicaErrorResponse(sendTimestamp, ex);
                }

                clusterNetSvc.messagingService().respond(senderConsistentId, msg, correlationId);

                if (request instanceof PrimaryReplicaRequest) {
                    ClusterNode localNode = clusterNetSvc.topologyService().localMember();

                    if (!localNode.name().equals(replica.proposedPrimary())) {
                        stopLeaseProlongation(request.groupId(), replica.proposedPrimary());
                    } else if (isConnectivityRelatedException(ex)) {
                        stopLeaseProlongation(request.groupId(), null);
                    }
                }

                if (ex == null && res.replicationFuture() != null) {
                    assert request instanceof PrimaryReplicaRequest;

                    res.replicationFuture().handle((res0, ex0) -> {
                        NetworkMessage msg0;

                        LOG.debug("Sending delayed response for replica request [request={}]", request);

                        if (ex == null) {
                            msg0 = prepareReplicaResponse(sendTimestamp, res0);
                        } else {
                            LOG.warn("Failed to process delayed response [request={}]", ex, request);

                            msg0 = prepareReplicaErrorResponse(sendTimestamp, ex);
                        }

                        // Using strong send here is important to avoid a reordering with a normal response.
                        clusterNetSvc.messagingService().send(senderConsistentId, ChannelType.DEFAULT, msg0);

                        return null;
                    });
                }

                return null;
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static boolean indicatesUnexpectedProblem(Throwable ex) {
        return !(ex instanceof ExpectedReplicationException);
    }

    /**
     * Checks this exception is caused of timeout or connectivity issue.
     *
     * @param ex An exception
     * @return True if this exception has thrown due to timeout or connection problem, false otherwise.
     */
    private static boolean isConnectivityRelatedException(Throwable ex) {
        if (ex instanceof ExecutionException || ex instanceof CompletionException) {
            ex = ex.getCause();
        }

        return ex instanceof TimeoutException || ex instanceof IOException;
    }

    private void onPlacementDriverMessageReceived(NetworkMessage msg0, String senderConsistentId, @Nullable Long correlationId) {
        if (!(msg0 instanceof PlacementDriverReplicaMessage)) {
            return;
        }

        var msg = (PlacementDriverReplicaMessage) msg0;

        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            CompletableFuture<Replica> replicaFut = replicas.computeIfAbsent(msg.groupId(), k -> new CompletableFuture<>());

            replicaFut
                    .thenCompose(replica -> replica.processPlacementDriverMessage(msg))
                    .whenComplete((response, ex) -> {
                        if (ex == null) {
                            clusterNetSvc.messagingService().respond(senderConsistentId, response, correlationId);
                        } else if (!(unwrapCause(ex) instanceof NodeStoppingException)) {
                            LOG.error("Failed to process placement driver message [msg={}]", ex, msg);
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
    private void stopLeaseProlongation(ReplicationGroupId groupId, String redirectNodeId) {
        LOG.info("The replica does not meet the requirements for the leaseholder [groupId={}, redirectNodeId={}]", groupId, redirectNodeId);

        msNodes.thenAccept(nodeIds -> {
            for (String nodeId : nodeIds) {
                ClusterNode node = clusterNetSvc.topologyService().getByConsistentId(nodeId);

                if (node != null) {
                    //TODO: IGNITE-19441 Stop lease prolongation message might be sent several
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
     * @param whenReplicaReady Future that completes when the replica become ready.
     * @param listener Replica listener.
     * @param raftClient Topology aware Raft client.
     * @param storageIndexTracker Storage index tracker.
     * @throws NodeStoppingException If node is stopping.
     * @throws ReplicaIsAlreadyStartedException Is thrown when a replica with the same replication group id has already been started.
     */
    public void startReplica(
            ReplicationGroupId replicaGrpId,
            CompletableFuture<Void> whenReplicaReady,
            ReplicaListener listener,
            TopologyAwareRaftGroupService raftClient,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            startReplicaInternal(replicaGrpId, whenReplicaReady, listener, raftClient, storageIndexTracker);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for starting a replica.
     *
     * @param replicaGrpId Replication group id.
     * @param whenReplicaReady Future that completes when the replica become ready.
     * @param listener Replica listener.
     * @param raftClient Topology aware Raft client.
     * @param storageIndexTracker Storage index tracker.
     */
    private void startReplicaInternal(
            ReplicationGroupId replicaGrpId,
            CompletableFuture<Void> whenReplicaReady,
            ReplicaListener listener,
            TopologyAwareRaftGroupService raftClient,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker
    ) {
        ClusterNode localNode = clusterNetSvc.topologyService().localMember();

        Replica newReplica = new Replica(
                replicaGrpId,
                whenReplicaReady,
                listener,
                storageIndexTracker,
                raftClient,
                localNode,
                executor,
                placementDriver
        );

        replicas.compute(replicaGrpId, (replicationGroupId, replicaFut) -> {
            if (replicaFut == null) {
                return completedFuture(newReplica);
            } else {
                if (replicaFut.isDone() && !replicaFut.isCancelled() && !replicaFut.isCompletedExceptionally()) {
                    return completedFuture(newReplica);
                }

                replicaFut.complete(newReplica);

                return replicaFut;
            }
        });
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
        CompletableFuture<Replica> removed = replicas.remove(replicaGrpId);

        if (removed != null) {
            if (!removed.isDone()) {
                removed.completeExceptionally(new ReplicaStoppingException(
                        replicaGrpId,
                        clusterNetSvc.topologyService().localMember()
                ));
            }

            if (!removed.isCompletedExceptionally()) {
                return removed
                        .thenCompose(Replica::shutdown)
                        .handle((notUsed, throwable) -> {
                            if (throwable == null) {
                                return true;
                            } else {
                                LOG.error("Failed to stop replica [replicaGrpId={}]", throwable, replicaGrpId);

                                return false;
                            }
                        });
            }

            return completedFuture(true);
        }

        return completedFuture(false);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        clusterNetSvc.messagingService().addMessageHandler(ReplicaMessageGroup.class, handler);
        clusterNetSvc.messagingService().addMessageHandler(PlacementDriverMessageGroup.class, placementDriverMessageHandler);
        messageGroupsToHandle.forEach(mg -> clusterNetSvc.messagingService().addMessageHandler(mg, handler));
        scheduledIdleSafeTimeSyncExecutor.scheduleAtFixedRate(
                this::idleSafeTimeSync,
                0,
                idleSafeTimePropagationPeriodMsSupplier.getAsLong(),
                TimeUnit.MILLISECONDS
        );

        cmgMgr.metaStorageNodes().whenComplete((nodes, e) -> {
                    if (e != null) {
                        msNodes.completeExceptionally(e);
                    } else {
                        msNodes.complete(nodes);
                    }
                }
        );

        localNodeId = clusterNetSvc.topologyService().localMember().id();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!metaStorageNodes.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        shutdownAndAwaitTermination(scheduledIdleSafeTimeSyncExecutor, 10, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        assert replicas.values().stream().noneMatch(CompletableFuture::isDone)
                : "There are replicas alive [replicas="
                    + replicas.entrySet().stream().filter(e -> e.getValue().isDone()).map(Entry::getKey).collect(toSet()) + ']';

        for (CompletableFuture<Replica> replicaFuture : replicas.values()) {
            if (!replicaFuture.isDone()) {
                replicaFuture.completeExceptionally(new NodeStoppingException());
            }
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
            @Nullable Long correlationId,
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
                            .timestampLong(clock.update(requestTimestamp).longValue())
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
    private void sendAwaitReplicaResponse(String senderConsistentId, @Nullable Long correlationId) {
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
                    .timestampLong(clock.nowLong())
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
                    .timestampLong(clock.nowLong())
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
        replicas.values().forEach(r -> {
            if (r.isDone()) {
                ReplicaSafeTimeSyncRequest req = REPLICA_MESSAGES_FACTORY.replicaSafeTimeSyncRequest()
                        .groupId(r.join().groupId())
                        .build();

                r.join().processRequest(req, localNodeId);
            }
        });
    }

    /**
     * Check if replica is started.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is started.
     */
    public boolean isReplicaStarted(ReplicationGroupId replicaGrpId) {
        return replicas.containsKey(replicaGrpId);
    }

    /**
     * Returns started replication groups.
     *
     * @return Set of started replication groups.
     */
    @TestOnly
    public Set<ReplicationGroupId> startedGroups() {
        return replicas.keySet();
    }

    /**
     * TODO: to be removed after IGNITE-20499 is fixed. This was introduced in a rush because of a burning release, should be fixe asap.
     */
    public static long idleSafeTimePropagationPeriodMs() {
        return Long.parseLong(
                System.getProperty(
                        IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS_PROPERTY,
                        Integer.toString(DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS)
                )
        );
    }
}
