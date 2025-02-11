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
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.AFTER_REPLICA_STARTED;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.BEFORE_REPLICA_STOPPED;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.thread.ThreadOperation.TX_STATE_STORAGE_ACCESS;
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCauseOrSuppressed;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.shouldSwitchToRequestsExecutor;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.IgniteThrottledLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.placementdriver.message.StopLeaseProlongationMessageResponse;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.LogStorageBudgetView;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LogStorageFactoryCreator;
import org.apache.ignite.internal.raft.storage.impl.VolatileRaftMetaStorage;
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
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteStripedReadWriteLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Replica manager maintains {@link Replica} instances on an Ignite node.
 *
 * <p>Manager allows starting, stopping, getting a {@link Replica} by its unique id.
 *
 * <p>Only a single instance of the class exists in Ignite node.
 */
public class ReplicaManager extends AbstractEventProducer<LocalReplicaEvent, LocalReplicaEventParameters> implements IgniteComponent {
    private static final long STOP_LEASE_PROLONGATION_RETRIES_TIMEOUT_MS = 60_000;

    private static final IgniteLogger LOG = Loggers.forClass(ReplicaManager.class);

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    private final IgniteThrottledLogger throttledLog;

    /** Busy lock to stop synchronously. */
    private final IgniteStripedReadWriteLock busyLock = new IgniteStripedReadWriteLock();

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

    /** Raft manager for RAFT-clients creation. */
    // TODO: move into {@method Replica#shutdown} https://issues.apache.org/jira/browse/IGNITE-22372
    private final RaftManager raftManager;

    /** Raft clients factory for raft server endpoints starting. */
    private final TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory;

    /** Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for volatile tables. */
    private final LogStorageFactoryCreator volatileLogStorageFactoryCreator;

    /** Raft command marshaller for raft server endpoints starting. */
    private final Marshaller raftCommandsMarshaller;

    /** Message handler for placement driver messages. */
    private final NetworkMessageHandler placementDriverMessageHandler;

    /** Placement driver. */
    private final PlacementDriver placementDriver;

    private final LongSupplier idleSafeTimePropagationPeriodMsSupplier;

    /** Replicas. */
    private final ConcurrentHashMap<ReplicationGroupId, CompletableFuture<Replica>> replicas = new ConcurrentHashMap<>();

    private final ClockService clockService;

    /** Scheduled executor for idle safe time sync. */
    private final ScheduledExecutorService scheduledIdleSafeTimeSyncExecutor;

    /** Executor that will be used to execute requests by replicas. */
    private final Executor requestsExecutor;

    /** Failure processor. */
    private final FailureManager failureManager;

    /** Set of message groups to handler as replica requests. */
    private final Set<Class<?>> messageGroupsToHandle;

    private final RaftGroupOptionsConfigurer partitionRaftConfigurer;

    /** Executor. */
    // TODO: IGNITE-20063 Maybe get rid of it
    private final ExecutorService executor;

    private final ReplicaStateManager replicaStateManager;

    private final ExecutorService replicasCreationExecutor;

    private volatile UUID localNodeId;

    private volatile String localNodeConsistentId;

    /* Temporary converter to support the zone based partitions in tests. **/
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this code
    private volatile Function<ReplicaRequest, ReplicationGroupId> groupIdConverter = r -> r.groupId().asReplicationGroupId();

    private volatile @Nullable HybridTimestamp lastIdleSafeTimeProposal;

    private final Function<ReplicationGroupId, CompletableFuture<byte[]>> getPendingAssignmentsSupplier;

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
     * @param failureManager Failure processor.
     * @param raftCommandsMarshaller Command marshaller for raft groups creation.
     * @param raftGroupServiceFactory A factory for raft-clients creation.
     * @param raftManager The manager made up of songs and words to spite all my troubles is not so bad at all.
     * @param volatileLogStorageFactoryCreator Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for
     *      volatile tables.
     * @param groupIdConverter Temporary converter to support the zone based partitions in tests.
     * @param getPendingAssignmentsSupplier The supplier of pending assignments for rebalance failover purposes.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this method
    @TestOnly
    public ReplicaManager(
            String nodeName,
            ClusterService clusterNetSvc,
            ClusterManagementGroupManager cmgMgr,
            ClockService clockService,
            Set<Class<?>> messageGroupsToHandle,
            PlacementDriver placementDriver,
            Executor requestsExecutor,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier,
            FailureManager failureManager,
            Marshaller raftCommandsMarshaller,
            TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            RaftManager raftManager,
            RaftGroupOptionsConfigurer partitionRaftConfigurer,
            LogStorageFactoryCreator volatileLogStorageFactoryCreator,
            Executor replicaStartStopExecutor,
            Function<ReplicaRequest, ReplicationGroupId> groupIdConverter,
            Function<ReplicationGroupId, CompletableFuture<byte[]>> getPendingAssignmentsSupplier
    ) {
        this(
                nodeName,
                clusterNetSvc,
                cmgMgr,
                clockService,
                messageGroupsToHandle,
                placementDriver,
                requestsExecutor,
                idleSafeTimePropagationPeriodMsSupplier,
                failureManager,
                raftCommandsMarshaller,
                raftGroupServiceFactory,
                raftManager,
                partitionRaftConfigurer,
                volatileLogStorageFactoryCreator,
                replicaStartStopExecutor,
                getPendingAssignmentsSupplier
        );

        this.groupIdConverter = groupIdConverter;
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this method.
    public void groupIdConverter(Function<ReplicaRequest, ReplicationGroupId> converter) {
        groupIdConverter = converter;
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
     * @param failureManager Failure processor.
     * @param raftCommandsMarshaller Command marshaller for raft groups creation.
     * @param raftGroupServiceFactory A factory for raft-clients creation.
     * @param raftManager The manager made up of songs and words to spite all my troubles is not so bad at all.
     * @param partitionRaftConfigurer Configurer of raft options on raft group creation.
     * @param volatileLogStorageFactoryCreator Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for
     *      volatile tables.
     * @param replicaStartStopExecutor Executor for asynchronous replicas lifecycle management.
     * @param getPendingAssignmentsSupplier The supplier of pending assignments for rebalance failover purposes.
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
            FailureManager failureManager,
            @Nullable Marshaller raftCommandsMarshaller,
            TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            RaftManager raftManager,
            RaftGroupOptionsConfigurer partitionRaftConfigurer,
            LogStorageFactoryCreator volatileLogStorageFactoryCreator,
            Executor replicaStartStopExecutor,
            Function<ReplicationGroupId, CompletableFuture<byte[]>> getPendingAssignmentsSupplier
    ) {
        this.clusterNetSvc = clusterNetSvc;
        this.cmgMgr = cmgMgr;
        this.clockService = clockService;
        this.messageGroupsToHandle = messageGroupsToHandle;
        this.volatileLogStorageFactoryCreator = volatileLogStorageFactoryCreator;
        this.handler = this::onReplicaMessageReceived;
        this.placementDriverMessageHandler = this::onPlacementDriverMessageReceived;
        this.placementDriver = placementDriver;
        this.requestsExecutor = requestsExecutor;
        this.idleSafeTimePropagationPeriodMsSupplier = idleSafeTimePropagationPeriodMsSupplier;
        this.failureManager = failureManager;
        this.raftCommandsMarshaller = raftCommandsMarshaller;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.raftManager = raftManager;
        this.partitionRaftConfigurer = partitionRaftConfigurer;
        this.getPendingAssignmentsSupplier = getPendingAssignmentsSupplier;
        this.replicaStateManager = new ReplicaStateManager(
                replicaStartStopExecutor,
                clockService,
                placementDriver,
                this
        );

        // This pool MUST be single-threaded to make sure idle safe time propagation attempts are not reordered on it.
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

        replicasCreationExecutor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "replica-manager", LOG, STORAGE_READ, STORAGE_WRITE)
        );

        throttledLog = Loggers.toThrottledLogger(LOG, executor);
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
        if (shouldSwitchToRequestsExecutor(STORAGE_READ, STORAGE_WRITE, TX_STATE_STORAGE_ACCESS)) {
            requestsExecutor.execute(() -> handleReplicaRequest(request, sender, correlationId));
        } else {
            handleReplicaRequest(request, sender, correlationId);
        }
    }

    private void handleReplicaRequest(ReplicaRequest request, ClusterNode sender, @Nullable Long correlationId) {
        if (!enterBusy()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Failed to process replica request (the node is stopping) [request={}].", request);
            }

            return;
        }

        ReplicationGroupId groupId = groupIdConverter.apply(request);

        String senderConsistentId = sender.name();

        try {
            // Notify the sender that the Replica is created and ready to process requests.
            if (request instanceof AwaitReplicaRequest) {
                replicas.compute(groupId, (replicationGroupId, replicaFut) -> {
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

            CompletableFuture<Replica> replicaFut = replicas.get(groupId);

            HybridTimestamp requestTimestamp = extractTimestamp(request);

            if (replicaFut == null || !replicaFut.isDone()) {
                sendReplicaUnavailableErrorResponse(senderConsistentId, correlationId, groupId, requestTimestamp);

                return;
            }

            if (requestTimestamp != null) {
                clockService.updateClock(requestTimestamp);
            }

            boolean sendTimestamp = request instanceof TimestampAware || request instanceof ReadOnlyDirectReplicaRequest;

            // replicaFut is always completed here.
            Replica replica = replicaFut.join();

            CompletableFuture<ReplicaResult> resFut = replica.processRequest(request, sender.id());

            resFut.whenComplete((res, ex) -> {
                try {
                    respond(request, correlationId, res, ex, sendTimestamp, senderConsistentId, groupId);
                } catch (RuntimeException e1) {
                    LOG.error("Error while trying to respond", e1);
                }
            });
        } finally {
            leaveBusy();
        }
    }

    private void respond(
            ReplicaRequest request,
            @Nullable Long correlationId,
            ReplicaResult res,
            Throwable ex,
            boolean sendTimestamp,
            String senderConsistentId,
            ReplicationGroupId groupId
    ) {
        NetworkMessage msg;

        if (ex == null) {
            msg = prepareReplicaResponse(sendTimestamp, res);
        } else {
            if (indicatesUnexpectedProblem(ex)) {
                throttledLog.warn("Failed to process replica request [request={}].", ex, request);
            } else {
                throttledLog.debug("Failed to process replica request [request={}].", ex, request);
            }

            msg = prepareReplicaErrorResponse(sendTimestamp, ex);
        }

        clusterNetSvc.messagingService().respond(senderConsistentId, msg, correlationId);

        if (request instanceof PrimaryReplicaRequest && isConnectivityRelatedException(ex)) {
            LOG.info("The replica does not meet the requirements for the leaseholder [groupId={}].", groupId);

            stopLeaseProlongation(groupId, null);
        }

        if (ex == null && res.applyResult().replicationFuture() != null) {
            res.applyResult().replicationFuture().whenComplete((res0, ex0) -> {
                NetworkMessage msg0;

                LOG.debug("Sending delayed response for replica request [request={}]", request);

                if (ex0 == null) {
                    msg0 = prepareReplicaResponse(sendTimestamp, new ReplicaResult(res0, null));
                } else {
                    LOG.warn("Failed to process delayed response [request={}]", ex0, request);

                    msg0 = prepareReplicaErrorResponse(sendTimestamp, ex0);
                }

                // Using strong send here is important to avoid a reordering with a normal response.
                clusterNetSvc.messagingService().send(senderConsistentId, ChannelType.DEFAULT, msg0);
            });
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

        var msg = (PlacementDriverReplicaMessage) msg0;

        if (!enterBusy()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Failed to process placement driver message (the node is stopping) [msg={}].", msg);
            }

            return;
        }

        try {
            CompletableFuture<Replica> replicaFut = replicas.computeIfAbsent(msg.groupId(), k -> new CompletableFuture<>());

            replicaFut
                    .thenCompose(replica -> replica.processPlacementDriverMessage(msg))
                    .whenComplete((response, ex) -> {
                        if (ex == null) {
                            clusterNetSvc.messagingService().respond(senderConsistentId, response, correlationId);
                        } else if (!(unwrapCause(ex) instanceof NodeStoppingException)) {
                            LOG.error("Failed to process placement driver message [msg={}].", ex, msg);
                        }
                    });
        } finally {
            leaveBusy();
        }
    }

    /**
     * Sends stop lease prolongation message to all participants of placement driver group.
     *
     * @param groupId Replication group id.
     * @param redirectNodeId Node consistent id to redirect.
     * @return Future that is completed when the lease is denied to prolong, containing the expiration time of this lease.
     */
    private CompletableFuture<HybridTimestamp> stopLeaseProlongation(
            ReplicationGroupId groupId,
            @Nullable String redirectNodeId
    ) {
        long startTime = System.currentTimeMillis();

        return stopLeaseProlongation(groupId, redirectNodeId, startTime + STOP_LEASE_PROLONGATION_RETRIES_TIMEOUT_MS);
    }

    /**
     * Sends stop lease prolongation message to all participants of placement driver group.
     *
     * @param groupId Replication group id.
     * @param redirectNodeId Node consistent id to redirect.
     * @param endTime Time to end the retries.
     * @return Future that is completed when the lease is denied to prolong, containing the expiration time of this lease.
     */
    private CompletableFuture<HybridTimestamp> stopLeaseProlongation(
            ReplicationGroupId groupId,
            @Nullable String redirectNodeId,
            long endTime
    ) {
        long timeout = endTime - System.currentTimeMillis();

        if (timeout <= 0) {
            return failedFuture(new IgniteException(INTERNAL_ERR, format("Failed to stop lease prolongation within timeout [groupId={}]",
                    groupId)));
        } else {
            return msNodes.thenCompose(nodeIds -> {
                List<CompletableFuture<NetworkMessage>> futs = new ArrayList<>();

                for (String nodeId : nodeIds) {
                    ClusterNode node = clusterNetSvc.topologyService().getByConsistentId(nodeId);

                    if (node != null) {
                        // TODO: IGNITE-19441 Stop lease prolongation message might be sent several times.
                        futs.add(
                                clusterNetSvc.messagingService()
                                        .invoke(node, PLACEMENT_DRIVER_MESSAGES_FACTORY.stopLeaseProlongationMessage()
                                                .groupId(groupId)
                                                .redirectProposal(redirectNodeId)
                                                .build(), timeout)
                                        .exceptionally(th -> null)
                        );
                    }
                }

                // We send StopLeaseProlongationMessage on every node of placement driver group, so there should be all nulls or
                // just one non-null, possible outcomes:
                // - it wasn't successfully handled anywhere (lease updater thread made successful ms.invoke, and SLPM handlers failed
                //   to do ms.invoke)
                // - it was successfully handled on one node of PD group, in this case we get one non-null
                // - it was successfully handled on some node, but message handling was delayed on some other node and it already got lease
                //   update from MS where this lease was denied, in this case it also returns null (slightly other case than
                //   failed ms.invoke but same outcome)
                return allOf(futs)
                        .thenCompose(unused -> {
                            NetworkMessage response = futs.stream()
                                    .map(CompletableFuture::join)
                                    .filter(resp -> resp instanceof StopLeaseProlongationMessageResponse
                                            && ((StopLeaseProlongationMessageResponse) resp).deniedLeaseExpirationTime() != null)
                                    .findAny()
                                    .orElse(null);

                            if (response == null) {
                                // Schedule the retry with delay to increase possibility that leases would be refreshed by LeaseTracker
                                // and new attempt will succeed.
                                return supplyAsync(() -> null, delayedExecutor(50, TimeUnit.MILLISECONDS))
                                        .thenComposeAsync(un -> stopLeaseProlongation(groupId, redirectNodeId, endTime), requestsExecutor);
                            } else {
                                return completedFuture(((StopLeaseProlongationMessageResponse) response).deniedLeaseExpirationTime());
                            }
                        });
            });
        }
    }

    /**
     * Creates and starts a new replica.
     *
     * @param raftGroupEventsListener Raft group events listener for raft group starting.
     * @param raftGroupListener Raft group listener for raft group starting.
     * @param isVolatileStorage is table storage volatile?
     * @param snapshotStorageFactory Snapshot storage factory for raft group option's parameterization.
     * @param createListener Due to creation of ReplicaListener in TableManager, the function returns desired listener by created
     *      raft-client inside {@link #startReplicaInternal} method.
     * @param replicaGrpId Replication group id.
     * @param storageIndexTracker Storage index tracker.
     * @param newConfiguration A configuration for new raft group.
     *
     * @return Future that promises ready new replica when done.
     */
    public CompletableFuture<Replica> startReplica(
            RaftGroupEventsListener raftGroupEventsListener,
            RaftGroupListener raftGroupListener,
            boolean isVolatileStorage,
            @Nullable SnapshotStorageFactory snapshotStorageFactory,
            Function<RaftGroupService, ReplicaListener> createListener,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            ReplicationGroupId replicaGrpId,
            PeersAndLearners newConfiguration
    ) throws NodeStoppingException {
        if (!enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return startReplicaInternal(
                    replicaGrpId,
                    snapshotStorageFactory,
                    newConfiguration,
                    raftGroupListener,
                    raftGroupEventsListener,
                    isVolatileStorage,
                    (raftClient) -> new ReplicaImpl(
                            replicaGrpId,
                            createListener.apply(raftClient),
                            storageIndexTracker,
                            clusterNetSvc.topologyService().localMember(),
                            executor,
                            placementDriver,
                            clockService,
                            replicaStateManager::reserveReplica,
                            getPendingAssignmentsSupplier,
                            failureManager
                    )
            );
        } finally {
            leaveBusy();
        }
    }

    /**
     * Starts a replica. If a replica with the same partition id already exists, the method throws an exception.
     *
     * @param replicaGrpId Replication group id.
     * @param snapshotStorageFactory Snapshot storage factory for raft group option's parameterization.
     * @param newConfiguration A configuration for new raft group.
     * @param raftGroupListener Raft group listener for the raft group being started.
     * @param raftGroupEventsListener Raft group events listener for raft group starting.
     * @param isVolatileStorage Whether partition storage is volatile for this partition.
     * @param partitionResources Resources managed by this replica (they will be closed on replica shutdown).
     * @throws NodeStoppingException If node is stopping.
     * @throws ReplicaIsAlreadyStartedException Is thrown when a replica with the same replication group id has already been
     *         started.
     */
    public CompletableFuture<Replica> startReplica(
            ReplicationGroupId replicaGrpId,
            Function<RaftGroupService, ReplicaListener> listenerFactory,
            SnapshotStorageFactory snapshotStorageFactory,
            PeersAndLearners newConfiguration,
            RaftGroupListener raftGroupListener,
            RaftGroupEventsListener raftGroupEventsListener,
            boolean isVolatileStorage,
            ManuallyCloseable partitionResources,
            IgniteSpinBusyLock busyLock
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return startReplicaInternal(
                    replicaGrpId,
                    snapshotStorageFactory,
                    newConfiguration,
                    raftGroupListener,
                    raftGroupEventsListener,
                    isVolatileStorage,
                    (raftClient) -> new ZonePartitionReplicaImpl(
                            replicaGrpId,
                            listenerFactory.apply(raftClient),
                            raftClient,
                            partitionResources
                    )
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Replica> startReplicaInternal(
            ReplicationGroupId replicaGrpId,
            @Nullable SnapshotStorageFactory snapshotStorageFactory,
            PeersAndLearners newConfiguration,
            RaftGroupListener raftGroupListener,
            RaftGroupEventsListener raftGroupEventsListener,
            boolean isVolatileStorage,
            Function<TopologyAwareRaftGroupService, Replica> replicaFactory
    ) throws NodeStoppingException {
        RaftNodeId raftNodeId = new RaftNodeId(replicaGrpId, new Peer(localNodeConsistentId));

        RaftGroupOptions groupOptions = groupOptionsForPartition(isVolatileStorage, snapshotStorageFactory);

        // TODO: move into {@method Replica#shutdown} https://issues.apache.org/jira/browse/IGNITE-22372
        // TODO: use RaftManager interface, see https://issues.apache.org/jira/browse/IGNITE-18273
        TopologyAwareRaftGroupService raftClient = ((Loza) raftManager).startRaftGroupNode(
                raftNodeId,
                newConfiguration,
                raftGroupListener,
                raftGroupEventsListener,
                groupOptions,
                raftGroupServiceFactory
        );

        LOG.info("Replica is about to start [replicationGroupId={}].", replicaGrpId);

        Replica newReplica = replicaFactory.apply(raftClient);

        CompletableFuture<Replica> newReplicaFuture = replicas.compute(replicaGrpId, (k, existingReplicaFuture) -> {
            if (existingReplicaFuture == null || existingReplicaFuture.isDone()) {
                assert existingReplicaFuture == null || isCompletedSuccessfully(existingReplicaFuture);
                LOG.info("Replica is started [replicationGroupId={}].", replicaGrpId);

                return completedFuture(newReplica);
            } else {
                LOG.info("Replica is started, existing replica waiter was completed [replicationGroupId={}].", replicaGrpId);

                existingReplicaFuture.complete(newReplica);

                return existingReplicaFuture;
            }
        });

        var eventParams = new LocalReplicaEventParameters(replicaGrpId);

        return fireEvent(AFTER_REPLICA_STARTED, eventParams)
                .exceptionally(e -> {
                    LOG.error("Error when notifying about AFTER_REPLICA_STARTED event.", e);

                    return null;
                })
                .thenCompose(v -> newReplicaFuture);
    }

    /**
     * Returns future with a replica if it was created or null if there no any replicas starting with given identifier.
     *
     * @param replicationGroupId Table-Partition identifier.
     * @return replica if it was created or null otherwise.
     */
    public CompletableFuture<Replica> replica(ReplicationGroupId replicationGroupId) {
        return replicas.get(replicationGroupId);
    }

    /**
     * Performs a {@code resetPeers} operation on raft node.
     *
     * @param replicaGrpId Replication group ID.
     * @param peersAndLearners New node configuration.
     */
    public void resetPeers(ReplicationGroupId replicaGrpId, PeersAndLearners peersAndLearners) {
        RaftNodeId raftNodeId = new RaftNodeId(replicaGrpId, new Peer(localNodeConsistentId));
        ((Loza) raftManager).resetPeers(raftNodeId, peersAndLearners);
    }

    private RaftGroupOptions groupOptionsForPartition(boolean isVolatileStorage, @Nullable SnapshotStorageFactory snapshotFactory) {
        RaftGroupOptions raftGroupOptions;

        if (isVolatileStorage) {
            LogStorageBudgetView view = ((Loza) raftManager).volatileRaft().logStorageBudget().value();
            raftGroupOptions = RaftGroupOptions.forVolatileStores()
                    .setLogStorageFactory(volatileLogStorageFactoryCreator.factory(view))
                    .raftMetaStorageFactory((groupId, raftOptions) -> new VolatileRaftMetaStorage());
        } else {
            raftGroupOptions = RaftGroupOptions.forPersistentStores();
        }

        raftGroupOptions.snapshotStorageFactory(snapshotFactory);
        raftGroupOptions.maxClockSkew((int) clockService.maxClockSkewMillis());
        raftGroupOptions.commandsMarshaller(raftCommandsMarshaller);

        // TODO: The options will be used by Loza only. Consider rafactoring. see https://issues.apache.org/jira/browse/IGNITE-18273
        partitionRaftConfigurer.configure(raftGroupOptions);

        return raftGroupOptions;
    }

    /**
     * Stops a replica by the partition group id.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is found and closed, false otherwise.
     * @throws NodeStoppingException If the node is stopping.
     */
    public CompletableFuture<Boolean> stopReplica(ReplicationGroupId replicaGrpId) throws NodeStoppingException {
        if (!enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return stopReplicaInternal(replicaGrpId);
        } finally {
            leaveBusy();
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

            if (!enterBusy()) {
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

                    return null;
                });
            } finally {
                leaveBusy();
            }
        });

        return isRemovedFuture
                .thenApplyAsync(v -> {
                    try {
                        // TODO: move into {@method Replica#shutdown} https://issues.apache.org/jira/browse/IGNITE-22372
                        raftManager.stopRaftNodes(replicaGrpId);
                    } catch (NodeStoppingException ignored) {
                        // No-op.
                    }

                    return v;
                }, replicaStateManager.replicaStartStopPool);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
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

        cmgMgr.metaStorageNodes().whenComplete((nodes, e) -> {
            if (e != null) {
                msNodes.completeExceptionally(e);
            } else {
                msNodes.complete(nodes);
            }
        });

        localNodeId = clusterNetSvc.topologyService().localMember().id();

        localNodeConsistentId = clusterNetSvc.topologyService().localMember().name();

        replicaStateManager.start(localNodeId);

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        replicaStateManager.stop();

        blockBusy();

        int shutdownTimeoutSeconds = 10;

        shutdownAndAwaitTermination(scheduledIdleSafeTimeSyncExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(executor, shutdownTimeoutSeconds, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(replicasCreationExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS);

        // There we're closing replicas' futures that was created by requests and should be completed with NodeStoppingException.
        try {
            IgniteUtils.closeAllManually(() -> {
                assert replicas.values().stream().noneMatch(CompletableFuture::isDone)
                        : "There are replicas alive [replicas="
                        + replicas.entrySet().stream().filter(e -> e.getValue().isDone()).map(Entry::getKey).collect(toSet()) + ']';

                replicas.values().forEach(replicaFuture -> replicaFuture.completeExceptionally(new NodeStoppingException()));
            });
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
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
                            .timestamp(clockService.updateClock(requestTimestamp))
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
    private NetworkMessage prepareReplicaResponse(boolean sendTimestamp, ReplicaResult result) {
        if (sendTimestamp) {
            HybridTimestamp commitTs = result.applyResult().commitTimestamp();
            return REPLICA_MESSAGES_FACTORY
                    .timestampAwareReplicaResponse()
                    .result(result.result())
                    .timestamp(commitTs == null ? clockService.current() : commitTs)
                    .build();
        } else {
            return REPLICA_MESSAGES_FACTORY
                    .replicaResponse()
                    .result(result.result())
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
                    .timestamp(clockService.now())
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
        if (!shouldAdvanceIdleSafeTime()) {
            // If previous attempt may still be waiting on the Metastorage SafeTime, we should not send the command ourselves as this
            // might be an indicator that Metastorage SafeTime has stuck for some time; if we send the command, it will have to add its
            // future, increasing (most probably, uselessly) heap pressure.
            return;
        }

        lastIdleSafeTimeProposal = clockService.now();

        for (Entry<ReplicationGroupId, CompletableFuture<Replica>> entry : replicas.entrySet()) {
            try {
                sendSafeTimeSyncIfReplicaReady(entry.getValue());
            } catch (Exception | AssertionError e) {
                LOG.warn("Error while trying to send a safe time sync request [groupId={}]", e, entry.getKey());
            } catch (Error e) {
                LOG.error("Error while trying to send a safe time sync request [groupId={}]", e, entry.getKey());

                failureManager.process(new FailureContext(CRITICAL_ERROR, e));
            }
        }
    }

    private void sendSafeTimeSyncIfReplicaReady(CompletableFuture<Replica> replicaFuture) {
        if (!isCompletedSuccessfully(replicaFuture)) {
            return;
        }

        Replica replica = replicaFuture.join();

        ReplicaSafeTimeSyncRequest req = REPLICA_MESSAGES_FACTORY.replicaSafeTimeSyncRequest()
                .groupId(toReplicationGroupIdMessage(replica.groupId()))
                .build();

        replica.processRequest(req, localNodeId).whenComplete((res, ex) -> {
            if (ex != null && !hasCauseOrSuppressed(ex, NodeStoppingException.class)
                    && !hasCauseOrSuppressed(ex, CancellationException.class)) {
                LOG.error("Could not advance safe time for {} to {}", ex, replica.groupId());
            }
        });
    }

    private boolean shouldAdvanceIdleSafeTime() {
        HybridTimestamp lastProposal = lastIdleSafeTimeProposal;
        if (lastProposal == null) {
            // No previous attempt, we have to do it ourselves.
            return true;
        }

        // This is the actuality time that was needed to be achieved for previous attempt to check that this node is still a primary.
        // If it's already achieved, then previous attempt is unblocked (most probably already finished), so we should proceed.
        // If it's not achieved yet, then the previous attempt is still waiting, so we should skip this round of idle safe time propagation.
        HybridTimestamp requiredLastAttemptActualityTime = lastProposal.addPhysicalTime(clockService.maxClockSkewMillis());

        return placementDriver.isActualAt(requiredLastAttemptActualityTime);
    }

    /**
     * Check if replica is started.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is started.
     */
    @TestOnly
    @VisibleForTesting
    @Deprecated
    public boolean isReplicaStarted(ReplicationGroupId replicaGrpId) {
        CompletableFuture<Replica> replicaFuture = replicas.get(replicaGrpId);
        return replicaFuture != null && isCompletedSuccessfully(replicaFuture);
    }

    /**
     * Can possibly start replica if it's not running or is stopping. Nothing happens if the replica is already running
     * ({@link ReplicaState#ASSIGNED} or {@link ReplicaState#PRIMARY_ONLY}) and {@code forcedAssignments} is {@code null}.
     * If the replica is {@link ReplicaState#ASSIGNED} and {@code forcedAssignments} is not {@code null} then peers will be
     * reset to the given assignments. See {@link ReplicaState} for exact replica state transitions.
     *
     * @param groupId Group id.
     * @param startOperation Replica start operation. Will be called if this method decides to start the replica.
     * @param forcedAssignments Assignments to reset forcibly, if needed. Assignments reset is only available when replica is started.
     * @return Completable future, the result means whether the replica was started.
     */
    public CompletableFuture<Boolean> weakStartReplica(
            ReplicationGroupId groupId,
            Supplier<CompletableFuture<Boolean>> startOperation,
            @Nullable Assignments forcedAssignments
    ) {
        return replicaStateManager.weakStartReplica(groupId, startOperation, forcedAssignments);
    }

    /**
     * Can possibly stop replica if it is running or starting, and is not a primary replica. Relies on the given reason. If
     * the reason is {@link WeakReplicaStopReason#EXCLUDED_FROM_ASSIGNMENTS} then the replica can be not stopped if it is still
     * a primary. If the reason is {@link WeakReplicaStopReason#PRIMARY_EXPIRED} then the replica is stopped only if its state
     * is {@link ReplicaState#PRIMARY_ONLY}, because this assumes that it was excluded from assignments before.
     * See {@link ReplicaState} for exact replica state transitions.
     *
     * @param groupId Group id.
     * @param reason Reason to stop replica.
     * @param stopOperation Replica stop operation.
     * @return Completable future, the result means whether the replica was stopped.
     */
    public CompletableFuture<Void> weakStopReplica(
            ReplicationGroupId groupId,
            WeakReplicaStopReason reason,
            Supplier<CompletableFuture<Void>> stopOperation
    ) {
        return replicaStateManager.weakStopReplica(groupId, reason, stopOperation);
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

    private static class ReplicaStateManager {
        private static final IgniteLogger LOG = Loggers.forClass(ReplicaStateManager.class);

        final Map<ReplicationGroupId, ReplicaStateContext> replicaContexts = new ConcurrentHashMap<>();

        final Executor replicaStartStopPool;

        final ClockService clockService;

        final PlacementDriver placementDriver;

        final ReplicaManager replicaManager;

        volatile UUID localNodeId;

        private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

        ReplicaStateManager(
                Executor replicaStartStopPool,
                ClockService clockService,
                PlacementDriver placementDriver,
                ReplicaManager replicaManager
        ) {
            this.replicaStartStopPool = replicaStartStopPool;
            this.clockService = clockService;
            this.placementDriver = placementDriver;
            this.replicaManager = replicaManager;
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
                        context.assertReservation(replicationGroupId, parameters.startTime());

                        // Unreserve if another replica was elected as primary, only if its lease start time is greater,
                        // otherwise it means that event is too late relatively to lease negotiation start and should be ignored.
                        if (parameters.startTime().compareTo(context.leaseStartTime) > 0) {
                            context.unreserve();

                            if (context.replicaState == ReplicaState.PRIMARY_ONLY) {
                                executeDeferredReplicaStop(replicationGroupId, context);
                            }
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
                            context.assertReservation(parameters.groupId(), parameters.startTime());

                            // Unreserve if primary replica expired, only if its lease start time is equal to reservation time,
                            // otherwise it means that event is too late relatively to lease negotiation start and should be ignored.
                            if (parameters.startTime().equals(context.leaseStartTime)) {
                                context.unreserve();

                                if (context.replicaState == ReplicaState.RESTART_PLANNED) {
                                    executeDeferredReplicaStop(parameters.groupId(), context);
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

        ReplicaStateContext getContext(ReplicationGroupId groupId) {
            return replicaContexts.computeIfAbsent(groupId,
                    // Treat the absence in the map as STOPPED.
                    k -> new ReplicaStateContext(ReplicaState.STOPPED, nullCompletedFuture()));
        }

        /**
         * Can possibly start replica if it's not running or is stopping.
         *
         * @param groupId Group id.
         * @param startOperation Replica start operation.
         * @param forcedAssignments Assignments to reset forcibly, if needed. Assignments reset is only available when replica is started.
         * @return Completable future, the result means whether the replica was started.
         */
        CompletableFuture<Boolean> weakStartReplica(
                ReplicationGroupId groupId,
                Supplier<CompletableFuture<Boolean>> startOperation,
                @Nullable Assignments forcedAssignments
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

                        replicaManager.resetPeers(groupId, fromAssignments(forcedAssignments.nodes()));
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
                    .handleAsync((v, e) -> startOperation.get(), replicaStartStopPool)
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
                    .exceptionally(e -> {
                        LOG.error("Replica start failed [groupId={}]", e, groupId);

                        throw new CompletionException(e);
                    });

            return context.previousOperationFuture;
        }

        /**
         * Can possibly stop replica if it is running or starting, and is not a primary replica. Relies on the given reason. If
         * the reason is {@link WeakReplicaStopReason#EXCLUDED_FROM_ASSIGNMENTS} then the replica can be not stopped if it is still
         * a primary. If the reason is {@link WeakReplicaStopReason#PRIMARY_EXPIRED} then the replica is stopped only if its state
         * is {@link ReplicaState#PRIMARY_ONLY}, because this assumes that it was excluded from assignments before.
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
                            planDeferredReplicaStop(groupId, context, null, stopOperation);
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
                                .thenCompose(leaseExpirationTime ->
                                        planDeferredReplicaStop(groupId, context, leaseExpirationTime, stopOperation)
                                );
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

        private CompletableFuture<Void> stopReplica(
                ReplicationGroupId groupId,
                ReplicaStateContext context,
                Supplier<CompletableFuture<Void>> stopOperation
        ) {
            context.replicaState = ReplicaState.STOPPING;
            context.previousOperationFuture = context.previousOperationFuture
                    .handleAsync((v, e) -> stopOperation.get(), replicaStartStopPool)
                    .thenCompose(stopOperationFuture -> stopOperationFuture.thenApply(v -> {
                        synchronized (context) {
                            context.replicaState = ReplicaState.STOPPED;
                        }

                        LOG.debug("Weak replica stop complete [grpId={}, state={}].", groupId, context.replicaState);

                        return true;
                    }))
                    .exceptionally(e -> {
                        LOG.error("Replica stop failed [groupId={}]", e, groupId);

                        throw new CompletionException(e);
                    });

            return context.previousOperationFuture.thenApply(v -> null);
        }

        private CompletableFuture<Void> planDeferredReplicaStop(
                ReplicationGroupId groupId,
                ReplicaStateContext context,
                @Nullable HybridTimestamp leaseExpirationTime,
                Supplier<CompletableFuture<Void>> deferredStopOperation
        ) {
            synchronized (context) {
                // TODO IGNITE-23702: proper sync with waiting of expiration event, and proper deferred stop after cancellation of
                //     reservation made by a lease that was not negotiated.
                context.deferredStopReadyFuture = leaseExpirationTime == null
                        ? new CompletableFuture<>()
                        : replicaManager.clockService.waitFor(leaseExpirationTime);

                return context.deferredStopReadyFuture
                        .thenComposeAsync(unused -> stopReplica(groupId, context, deferredStopOperation), replicaManager.requestsExecutor);
            }
        }

        private static void executeDeferredReplicaStop(ReplicationGroupId groupId, ReplicaStateContext context) {
            assert context.deferredStopReadyFuture != null : "Stop operation future is not set [groupId=" + groupId + "].";

            context.deferredStopReadyFuture.complete(null);
            context.deferredStopReadyFuture = null;
        }

        /**
         * Reserve replica as primary.
         *
         * @param groupId Group id.
         * @return Whether the replica was successfully reserved.
         */
        boolean reserveReplica(ReplicationGroupId groupId, HybridTimestamp leaseStartTime) {
            ReplicaStateContext context = getContext(groupId);

            synchronized (context) {
                ReplicaState state = context.replicaState;

                if (state == ReplicaState.STOPPING || state == ReplicaState.STOPPED) {
                    if (state == ReplicaState.STOPPED) {
                        replicaContexts.remove(groupId);
                    }

                    if (context.reservedForPrimary) {
                        throw new AssertionError("Unexpected replica reservation with " + state + " state [groupId=" + groupId + "].");
                    }
                } else if (state != ReplicaState.RESTART_PLANNED) {
                    context.reserve(groupId, leaseStartTime);
                }

                return context.reservedForPrimary;
            }
        }

        @TestOnly
        boolean isReplicaPrimaryOnly(ReplicationGroupId groupId) {
            ReplicaStateContext context = getContext(groupId);

            synchronized (context) {
                return context.replicaState == ReplicaState.PRIMARY_ONLY;
            }
        }
    }

    /**
     * Destroys replication protocol storages for the given group ID.
     *
     * @param replicaGrpId Replication group ID.
     * @param isVolatileStorage is table storage volatile?
     * @throws NodeStoppingException If the node is being stopped.
     */
    public void destroyReplicationProtocolStorages(ReplicationGroupId replicaGrpId, boolean isVolatileStorage)
            throws NodeStoppingException {
        RaftNodeId raftNodeId = new RaftNodeId(replicaGrpId, new Peer(localNodeConsistentId));
        RaftGroupOptions groupOptions = groupOptionsForPartition(isVolatileStorage, null);

        ((Loza) raftManager).destroyRaftNodeStorages(raftNodeId, groupOptions);
    }

    private static class ReplicaStateContext {
        /** Replica state. */
        ReplicaState replicaState;

        /**
         * Future of the previous operation, to linearize the starts and stops of replica. The result of the future is whether
         * the operation was actually performed (for example, partition start operation can not start replica or raft node locally).
         */
        CompletableFuture<Boolean> previousOperationFuture;

        /**
         * Whether the replica is reserved to serve as a primary even if it is not included into assignments. If it is {@code} true,
         * then {@link #weakStopReplica(ReplicationGroupId, WeakReplicaStopReason, Supplier)} transfers {@link ReplicaState#ASSIGNED}
         * to {@link ReplicaState#PRIMARY_ONLY} instead of {@link ReplicaState#STOPPING}.
         * Replica is reserved when it is in progress of lease negotiation and stays reserved when it's primary. The negotiation moves
         * this flag to {@code true}. Primary replica expiration or the election of different node as a leaseholder moves this flag
         * to {@code false}.
         */
        boolean reservedForPrimary;

        /**
         * Lease start time of the lease this replica is reserved for, not {@code null} if {@link #reservedForPrimary} is {@code true}.
         */
        @Nullable
        HybridTimestamp leaseStartTime;

        /**
         * Future that should be complete when the deferred stop operation is ready to begin. Deferred stop operation is the stop of
         * replica that was reserved for becoming primary, and needs to be stopped.
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
                throw new IllegalArgumentException(format("Replica reservation failed: newer lease has already reserved this replica ["
                        + "groupId={}, requestedLeaseStartTime={}, newerLeaseStartTime={}].", groupId, leaseStartTime,
                        this.leaseStartTime));
            }
            this.leaseStartTime = leaseStartTime;
            reservedForPrimary = true;
        }

        void unreserve() {
            // TODO IGNITE-23702: should also lead to replica stop if it is PRIMARY_ONLY.
            reservedForPrimary = false;
            leaseStartTime = null;
        }

        void assertReservation(ReplicationGroupId groupId, HybridTimestamp leaseStartTime) {
            assert reservedForPrimary : "Replica is elected as primary but not reserved [groupId="
                    + groupId + ", leaseStartTime=" + leaseStartTime + "].";
            assert leaseStartTime != null : "Replica is reserved but lease start time is null [groupId="
                    + groupId + ", leaseStartTime=" + leaseStartTime + "].";
        }
    }

    /**
     * Replica lifecycle states.
     * <br>
     * Transitions:
     * <br>
     * On {@link #weakStartReplica(ReplicationGroupId, Supplier, Assignments)} (this assumes that the replica is included into assignments):
     * <ul>
     *     <li>if {@link #ASSIGNED}: next state is {@link #ASSIGNED};</li>
     *     <li>if {@link #PRIMARY_ONLY}: next state is {@link #ASSIGNED};</li>
     *     <li>if {@link #STOPPED} or {@link #STOPPING}: next state is {@link #STARTING}, replica is started after stop operation
     *         completes;</li>
 *         <li>if {@link #RESTART_PLANNED}: produces {@link AssertionError} because replica should be stopped first;</li>
     *     <li>if {@link #STARTING}: produces {@link AssertionError}.</li>
     * </ul>
     * On {@link #weakStopReplica(ReplicationGroupId, WeakReplicaStopReason, Supplier)} the next state also depends on given
     * {@link WeakReplicaStopReason}:
     * <ul>
     *     <li>if {@link WeakReplicaStopReason#EXCLUDED_FROM_ASSIGNMENTS}:</li>
     *     <ul>
     *         <li>if {@link #ASSIGNED}: when {@link ReplicaStateContext#reservedForPrimary} is {@code true} then the next state
     *             is {@link #PRIMARY_ONLY}, otherwise the replica is stopped, the next state is {@link #STOPPING};</li>
     *         <li>if {@link #PRIMARY_ONLY} or {@link #STOPPING}: no-op.</li>
     *         <li>if {@link #RESTART_PLANNED} no-op, because replica will be stopped within deferred operation;</li>
     *         <li>if {@link #STARTING}: replica is stopped, the next state is {@link #STOPPING};</li>
     *         <li>if {@link #STOPPED}: replica is stopped.</li>
     *     </ul>
     *     <li>if {@link WeakReplicaStopReason#PRIMARY_EXPIRED}:</li>
     *     <ul>
     *         <li>if {@link #PRIMARY_ONLY} replica is stopped, the next state is {@link #STOPPING}. Otherwise no-op.</li>
 *         </ul>
 *         <li>if {@link WeakReplicaStopReason#RESTART}: this is explicit manual replica restart for disaster recovery purposes,
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
         * Local node, where the replica is located, is included into the union of stable and pending assignments. The replica can
         * be either primary or non-primary. Assumes that the replica is started.
         */
        ASSIGNED,

        /**
         * Local node is excluded from the union of stable and pending assignments but the replica is a primary replica and hence
         * can't be stopped. Assumes that the replica is started.
         */
        PRIMARY_ONLY,

        /**
         * Replica is going to be restarted, this state means that it is primary and needs to wait for lease expiration first.
         * After lease is expired, replica is stopped and transferred to {@link ReplicaState#STOPPING} state.
         */
        RESTART_PLANNED,

        /** Replica is stopping. */
        STOPPING,

        /** Replica is stopped. */
        STOPPED
    }

    /**
     * Reasons to stop a replica.
     */
    public enum WeakReplicaStopReason {
        /** If the local node is excluded from the union of stable and pending assignments. */
        EXCLUDED_FROM_ASSIGNMENTS,

        /** If the primary replica expired (A replica can stay alive when the node is not in assignments, if it's a primary replica). */
        PRIMARY_EXPIRED,

        /** Explicit manual replica restart for disaster recovery purposes. */
        RESTART
    }

    // TODO: IGNITE-22630 Fix serialization into message
    private static ReplicationGroupIdMessage toReplicationGroupIdMessage(ReplicationGroupId replicationGroupId) {
        if (replicationGroupId instanceof TablePartitionId) {
            return toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, (TablePartitionId) replicationGroupId);
        } else if (replicationGroupId instanceof ZonePartitionId) {
            return toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, (ZonePartitionId) replicationGroupId);
        }

        throw new AssertionError("Not supported: " + replicationGroupId);
    }

    private boolean enterBusy() {
        return !busyLock.isWriteLockedByCurrentThread() && busyLock.readLock().tryLock();
    }

    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    private void blockBusy() {
        busyLock.writeLock().lock();
    }
}
