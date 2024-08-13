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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.AFTER_REPLICA_STARTED;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.BEFORE_REPLICA_STOPPED;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.thread.ThreadOperation.TX_STATE_STORAGE_ACCESS;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import org.apache.ignite.internal.affinity.Assignments;
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
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
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
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.thread.ThreadAttributes;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.storage.impl.VolatileRaftMetaStorage;
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
    private final FailureProcessor failureProcessor;

    /** Set of message groups to handler as replica requests. */
    private final Set<Class<?>> messageGroupsToHandle;

    /** Executor. */
    // TODO: IGNITE-20063 Maybe get rid of it
    private final ExecutorService executor;

    private final ReplicaStateManager replicaStateManager;

    private final ExecutorService replicasCreationExecutor;

    private volatile String localNodeId;

    private volatile String localNodeConsistentId;

    /* Temporary converter to support the zone based partitions in tests. **/
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this code
    private Function<ReplicaRequest, ReplicationGroupId> groupIdConverter = r -> r.groupId().asReplicationGroupId();

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
     * @param failureProcessor Failure processor.
     * @param raftCommandsMarshaller Command marshaller for raft groups creation.
     * @param raftGroupServiceFactory A factory for raft-clients creation.
     * @param raftManager The manager made up of songs and words to spite all my troubles is not so bad at all.
     * @param volatileLogStorageFactoryCreator Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for
     *      volatile tables.
     * @param groupIdConverter Temporary converter to support the zone based partitions in tests.
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
            FailureProcessor failureProcessor,
            Marshaller raftCommandsMarshaller,
            TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            RaftManager raftManager,
            LogStorageFactoryCreator volatileLogStorageFactoryCreator,
            Executor replicaStartStopExecutor,
            Function<ReplicaRequest, ReplicationGroupId> groupIdConverter
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
                failureProcessor,
                raftCommandsMarshaller,
                raftGroupServiceFactory,
                raftManager,
                volatileLogStorageFactoryCreator,
                replicaStartStopExecutor
        );

        this.groupIdConverter = groupIdConverter;
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
     * @param failureProcessor Failure processor.
     * @param raftCommandsMarshaller Command marshaller for raft groups creation.
     * @param raftGroupServiceFactory A factory for raft-clients creation.
     * @param raftManager The manager made up of songs and words to spite all my troubles is not so bad at all.
     * @param volatileLogStorageFactoryCreator Creator for {@link org.apache.ignite.internal.raft.storage.LogStorageFactory} for
     *      volatile tables.
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
            FailureProcessor failureProcessor,
            Marshaller raftCommandsMarshaller,
            TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            RaftManager raftManager,
            LogStorageFactoryCreator volatileLogStorageFactoryCreator,
            Executor replicaStartStopExecutor
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
        this.failureProcessor = failureProcessor;
        this.raftCommandsMarshaller = raftCommandsMarshaller;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.raftManager = raftManager;
        this.replicaStateManager = new ReplicaStateManager(replicaStartStopExecutor, clockService, placementDriver, this);

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
                    stopLeaseProlongation(groupId, null);
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

        var msg = (PlacementDriverReplicaMessage) msg0;

        if (!busyLock.enterBusy()) {
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

    private CompletableFuture<Replica> startReplicaInternal(
            RaftGroupEventsListener raftGroupEventsListener,
            RaftGroupListener raftGroupListener,
            boolean isVolatileStorage,
            SnapshotStorageFactory snapshotStorageFactory,
            Function<RaftGroupService, ReplicaListener> createListener,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            TablePartitionId replicaGrpId,
            PeersAndLearners newConfiguration
    ) throws NodeStoppingException {
        RaftNodeId raftNodeId = new RaftNodeId(replicaGrpId, new Peer(localNodeConsistentId));

        RaftGroupOptions groupOptions = groupOptionsForPartition(
                isVolatileStorage,
                snapshotStorageFactory);

        // TODO: move into {@method Replica#shutdown} https://issues.apache.org/jira/browse/IGNITE-22372
        // TODO: use RaftManager interface, see https://issues.apache.org/jira/browse/IGNITE-18273
        CompletableFuture<TopologyAwareRaftGroupService> newRaftClientFut = ((Loza) raftManager).startRaftGroupNode(
                raftNodeId,
                newConfiguration,
                raftGroupListener,
                raftGroupEventsListener,
                groupOptions,
                raftGroupServiceFactory
        );

        return startReplica(
                replicaGrpId,
                newConfiguration,
                createListener,
                storageIndexTracker,
                newRaftClientFut
        );
    }

    /**
     * Creates and starts a new replica.
     *
     * @param raftGroupEventsListener Raft group events listener for raft group starting.
     * @param raftGroupListener Raft group listener for raft group starting.
     * @param isVolatileStorage is table storage volatile?
     * @param snapshotStorageFactory Snapshot storage factory for raft group option's parameterization.
     * @param createListener Due to creation of ReplicaListener in TableManager, the function returns desired listener by created
     *      raft-client inside {@link #startReplica} method.
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
            SnapshotStorageFactory snapshotStorageFactory,
            Function<RaftGroupService, ReplicaListener> createListener,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            TablePartitionId replicaGrpId,
            PeersAndLearners newConfiguration
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return startReplicaInternal(
                    raftGroupEventsListener,
                    raftGroupListener,
                    isVolatileStorage,
                    snapshotStorageFactory,
                    createListener,
                    storageIndexTracker,
                    replicaGrpId,
                    newConfiguration);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Starts a replica. If a replica with the same partition id already exists, the method throws an exception.
     *
     * @param replicaGrpId Replication group id.
     * @param snapshotStorageFactory Snapshot storage factory for raft group option's parameterization.
     * @param newConfiguration A configuration for new raft group.
     * @param raftGroupListener Raft group listener for raft group starting.
     * @param raftGroupEventsListener Raft group events listener for raft group starting.
     * @throws NodeStoppingException If node is stopping.
     * @throws ReplicaIsAlreadyStartedException Is thrown when a replica with the same replication group id has already been
     *         started.
     */
    public CompletableFuture<Replica> startReplica(
            ReplicationGroupId replicaGrpId,
            Function<RaftGroupService, ReplicaListener> listener,
            SnapshotStorageFactory snapshotStorageFactory,
            PeersAndLearners newConfiguration,
            RaftGroupListener raftGroupListener,
            RaftGroupEventsListener raftGroupEventsListener,
            IgniteSpinBusyLock busyLock
    ) throws NodeStoppingException {
        RaftGroupOptions groupOptions = groupOptionsForPartition(
                false,
                snapshotStorageFactory);

        RaftNodeId raftNodeId = new RaftNodeId(replicaGrpId, new Peer(localNodeConsistentId));

        CompletableFuture<TopologyAwareRaftGroupService> newRaftClientFut = ((Loza) raftManager).startRaftGroupNode(
                raftNodeId,
                newConfiguration,
                raftGroupListener,
                raftGroupEventsListener,
                groupOptions,
                raftGroupServiceFactory
        );

        return newRaftClientFut.thenComposeAsync(raftClient -> {
            if (!busyLock.enterBusy()) {
                return failedFuture(new NodeStoppingException());
            }

            try {
                LOG.info("Replica is about to start [replicationGroupId={}].", replicaGrpId);

                Replica newReplica = new ZonePartitionReplicaImpl(
                        replicaGrpId,
                        listener.apply(raftClient),
                        raftClient
                );

                CompletableFuture<Replica> replicaFuture = replicas.compute(replicaGrpId, (k, existingReplicaFuture) -> {
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
            } finally {
                busyLock.leaveBusy();
            }
        }, executor);
    }

    /**
     * Starts a raft-client and pass it to a replica creation if the replica should be started too. If a replica with the same partition id
     * already exists, the method throws an exception.
     * TODO: must be deleted or be private after https://issues.apache.org/jira/browse/IGNITE-22373
     *
     * @param replicaGrpId Replication group id.
     * @param newConfiguration Peers and Learners of the Raft group.
     * @param createListener A clojure that returns done {@link ReplicaListener} by given raft-client {@link RaftGroupService}.
     * @param storageIndexTracker Storage index tracker.
     * @param newRaftClientFut A future that returns created raft-client.
     * @throws NodeStoppingException If node is stopping.
     * @throws ReplicaIsAlreadyStartedException Is thrown when a replica with the same replication group id has already been started.
     */
    @VisibleForTesting
    @Deprecated
    public CompletableFuture<Replica> startReplica(
            ReplicationGroupId replicaGrpId,
            PeersAndLearners newConfiguration,
            Function<RaftGroupService, ReplicaListener> createListener,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            CompletableFuture<TopologyAwareRaftGroupService> newRaftClientFut
    ) throws NodeStoppingException {
        LOG.info("Replica is about to start [replicationGroupId={}].", replicaGrpId);

        return newRaftClientFut
                .thenApplyAsync(createListener, replicasCreationExecutor)
                .thenCompose(replicaListener -> startReplica(replicaGrpId, storageIndexTracker, completedFuture(replicaListener)));
    }

    /**
     * Creates and start new replica.
     * TODO: must be deleted or be private after https://issues.apache.org/jira/browse/IGNITE-22373
     *
     * @param replicaGrpId Replication group id.
     * @param storageIndexTracker Storage index tracker.
     * @param newReplicaListenerFut Future that returns ready ReplicaListener for replica creation.
     * @return Future that promises ready new replica when done.
     */
    @VisibleForTesting
    @Deprecated
    public CompletableFuture<Replica> startReplica(
            ReplicationGroupId replicaGrpId,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            CompletableFuture<ReplicaListener> newReplicaListenerFut
    ) {

        ClusterNode localNode = clusterNetSvc.topologyService().localMember();

        CompletableFuture<Replica> replicaFuture = newReplicaListenerFut.thenCompose(listener -> {
            Replica newReplica = new ReplicaImpl(
                    replicaGrpId,
                    listener,
                    storageIndexTracker,
                    localNode,
                    executor,
                    placementDriver,
                    clockService,
                    replicaStateManager::reserveReplica
            );

            return replicas.compute(replicaGrpId, (k, existingReplicaFuture) -> {
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

    private RaftGroupOptions groupOptionsForPartition(boolean isVolatileStorage, SnapshotStorageFactory snapshotFactory) {
        RaftGroupOptions raftGroupOptions;

        if (isVolatileStorage) {
            LogStorageBudgetView view = ((Loza) raftManager).volatileRaft().logStorage().value();
            raftGroupOptions = RaftGroupOptions.forVolatileStores()
                    .setLogStorageFactory(volatileLogStorageFactoryCreator.factory(view))
                    .raftMetaStorageFactory((groupId, raftOptions) -> new VolatileRaftMetaStorage());
        } else {
            raftGroupOptions = RaftGroupOptions.forPersistentStores();
        }

        raftGroupOptions.snapshotStorageFactory(snapshotFactory);

        raftGroupOptions.commandsMarshaller(raftCommandsMarshaller);

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

                    return null;
                });
            } finally {
                busyLock.leaveBusy();
            }
        });

        return isRemovedFuture
                .thenApply(v -> {
                    try {
                        // TODO: move into {@method Replica#shutdown} https://issues.apache.org/jira/browse/IGNITE-22372
                        raftManager.stopRaftNodes(replicaGrpId);
                    } catch (NodeStoppingException ignored) {
                        // No-op.
                    }
                    return v;
                });
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

        busyLock.block();

        int shutdownTimeoutSeconds = 10;

        shutdownAndAwaitTermination(scheduledIdleSafeTimeSyncExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(executor, shutdownTimeoutSeconds, TimeUnit.SECONDS);
        shutdownAndAwaitTermination(replicasCreationExecutor, shutdownTimeoutSeconds, TimeUnit.SECONDS);

//        assert replicas.values().stream().noneMatch(CompletableFuture::isDone)
//                : "There are replicas alive [replicas="
//                + replicas.entrySet().stream().filter(e -> e.getValue().isDone()).map(Entry::getKey).collect(toSet()) + ']';

        for (CompletableFuture<Replica> replicaFuture : replicas.values()) {
            replicaFuture.completeExceptionally(new NodeStoppingException());
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
    private NetworkMessage prepareReplicaResponse(boolean sendTimestamp, Object result) {
        if (sendTimestamp) {
            return REPLICA_MESSAGES_FACTORY
                    .timestampAwareReplicaResponse()
                    .result(result)
                    .timestamp(clockService.now())
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
                    .groupId(toReplicationGroupIdMessage(replica.groupId()))
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

    @TestOnly
    public boolean isReplicaPrimaryOnly(ReplicationGroupId groupId) {
        return replicaStateManager.isReplicaPrimaryOnly(groupId);
    }

    private static class ReplicaStateManager {
        private static final IgniteLogger LOG = Loggers.forClass(ReplicaStateManager.class);

        final Map<ReplicationGroupId, ReplicaStateContext> replicaContexts = new ConcurrentHashMap<>();

        final Executor replicaStartStopPool;

        final ClockService clockService;

        final PlacementDriver placementDriver;

        final ReplicaManager replicaManager;

        volatile String localNodeId;

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

        void start(String localNodeId) {
            this.localNodeId = localNodeId;
            placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, this::onPrimaryElected);
            placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, this::onPrimaryExpired);
        }

        private CompletableFuture<Boolean> onPrimaryElected(PrimaryReplicaEventParameters parameters) {
            ReplicationGroupId groupId = parameters.groupId();
            ReplicaStateContext context = getContext(groupId);

            synchronized (context) {
                if (localNodeId.equals(parameters.leaseholderId())) {
                    assert context.replicaState != ReplicaState.STOPPED : "Unexpected primary replica state STOPPED [groupId="
                            + groupId + ", leaseStartTime=" + parameters.startTime() + "].";
                } else if (context.reservedForPrimary) {
                    context.assertReservation(groupId, parameters.startTime());

                    // Unreserve if another replica was elected as primary, only if its lease start time is greater,
                    // otherwise it means that event is too late relatively to lease negotiation start and should be ignored.
                    if (parameters.startTime().compareTo(context.leaseStartTime) > 0) {
                        context.unreserve();

                        if (context.replicaState == ReplicaState.PRIMARY_ONLY) {
                            stopReplica(groupId, context, context.deferredStopOperation, WeakReplicaStopReason.PRIMARY_EXPIRED);
                        }
                    }
                }
            }

            return falseCompletedFuture();
        }

        private CompletableFuture<Boolean> onPrimaryExpired(PrimaryReplicaEventParameters parameters) {
            if (localNodeId.equals(parameters.leaseholderId())) {
                ReplicaStateContext context = replicaContexts.get(parameters.groupId());

                if (context != null) {
                    synchronized (context) {
                        context.assertReservation(parameters.groupId(), parameters.startTime());
                        // Unreserve if primary replica expired, only if its lease start time is greater,
                        // otherwise it means that event is too late relatively to lease negotiation start and should be ignored.
                        if (parameters.startTime().equals(context.leaseStartTime)) {
                            context.unreserve();
                        }
                    }
                }
            }

            return falseCompletedFuture();
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
                    }));

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
                            context.deferredStopOperation = stopOperation;
                        } else {
                            return stopReplica(groupId, context, stopOperation, reason);
                        }
                    } else if (state == ReplicaState.STARTING) {
                        return stopReplica(groupId, context, stopOperation, reason);
                    } else if (state == ReplicaState.STOPPED) {
                        // We need to stop replica and destroy storages anyway, because they can be already created.
                        // See TODO-s for IGNITE-19713
                        return stopReplica(groupId, context, stopOperation, reason);
                    } // else: no-op.
                } else if (reason == WeakReplicaStopReason.RESTART) {
                    // Explicit restart: always stop.
                    return stopReplica(groupId, context, stopOperation, reason);
                } else {
                    assert reason == WeakReplicaStopReason.PRIMARY_EXPIRED : "Unknown replica stop reason: " + reason;

                    if (state == ReplicaState.PRIMARY_ONLY) {
                        return stopReplica(groupId, context, stopOperation, reason);
                    } // else: no-op.
                }

                LOG.debug("Weak replica stop complete [grpId={}, state={}].", groupId, context.replicaState);

                return nullCompletedFuture();
            }
        }

        private CompletableFuture<Void> stopReplica(
                ReplicationGroupId groupId,
                ReplicaStateContext context,
                Supplier<CompletableFuture<Void>> stopOperation,
                WeakReplicaStopReason reason
        ) {
            context.replicaState = ReplicaState.STOPPING;
            context.previousOperationFuture = context.previousOperationFuture
                    .handleAsync((v, e) -> stopOperation.get(), replicaStartStopPool)
                    .thenCompose(stopOperationFuture -> stopOperationFuture.thenApply(v -> {
                        synchronized (context) {
                            context.replicaState = ReplicaState.STOPPED;

                            if (reason != WeakReplicaStopReason.RESTART) {
                                // No need to remove the context while restarting, it can lead to the loss of reservation context.
                                replicaContexts.remove(groupId);
                            }
                        }

                        LOG.debug("Weak replica stop complete [grpId={}, state={}].", groupId, context.replicaState);

                        return true;
                    }));

            return context.previousOperationFuture.thenApply(v -> null);
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
                } else {
                    context.reserve(leaseStartTime);
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
         * Replica is reserved when it is primary and when it is in progress of lease negotiation. The negotiation moves this flag to
         * {@code true}. Primary replica expiration or the election of different node as a leaseholder moves this flag to {@code false}.
         */
        boolean reservedForPrimary;

        /**
         * Lease start time of the lease this replica is reserved for, not {@code null} if {@link #reservedForPrimary} is {@code true}.
         */
        @Nullable
        HybridTimestamp leaseStartTime;

        /**
         * Deferred stop operation for replica that was reserved for becoming primary, but hasn't become primary and was excluded from
         * assignments.
         */
        Supplier<CompletableFuture<Void>> deferredStopOperation;

        ReplicaStateContext(ReplicaState replicaState, CompletableFuture<Boolean> previousOperationFuture) {
            this.replicaState = replicaState;
            this.previousOperationFuture = previousOperationFuture;
        }

        void reserve(HybridTimestamp leaseStartTime) {
            reservedForPrimary = true;
            this.leaseStartTime = leaseStartTime;
        }

        void unreserve() {
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
     *         <li>if {@link #STARTING}: replica is stopped, the next state is {@link #STOPPING};</li>
     *         <li>if {@link #STOPPED}: replica is stopped, see TODO-s for IGNITE-19713.</li>
     *     </ul>
     *     <li>if {@link WeakReplicaStopReason#PRIMARY_EXPIRED}:</li>
     *     <ul>
     *         <li>if {@link #PRIMARY_ONLY} replica is stopped, the next state is {@link #STOPPING}. Otherwise no-op.</li>
 *         </ul>
 *         <li>if {@link WeakReplicaStopReason#RESTART}: this is explicit manual replica restart for disaster recovery purposes,
     *         replica is stopped, the next state is {@link #STOPPING}.</li>
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
}
