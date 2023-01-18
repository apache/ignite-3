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

import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.replicator.exception.ReplicaIsAlreadyStartedException;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.AwaitReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;
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
    /** Idle safe time propagation period. */
    private static final int IDLE_SAFE_TIME_PROPAGATION_PERIOD_SECONDS = 1;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaManager.class);

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Replica message handler. */
    private final NetworkMessageHandler handler;

    /** Replicas. */
    private final ConcurrentHashMap<ReplicationGroupId, CompletableFuture<Replica>> replicas = new ConcurrentHashMap<>();

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /** Scheduled executor for idle safe time sync. */
    private final ScheduledExecutorService scheduledIdleSafeTimeSyncExecutor =
            Executors.newScheduledThreadPool(1, new NamedThreadFactory("scheduled-idle-safe-time-sync-thread", LOG));

    /** Set of message groups to handler as replica requests. */
    private final Set<Class<?>> messageGroupsToHandle;

    /**
     * Constructor for a    replica service.
     *
     * @param clusterNetSvc Cluster network service.
     * @param clock A hybrid logical clock.
     */
    public ReplicaManager(
            ClusterService clusterNetSvc,
            HybridClock clock,
            Set<Class<?>> messageGroupsToHandle
    ) {
        this.clusterNetSvc = clusterNetSvc;
        this.clock = clock;
        this.messageGroupsToHandle = messageGroupsToHandle;
        this.handler = (message, senderConsistentId, correlationId) -> {
            if (!busyLock.enterBusy()) {
                throw new IgniteException(new NodeStoppingException());
            }

            try {
                if (!(message instanceof ReplicaRequest)) {
                    return;
                }

                ReplicaRequest request = (ReplicaRequest) message;

                // Notify the sender that the Replica is created and ready to process requests.
                if (request instanceof AwaitReplicaRequest) {
                    replicas.compute(request.groupId(), (replicationGroupId, replicaFut) -> {
                        if (replicaFut == null) {
                            replicaFut = new CompletableFuture<>();
                        }

                        if (!replicaFut.isDone()) {
                            replicaFut.thenCompose(
                                    ignore -> {
                                        IgniteUtils.inBusyLock(
                                                busyLock,
                                                () -> sendAwaitReplicaResponse(senderConsistentId, correlationId)
                                        );

                                        return null;
                                    }
                            );

                            return replicaFut;
                        } else {
                            IgniteUtils.inBusyLock(busyLock, () -> sendAwaitReplicaResponse(senderConsistentId, correlationId));

                            return replicaFut;
                        }
                    });

                    return;
                }

                CompletableFuture<Replica> replicaFut = replicas.get(request.groupId());

                HybridTimestamp requestTimestamp = extractTimestamp(request);

                if (replicaFut == null || !replicaFut.isDone()) {
                    sendReplicaUnavailableErrorResponse(senderConsistentId, correlationId, request, requestTimestamp);

                    return;
                }

                // replicaFut is always completed here.
                CompletableFuture<Object> result = replicaFut.join().processRequest(request);

                result.handle((res, ex) -> {
                    NetworkMessage msg;

                    if (ex == null) {
                        msg = prepareReplicaResponse(requestTimestamp, res);
                    } else {
                        LOG.warn("Failed to process replica request [request={}]", ex, request);

                        msg = prepareReplicaErrorResponse(requestTimestamp, ex);
                    }

                    clusterNetSvc.messagingService().respond(senderConsistentId, msg, correlationId);

                    return null;
                });
            } finally {
                busyLock.leaveBusy();
            }
        };
    }

    /**
     * Starts a replica. If a replica with the same partition id already exists, the method throws an exception.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @return New replica.
     * @throws NodeStoppingException If node is stopping.
     * @throws ReplicaIsAlreadyStartedException Is thrown when a replica with the same replication group id has already been started.
     */
    public Replica startReplica(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener
    ) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return startReplicaInternal(replicaGrpId, listener);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for starting a replica.
     *
     * @param replicaGrpId   Replication group id.
     * @param listener Replica listener.
     * @return New replica.
     */
    private Replica startReplicaInternal(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener
    ) {
        Replica newReplica = new Replica(replicaGrpId, listener);

        replicas.compute(replicaGrpId, (replicationGroupId, replicaFut) -> {
            if (replicaFut == null) {
                return CompletableFuture.completedFuture(newReplica);
            } else {
                if (replicaFut.isDone() && !replicaFut.isCancelled() && !replicaFut.isCompletedExceptionally()) {
                    return CompletableFuture.completedFuture(newReplica);
                }

                replicaFut.complete(newReplica);

                return replicaFut;
            }
        });

        return newReplica;
    }

    /**
     * Stops a replica by the partition group id.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is found and closed, false otherwise.
     * @throws NodeStoppingException If the node is stopping.
     */
    public boolean stopReplica(ReplicationGroupId replicaGrpId) throws NodeStoppingException {
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
    private boolean stopReplicaInternal(ReplicationGroupId replicaGrpId) {
        return replicas.remove(replicaGrpId) != null;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        clusterNetSvc.messagingService().addMessageHandler(ReplicaMessageGroup.class, handler);
        messageGroupsToHandle.forEach(mg -> clusterNetSvc.messagingService().addMessageHandler(mg, handler));
        scheduledIdleSafeTimeSyncExecutor.scheduleAtFixedRate(
                this::idleSafeTimeSync,
                0,
                IDLE_SAFE_TIME_PROPAGATION_PERIOD_SECONDS,
                TimeUnit.SECONDS
        );
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        shutdownAndAwaitTermination(scheduledIdleSafeTimeSyncExecutor, 10, TimeUnit.SECONDS);

        assert replicas.isEmpty() : "There are replicas alive [replicas=" + replicas.keySet() + ']';
    }

    /**
     * Extract a hybrid timestamp from timestamp aware request or return null.
     */
    private HybridTimestamp extractTimestamp(ReplicaRequest request) {
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
            ReplicaRequest request,
            HybridTimestamp requestTimestamp
    ) {
        if (requestTimestamp != null) {
            clusterNetSvc.messagingService().respond(
                    senderConsistentId,
                    REPLICA_MESSAGES_FACTORY
                            .errorTimestampAwareReplicaResponse()
                            .throwable(
                                    new ReplicaUnavailableException(
                                            request.groupId(),
                                            clusterNetSvc.topologyService().localMember())
                            )
                            .timestamp(clock.update(requestTimestamp))
                            .build(),
                    correlationId);
        } else {
            clusterNetSvc.messagingService().respond(
                    senderConsistentId,
                    REPLICA_MESSAGES_FACTORY
                            .errorReplicaResponse()
                            .throwable(
                                    new ReplicaUnavailableException(
                                        request.groupId(),
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
    private NetworkMessage prepareReplicaResponse(HybridTimestamp requestTimestamp, Object result) {
        if (requestTimestamp != null) {
            return REPLICA_MESSAGES_FACTORY
                    .timestampAwareReplicaResponse()
                    .result(result)
                    .timestamp(clock.update(requestTimestamp))
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
    private NetworkMessage prepareReplicaErrorResponse(HybridTimestamp requestTimestamp, Throwable ex) {
        if (requestTimestamp != null) {
            return REPLICA_MESSAGES_FACTORY
                    .errorTimestampAwareReplicaResponse()
                    .throwable(ex)
                    .timestamp(clock.update(requestTimestamp))
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

                r.join().processRequest(req);
            }
        });
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
}
