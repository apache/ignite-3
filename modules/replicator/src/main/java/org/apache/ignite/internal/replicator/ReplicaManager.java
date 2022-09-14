/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.replicator.exception.ReplicaAlreadyIsStartedException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponseBuilder;
import org.apache.ignite.internal.replicator.message.ErrorTimestampAwareReplicaResponseBuilder;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

/**
 * Manager to rule replicas.
 * Only a single instance of the class exists in Ignite node.
 * This class allow to start/stop/get a replica.
 */
public class ReplicaManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaManager.class);

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Cluster network service. */
    private final ClusterService clusterNetSvc;

    /** Replica message handler. */
    private final NetworkMessageHandler handler;

    /** Replicas. */
    private final ConcurrentHashMap<String, Replica> replicas = new ConcurrentHashMap<>();

    /** A hybrid logical clock. */
    private final HybridClock clock;

    // TODO: sanpwc tmp, remove
    private final ConcurrentHashSet<Class<?>> registeredMessageGroups;

    /**
     * Constructor for replica service.
     *
     * @param clusterNetSvc Cluster network service.
     * @param clock A hybrid logical clock.
     */
    public ReplicaManager(ClusterService clusterNetSvc, HybridClock clock) {
        this.clusterNetSvc = clusterNetSvc;
        this.clock = clock;
        this.handler = (message, senderAddr, correlationId) -> {
            if (!busyLock.enterBusy()) {
                throw new IgniteException(new NodeStoppingException());
            }

            try {
                assert message instanceof ReplicaRequest : IgniteStringFormatter.format("Unexpected message [message={}]", message);

                ReplicaRequest request = (ReplicaRequest) message;

                HybridTimestamp requestTimestamp = extractTimestamp(request);

                Replica replica = replicas.get(request.groupId());

                if (replica == null) {
                    sendReplicaUnavailableErrorResponse(senderAddr, correlationId, request, requestTimestamp);
                }

                CompletableFuture<Object> result = replica.processRequest(request);

                result.handle((res, ex) -> {
                    NetworkMessage msg;

                    if (ex == null) {
                        msg = prepareReplicaResponse(requestTimestamp, res);
                    } else {
                        msg = prepareReplicaErrorResponse(request, requestTimestamp, ex);
                    }

                    clusterNetSvc.messagingService().respond(senderAddr, msg, correlationId);

                    return null;
                });
            } finally {
                busyLock.leaveBusy();
            }
        };
        this.registeredMessageGroups = new ConcurrentHashSet<>();
    }

    /**
     * Gets a replica by its replica group id.
     *
     * @param replicaGrpId Replication group id.
     * @return Instance of the replica or {@code null} if the replica is not started.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     */
    public Replica replica(String replicaGrpId) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            return replicas.get(replicaGrpId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Starts a replica. If a replica with the same partition id is already exist the method throws an exception.
     *
     * @param replicaGrpId Replication group id.
     * @param raftGroupService Raft group service.
     * @param listener Replica listener.
     * @return Replica.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     * @throws ReplicaAlreadyIsStartedException Is thrown when a replica with the same replication group id already started.
     */
    public Replica startReplica(
            String replicaGrpId,
            RaftGroupService raftGroupService,
            ReplicaListener listener) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            // TODO: sanpwc tmp, remove
            listener.messageGroups().forEach(mg -> {
                if (registeredMessageGroups.add(mg)) {
                    clusterNetSvc.messagingService().addMessageHandler(mg, handler);
                }
            });

            return startReplicaInternal(replicaGrpId, raftGroupService, listener);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for start a replica.
     *
     * @param replicaGrpId   Replication group id.
     * @param raftGroupService Raft group service.
     * @param listener Replica listener.
     * @return Replica.
     */
    private Replica startReplicaInternal(String replicaGrpId, RaftGroupService raftGroupService, ReplicaListener listener) {
        var replica = new Replica(replicaGrpId, raftGroupService, listener);

        Replica previous = replicas.putIfAbsent(replicaGrpId, replica);

        if (previous != null) {
            throw new ReplicaAlreadyIsStartedException(replicaGrpId);
        }

        return replica;
    }

    /**
     * Stops a replica by the partition group id.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is found and closed, false otherwise.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     */
    public boolean stopReplica(String replicaGrpId) throws NodeStoppingException {
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
     * An internal method for stopping a replica.
     *
     * @param replicaGrpId Replication group id.
     * @return True if the replica is found and closed, false otherwise.
     */
    private boolean stopReplicaInternal(String replicaGrpId) {
        Replica replica = replicas.remove(replicaGrpId);

        if (replica == null) {
            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        clusterNetSvc.messagingService().addMessageHandler(ReplicaMessageGroup.class, handler);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        assert replicas.isEmpty() : IgniteStringFormatter.format("There are replicas alive [replicas={}]", replicas.keySet());
    }

    /**
     * Determines whether a replication group should be started locally
     * according to a collection of nodes that should have a replication group.
     */
    public boolean shouldHaveReplicationGroupLocally(Collection<ClusterNode> replicas) {
        String locNodeName = clusterNetSvc.topologyService().localMember().name();

        return replicas.stream().anyMatch(r -> locNodeName.equals(r.name()));
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
            NetworkAddress senderAddr,
            @Nullable Long correlationId,
            ReplicaRequest request,
            HybridTimestamp requestTimestamp) {
        var traceId = UUID.randomUUID();

        String errorMessage = IgniteStringFormatter.format("Replica is not ready "
                        + "[replicationGroupId={}, node={}]",
                request.groupId(), clusterNetSvc.topologyService().localMember());

        if (requestTimestamp != null) {
            clusterNetSvc.messagingService().respond(
                    senderAddr,
                    REPLICA_MESSAGES_FACTORY
                            .errorTimestampAwareReplicaResponse()
                            .errorMessage(errorMessage)
                            .errorCode(Replicator.REPLICA_UNAVAILABLE_ERR)
                            .errorTraceId(traceId)
                            .timestamp(clock.update(requestTimestamp))
                            .build(),
                    correlationId);
        } else {
            clusterNetSvc.messagingService().respond(
                    senderAddr,
                    REPLICA_MESSAGES_FACTORY
                            .errorReplicaResponse()
                            .errorMessage(errorMessage)
                            .errorCode(Replicator.REPLICA_UNAVAILABLE_ERR)
                            .errorTraceId(traceId)
                            .build(),
                    correlationId);
        }

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
    private NetworkMessage prepareReplicaErrorResponse(ReplicaRequest request, HybridTimestamp requestTimestamp, Throwable ex) {
        var traceId = UUID.randomUUID();

        LOG.warn("Exception was thrown [traceId={}]", ex, traceId);

        String errorMessage = IgniteStringFormatter.format("Process replication response finished with "
                + "exception [replicaGrpId={}, msg={}]", request.groupId(), ex.getMessage());

        if (requestTimestamp != null) {
            ErrorTimestampAwareReplicaResponseBuilder errorBuilder = REPLICA_MESSAGES_FACTORY
                    .errorTimestampAwareReplicaResponse()
                    .errorClassName(ex.getClass().getName())
                    .errorTraceId(traceId)
                    .timestamp(clock.update(requestTimestamp));

            if (ex instanceof IgniteInternalException) {
                return errorBuilder.errorMessage(ex.getMessage())
                        .errorCode(((IgniteInternalException) ex).code())
                        .build();
            } else {
                return errorBuilder.errorMessage(errorMessage)
                        .errorCode(Replicator.REPLICA_COMMON_ERR)
                        .build();
            }
        } else {
            ErrorReplicaResponseBuilder errorBuilder = REPLICA_MESSAGES_FACTORY
                    .errorReplicaResponse()
                    .errorClassName(ex.getClass().getName())
                    .errorTraceId(traceId);

            if (ex instanceof IgniteInternalException) {
                return errorBuilder.errorMessage(ex.getMessage())
                        .errorCode(((IgniteInternalException) ex).code())
                        .build();
            } else {
                return errorBuilder.errorMessage(errorMessage)
                        .errorCode(Replicator.REPLICA_COMMON_ERR)
                        .build();
            }
        }
    }
}
