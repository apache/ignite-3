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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_COMMON_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_TIMEOUT_ERR;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.AwaitReplicaRequest;
import org.apache.ignite.internal.replicator.message.AwaitReplicaResponse;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.TestOnly;

/** The service is intended to execute requests on replicas. */
public class ReplicaService {
    /** Message service. */
    private final MessagingService messagingService;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    private final Executor partitionOperationsExecutor;

    private final ReplicationConfiguration replicationConfiguration;

    /** Requests to retry. */
    private final Map<String, CompletableFuture<NetworkMessage>> pendingInvokes = new ConcurrentHashMap<>();

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /**
     * The constructor of replica client.
     *
     * @param messagingService Cluster message service.
     * @param clock A hybrid logical clock.
     * @param replicationConfiguration Replication configuration.
     */
    @TestOnly
    public ReplicaService(
            MessagingService messagingService,
            HybridClock clock,
            ReplicationConfiguration replicationConfiguration
    ) {
        this(
                messagingService,
                clock,
                ForkJoinPool.commonPool(),
                replicationConfiguration
        );
    }

    /**
     * The constructor of replica client.
     *
     * @param messagingService Cluster message service.
     * @param clock A hybrid logical clock.
     * @param partitionOperationsExecutor Partition operation executor.
     * @param replicationConfiguration Replication configuration.
     */
    public ReplicaService(
            MessagingService messagingService,
            HybridClock clock,
            Executor partitionOperationsExecutor,
            ReplicationConfiguration replicationConfiguration
    ) {
        this.messagingService = messagingService;
        this.clock = clock;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.replicationConfiguration = replicationConfiguration;
    }

    /**
     * Sends request to the replica node.
     *
     * @param targetNodeConsistentId A consistent id of the replica node..
     * @param req  Replica request.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see ReplicaUnavailableException If replica with given replication group id doesn't exist or not started yet.
     * @see ReplicationTimeoutException If the response could not be received due to a timeout.
     */
    private <R> CompletableFuture<R> sendToReplica(String targetNodeConsistentId, ReplicaRequest req) {
        CompletableFuture<R> res = new CompletableFuture<>();

        messagingService.invoke(
                targetNodeConsistentId,
                req,
                replicationConfiguration.rpcTimeout().value()
        ).whenComplete((response, throwable) -> {
            if (throwable != null) {
                throwable = unwrapCause(throwable);

                if (throwable instanceof TimeoutException) {
                    // As a timeout has happened, we are probably on the system delayer thread, we should leave it.
                    partitionOperationsExecutor.execute(() -> res.completeExceptionally(new ReplicationTimeoutException(req.groupId())));
                } else {
                    res.completeExceptionally(withCause(
                            ReplicationException::new,
                            REPLICA_COMMON_ERR,
                            "Failed to process replica request [replicaGroupId=" + req.groupId() + ']',
                            throwable));
                }
            } else {
                assert response instanceof ReplicaResponse : "Unexpected message response [resp=" + response + ']';

                if (response instanceof TimestampAware) {
                    clock.update(((TimestampAware) response).timestamp());
                }

                if (response instanceof ErrorReplicaResponse) {
                    var errResp = (ErrorReplicaResponse) response;

                    if (errResp.throwable() instanceof ReplicaUnavailableException) {
                        CompletableFuture<NetworkMessage> awaitReplicaFut = pendingInvokes.computeIfAbsent(
                                targetNodeConsistentId,
                                consistentId -> {
                                    AwaitReplicaRequest awaitReplicaReq = REPLICA_MESSAGES_FACTORY.awaitReplicaRequest()
                                            .groupId(req.groupId())
                                            .build();

                                    return messagingService.invoke(
                                            targetNodeConsistentId,
                                            awaitReplicaReq,
                                            replicationConfiguration.rpcTimeout().value()
                                    );
                                }
                        );

                        awaitReplicaFut.handle((response0, throwable0) -> {
                            pendingInvokes.remove(targetNodeConsistentId, awaitReplicaFut);

                            if (throwable0 != null) {
                                throwable0 = unwrapCause(throwable0);

                                if (throwable0 instanceof TimeoutException) {
                                    res.completeExceptionally(withCause(
                                            ReplicationTimeoutException::new,
                                            REPLICA_TIMEOUT_ERR,
                                            format(
                                                    "Could not wait for the replica readiness due to timeout [replicaGroupId={}, req={}]",
                                                    req.groupId(),
                                                    req.getClass().getSimpleName()
                                            ),
                                            throwable0));
                                } else {
                                    res.completeExceptionally(withCause(
                                            ReplicationException::new,
                                            REPLICA_COMMON_ERR,
                                            format(
                                                    "Failed to process replica request [replicaGroupId={}, req={}]",
                                                    req.groupId(),
                                                    req.getClass().getSimpleName()
                                            ),
                                            throwable0));
                                }
                            } else {
                                if (response0 instanceof ErrorReplicaResponse) {
                                    res.completeExceptionally(((ErrorReplicaResponse) response0).throwable());
                                } else {
                                    assert response0 instanceof AwaitReplicaResponse :
                                            "Incorrect response type [type=" + response0.getClass().getSimpleName() + ']';

                                    sendToReplica(targetNodeConsistentId, req).whenComplete((r, e) -> {
                                        if (e != null) {
                                            res.completeExceptionally(e);
                                        } else {
                                            res.complete((R) r);
                                        }
                                    });
                                }
                            }

                            return null;
                        });
                    } else {
                        res.completeExceptionally(errResp.throwable());
                    }
                } else {
                    res.complete((R) ((ReplicaResponse) response).result());
                }
            }
        });

        return res;
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a result of request processing.
     *
     * @param node    Replica node.
     * @param request Request.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see ReplicaUnavailableException If replica with given replication group id doesn't exist or not started yet.
     * @see ReplicationTimeoutException If the response could not be received due to a timeout.
     */
    public <R> CompletableFuture<R> invoke(ClusterNode node, ReplicaRequest request) {
        return sendToReplica(node.name(), request);
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a result of request processing.
     *
     * @param replicaConsistentId A consistent id of the replica node.
     * @param request Request.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see ReplicaUnavailableException If replica with given replication group id doesn't exist or not started yet.
     * @see ReplicationTimeoutException If the response could not be received due to a timeout.
     */
    public <R> CompletableFuture<R> invoke(String replicaConsistentId, ReplicaRequest request) {
        return sendToReplica(replicaConsistentId, request);
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a result of request processing.
     *
     * @param node      Replica node.
     * @param request   Request.
     * @param storageId Storage id.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see ReplicaUnavailableException If replica with given replication group id doesn't exist or not started yet.
     * @see ReplicationTimeoutException If the response could not be received due to a timeout.
     */
    public <R> CompletableFuture<R> invoke(ClusterNode node, ReplicaRequest request, String storageId) {
        return sendToReplica(node.name(), request);
    }

    public MessagingService messagingService() {
        return messagingService;
    }
}
