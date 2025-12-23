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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.ExceptionUtils.matchAny;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.GROUP_OVERLOADED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_COMMON_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_MISS_ERR;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_TIMEOUT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.exception.AwaitReplicaTimeoutException;
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
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** The service is intended to execute requests on replicas. */
public class ReplicaService {
    /** Message service. */
    private final MessagingService messagingService;

    /** A hybrid logical clock service. */
    private final ClockService clockService;

    private final Executor partitionOperationsExecutor;

    private final ReplicationConfiguration replicationConfiguration;

    private @Nullable final ScheduledExecutorService retryExecutor;

    /** Requests to retry. */
    private final Map<String, CompletableFuture<NetworkMessage>> pendingInvokes = new ConcurrentHashMap<>();

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /**
     * The constructor of replica client.
     *
     * @param messagingService Cluster message service.
     * @param clockService A hybrid logical clock service.
     * @param replicationConfiguration Replication configuration.
     */
    @TestOnly
    public ReplicaService(
            MessagingService messagingService,
            ClockService clockService,
            ReplicationConfiguration replicationConfiguration
    ) {
        this(
                messagingService,
                clockService,
                ForkJoinPool.commonPool(),
                replicationConfiguration,
                null
        );
    }

    /**
     * The constructor of replica client.
     *
     * @param messagingService Cluster message service.
     * @param clockService A hybrid logical clock service.
     * @param partitionOperationsExecutor Partition operation executor.
     * @param replicationConfiguration Replication configuration.
     * @param retryExecutor Retry executor.
     */
    public ReplicaService(
            MessagingService messagingService,
            ClockService clockService,
            Executor partitionOperationsExecutor,
            ReplicationConfiguration replicationConfiguration,
            @Nullable ScheduledExecutorService retryExecutor
    ) {
        this.messagingService = messagingService;
        this.clockService = clockService;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.replicationConfiguration = replicationConfiguration;
        this.retryExecutor = retryExecutor;
    }

    private <R> CompletableFuture<R> sendToReplica(String targetNodeConsistentId, ReplicaRequest req) {
        return (CompletableFuture<R>) sendToReplicaRaw(targetNodeConsistentId, req).thenApply(res -> res.result());
    }

    /**
     * Sends request to the replica node and provides raw response.
     *
     * <p>If the replica is not yet available, the method will automatically wait for the replica to become ready
     * before retrying the request. The wait is bounded by the RPC timeout configured in {@code ReplicationConfiguration}.
     *
     * @param targetNodeConsistentId A consistent id of the replica node.
     * @param req Replica request.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see AwaitReplicaTimeoutException If the replica does not become ready within the RPC timeout period.
     * @see ReplicationTimeoutException If the response could not be received due to a timeout.
     */
    private CompletableFuture<ReplicaResponse> sendToReplicaRaw(String targetNodeConsistentId, ReplicaRequest req) {
        CompletableFuture<ReplicaResponse> res = new CompletableFuture<>();

        messagingService.invoke(
                targetNodeConsistentId,
                req,
                replicationConfiguration.rpcTimeoutMillis().value()
        ).whenComplete((response, throwable) -> {
            if (throwable != null) {
                throwable = unwrapCause(throwable);

                if (throwable instanceof TimeoutException) {
                    // As a timeout has happened, we are probably on the system delayer thread, we should leave it.
                    partitionOperationsExecutor.execute(
                            () -> res.completeExceptionally(new ReplicationTimeoutException(req.groupId().asReplicationGroupId()))
                    );
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
                    clockService.updateClock(((TimestampAware) response).timestamp());
                }

                if (response instanceof ErrorReplicaResponse) {
                    var errResp = (ErrorReplicaResponse) response;

                    if (errResp.throwable() instanceof ReplicaUnavailableException) {
                        CompletableFuture<NetworkMessage> requestFuture = new CompletableFuture<>();

                        CompletableFuture<NetworkMessage> awaitReplicaFut = pendingInvokes.computeIfAbsent(
                                targetNodeConsistentId,
                                consistentId -> requestFuture
                        );

                        // Means we have put this future, so proceed with the call.
                        // We use such approach here instead of sending network message in the computeIfAbsent lambda to avoid deadlocks.
                        if (awaitReplicaFut == requestFuture) {
                            AwaitReplicaRequest awaitReplicaReq = REPLICA_MESSAGES_FACTORY.awaitReplicaRequest()
                                    .groupId(req.groupId())
                                    .build();

                            messagingService.invoke(
                                    targetNodeConsistentId,
                                    awaitReplicaReq,
                                    replicationConfiguration.rpcTimeoutMillis().value()
                            ).whenComplete((networkMessage, e) -> {
                                if (e != null) {
                                    awaitReplicaFut.completeExceptionally(e);
                                } else {
                                    awaitReplicaFut.complete(networkMessage);
                                }
                            });
                        }

                        // Use handleAsync to avoid interaction in the network thread
                        awaitReplicaFut.handleAsync((response0, throwable0) -> {
                            pendingInvokes.remove(targetNodeConsistentId, awaitReplicaFut);

                            if (throwable0 != null) {
                                throwable0 = unwrapCause(throwable0);

                                if (throwable0 instanceof TimeoutException) {
                                    res.completeExceptionally(withCause(
                                            AwaitReplicaTimeoutException::new,
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

                                    sendToReplicaRaw(targetNodeConsistentId, req).whenComplete((r, e) -> {
                                        if (e != null) {
                                            res.completeExceptionally(e);
                                        } else {
                                            res.complete(r);
                                        }
                                    });
                                }
                            }

                            return null;
                        }, partitionOperationsExecutor);
                    } else {
                        int replicaOperationRetryInterval = replicationConfiguration.replicaOperationRetryIntervalMillis().value();
                        if (retryExecutor != null
                                && matchAny(unwrapCause(errResp.throwable()), ACQUIRE_LOCK_ERR, REPLICA_MISS_ERR, GROUP_OVERLOADED_ERR)
                                && replicaOperationRetryInterval > 0) {
                            retryExecutor.schedule(
                                    // Need to resubmit again to pool which is valid for synchronous IO execution.
                                    () -> partitionOperationsExecutor.execute(() -> res.completeExceptionally(errResp.throwable())),
                                    replicaOperationRetryInterval,
                                    MILLISECONDS
                            );
                        } else {
                            res.completeExceptionally(errResp.throwable());
                        }
                    }
                } else {
                    res.complete((ReplicaResponse) response);
                }
            }
        });

        return res;
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a result of request processing.
     *
     * <p>If the replica is not yet available, the method will automatically wait for the replica to become ready
     * before retrying the request. The wait is bounded by the RPC timeout configured in {@code ReplicationConfiguration}.
     *
     * @param node Replica node.
     * @param request Request.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see AwaitReplicaTimeoutException If the replica does not become ready within the RPC timeout period.
     * @see ReplicationTimeoutException If the response could not be received due to a timeout.
     */
    public <R> CompletableFuture<R> invoke(InternalClusterNode node, ReplicaRequest request) {
        return invokeRaw(node, request).thenApply(r -> (R) r.result());
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a result of request processing.
     *
     * <p>If the replica is not yet available, the method will automatically wait for the replica to become ready
     * before retrying the request. The wait is bounded by the RPC timeout configured in {@code ReplicationConfiguration}.
     *
     * @param replicaConsistentId A consistent id of the replica node.
     * @param request Request.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see AwaitReplicaTimeoutException If the replica does not become ready within the RPC timeout period.
     * @see ReplicationTimeoutException If the response could not be received due to a timeout.
     */
    public <R> CompletableFuture<R> invoke(String replicaConsistentId, ReplicaRequest request) {
        return sendToReplica(replicaConsistentId, request);
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a result of request processing.
     *
     * <p>If the replica is not yet available, the method will automatically wait for the replica to become ready
     * before retrying the request. The wait is bounded by the RPC timeout configured in {@code ReplicationConfiguration}.
     *
     * @param node Replica node.
     * @param request Request.
     * @param storageId Storage id.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see AwaitReplicaTimeoutException If the replica does not become ready within the RPC timeout period.
     * @see ReplicationTimeoutException If the response could not be received due to a timeout.
     */
    public <R> CompletableFuture<R> invoke(InternalClusterNode node, ReplicaRequest request, String storageId) {
        return sendToReplica(node.name(), request);
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a raw response.
     * This can be used to carry additional metadata to the caller.
     *
     * @param node Cluster node.
     * @param request The request.
     * @return Response future with either evaluation raw response or completed exceptionally.
     */
    public CompletableFuture<ReplicaResponse> invokeRaw(InternalClusterNode node, ReplicaRequest request) {
        return invokeRaw(node.name(), request);
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a raw response.
     * This can be used to carry additional metadata to the caller.
     *
     * @param targetNodeConsistentId A consistent id of the replica node.
     * @param request The request.
     * @return Response future with either evaluation raw response or completed exceptionally.
     */
    public CompletableFuture<ReplicaResponse> invokeRaw(String targetNodeConsistentId, ReplicaRequest request) {
        return sendToReplicaRaw(targetNodeConsistentId, request);
    }

    public MessagingService messagingService() {
        return messagingService;
    }
}
