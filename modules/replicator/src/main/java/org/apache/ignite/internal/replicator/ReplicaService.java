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

import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_COMMON_ERR;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.AwaitReplicaRequest;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;

/**
 * The service is intended to execute requests on replicas.
 */
public class ReplicaService {
    /** Network timeout. */
    private static final int RPC_TIMEOUT = 3000;

    /** Message service. */
    private final MessagingService messagingService;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    private final Map<ClusterNode, CompletableFuture<NetworkMessage>> pendingInvokes = new ConcurrentHashMap<>();

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /**
     * The constructor of replica client.
     *
     * @param messagingService Cluster message service.
     * @param clock A hybrid logical clock.
     */
    public ReplicaService(
            MessagingService messagingService,
            HybridClock clock
    ) {
        this.messagingService = messagingService;
        this.clock = clock;
    }

    /**
     * Sends request to the replica node.
     *
     * @param node Cluster node which holds a replica.
     * @param req  Replica request.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see ReplicaUnavailableException If replica with given replication group id doesn't exist or not started yet.
     */
    private <R> CompletableFuture<R> sendToReplica(ClusterNode node, ReplicaRequest req) {

        AtomicReference<CompletableFuture<R>> res = new AtomicReference<>(new CompletableFuture<>());

        // TODO: IGNITE-17824 Use named executor instead of default one in order to process replica Response.
        messagingService.invoke(node, req, RPC_TIMEOUT).whenCompleteAsync((response, throwable) -> {
//            System.out.println("sendToReplica");
            if (throwable != null) {
                if (throwable instanceof CompletionException) {
                    throwable = throwable.getCause();
                }

                if (throwable instanceof TimeoutException) {
                    res.get().completeExceptionally(new ReplicationTimeoutException(req.groupId()));
                }

                res.get().completeExceptionally(withCause(
                        ReplicationException::new,
                        REPLICA_COMMON_ERR,
                        "Failed to process replica request [replicaGroupId=" + req.groupId() + ']',
                        throwable));
            } else {
                assert response instanceof ReplicaResponse : "Unexpected message response [resp=" + response + ']';

                if (response instanceof TimestampAware) {
                    clock.update(((TimestampAware) response).timestamp());
                }

                if (response instanceof ErrorReplicaResponse) {
                    var errResp = (ErrorReplicaResponse) response;

                    if (errResp.throwable() instanceof ReplicaUnavailableException) {
//                        System.out.println("ReplicaUnavailableException qwer");

                        pendingInvokes.compute(node, (clusterNode, fut) -> {
                            AwaitReplicaRequest awaitReplicaRequest = REPLICA_MESSAGES_FACTORY.awaitReplicaRequest()
                                    .groupId(req.groupId())
                                    .build();

                            AtomicReference<CompletableFuture<NetworkMessage>> awaitReplicaRequestFut = new AtomicReference<>(fut);

                            if (fut == null) {
                                awaitReplicaRequestFut.set(messagingService.invoke(node, awaitReplicaRequest, RPC_TIMEOUT)
                                        .whenComplete((response0, throwable0) -> {
                                            pendingInvokes.remove(node);
                                        }));
                            }

                            awaitReplicaRequestFut.get().handle((response0, throwable0) -> {
                                if (throwable0 != null) {
                                    if (throwable0 instanceof CompletionException) {
                                        throwable0 = throwable0.getCause();
                                    }

                                    if (throwable0 instanceof TimeoutException) {
                                        res.get().completeExceptionally(new ReplicationTimeoutException(req.groupId()));
                                    }

                                    res.get().completeExceptionally(withCause(
                                            ReplicationException::new,
                                            REPLICA_COMMON_ERR,
                                            "Failed to process replica request [replicaGroupId=" + req.groupId() + ']',
                                            throwable0));
                                } else {
//                                System.out.println("retryFut");
                                    CompletableFuture<R> retryFut = sendToReplica(node, req);

                                    res.get().thenCompose(ignore0 -> retryFut);

                                    res.get().complete(null);
                                }

                                return null;
                            });

                            return awaitReplicaRequestFut.get();
                        });

                    } else {
                        res.get().completeExceptionally(errResp.throwable());
                    }
                } else {
//                    System.out.println("before res.get().complete((R) ((ReplicaResponse) response).result());");
                    res.get().complete((R) ((ReplicaResponse) response).result());
                }
            }
        });

//        System.out.println("res.get() " + res.get());

        return res.get();
    }

    public static void main(String[] args) {
        CompletableFuture<Object> fut = new CompletableFuture<>();

        fut = fut.thenCompose(x -> {
            System.out.println("111");
            return null;
        });

        fut = fut.thenCompose(x -> {
            System.out.println("222");
            return null;
        });

        fut.complete(null);
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a result of request processing.
     *
     * @param node    Replica node.
     * @param request Request.
     * @return Response future with either evaluation result or completed exceptionally.
     * @see NodeStoppingException If either supplier or demander node is stopping.
     * @see ReplicaUnavailableException If replica with given replication group id doesn't exist or not started yet.
     */
    public <R> CompletableFuture<R> invoke(ClusterNode node, ReplicaRequest request) {
        return sendToReplica(node, request);
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
     */
    public <R> CompletableFuture<R> invoke(ClusterNode node, ReplicaRequest request, String storageId) {
        return sendToReplica(node, request);
    }
}
