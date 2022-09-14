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

import static org.apache.ignite.internal.util.ExceptionUtils.withCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_COMMON_ERR;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;

/**
 * The service is intended to execute requests on replicas.
 */
public class ReplicaService {
    /** Network timeout. */
    private static final int RPC_TIMEOUT = 3000;

    /** Replica manager. */
    private final ReplicaManager replicaManager;

    /** Message service. */
    private final MessagingService messagingService;

    /** Local node. */
    private final ClusterNode localNode;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /**
     * The constructor of replica client.
     *
     * @param replicaManager   Replica manager.
     * @param messagingService Cluster message service.
     * @param topologyService  Topology service.
     * @param clock A hybrid logical clock.
     */
    public ReplicaService(
            ReplicaManager replicaManager,
            MessagingService messagingService,
            TopologyService topologyService,
            HybridClock clock
    ) {
        this.replicaManager = replicaManager;
        this.messagingService = messagingService;

        this.localNode = topologyService.localMember();
        this.clock = clock;
    }

    /**
     * Sends request to the replica node.
     *
     * @param node Cluster node which holds a replica.
     * @param req  Replica request.
     * @return Response future.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     * @throws ReplicaUnavailableException If replica node does not exists or not started yet.
     */
    private <R> CompletableFuture<R> sendToReplica(ClusterNode node, ReplicaRequest req) throws NodeStoppingException {
        if (localNode.equals(node)) {
            Replica replica = replicaManager.replica(req.groupId());

            if (replica == null) {
                throw new ReplicaUnavailableException(req.groupId(), node);
            }

            return (CompletableFuture<R>) replica.processRequest(req);
        }

        CompletableFuture<R> res = new CompletableFuture<>();

        messagingService.invoke(node.address(), req, RPC_TIMEOUT).whenComplete((response, throwable) -> {
            if (throwable != null) {
                if (throwable instanceof TimeoutException) {
                    res.completeExceptionally(new ReplicationTimeoutException(req.groupId()));
                }

                res.completeExceptionally(withCause(
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
                    res.completeExceptionally(errResp.throwable());
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
     * @return A future holding the response or error if handling of the request resulted in an error.
     * @throws NodeStoppingException If node is stopping.
     * @throws ReplicaUnavailableException If replica node does not exists or not started yet.
     */
    public <R> CompletableFuture<R> invoke(ClusterNode node, ReplicaRequest request) throws NodeStoppingException {
        return sendToReplica(node, request);
    }

    /**
     * Sends a request to the given replica {@code node} and returns a future that will be completed with a result of request processing.
     *
     * @param node      Replica node.
     * @param request   Request.
     * @param storageId Storage id.
     * @return A future holding the response or error if handling of the request resulted in an error.
     * @throws NodeStoppingException If node is stopping.
     * @throws ReplicaUnavailableException If replica node does not exists or not started yet.
     */
    public <R> CompletableFuture<R> invoke(ClusterNode node, ReplicaRequest request, String storageId) throws NodeStoppingException {
        return sendToReplica(node, request);
    }
}
