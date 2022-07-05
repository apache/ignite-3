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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.replicator.exception.ExceptionUtils;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.CleanupResponse;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequestLocator;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.replicator.message.WaiteOperationsResultResponse;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;

/**
 * The service is intended to execute requests on replicas.
 * TODO:IGNITE-17255 Implement ReplicaService.
 */
public class ReplicaService {
    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Network timeout. */
    private static final int RPC_TIMEOUT = 3000;

    /** Replica manager. */
    private final ReplicaManager replicaManager;

    /** Message service. */
    private final MessagingService messagingService;

    /** Local node. */
    private final ClusterNode localNode;

    /**
     * The constructor of replica client.
     *
     * @param replicaManager   Replica manager.
     * @param messagingService Cluster message service.
     * @param topologyService  Topology service.
     */
    public ReplicaService(
            ReplicaManager replicaManager,
            MessagingService messagingService,
            TopologyService topologyService
    ) {
        this.replicaManager = replicaManager;
        this.messagingService = messagingService;

        this.localNode = topologyService.localMember();
    }

    /**
     * Sends request to the replica node.
     *
     * @param node Cluster node which holds a replica.
     * @param req  Replica request.
     * @return Response future.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     */
    private CompletableFuture<ReplicaResponse> sendToReplica(ClusterNode node, ReplicaRequest req) throws NodeStoppingException {
        if (localNode.equals(node)) {
            Replica replica = replicaManager.replica(req.locator().groupId());

            if (replica == null) {
                //TODO:IGNITE-17255 Provide an exceptional response when the replica is absent.
            }

            return CompletableFuture.completedFuture(replica.processRequest(req));
        }

        return messagingService.invoke(node.address(), req, RPC_TIMEOUT).thenApply(msg -> {
            assert msg instanceof ReplicaResponse : IgniteStringFormatter.format("Unexpected message response [resp={}]", msg);

            return (ReplicaResponse) msg;
        }).whenComplete((response, throwable) -> {
            if (throwable != null) {
                if (throwable instanceof TimeoutException) {
                    throw new ReplicationTimeoutException(req.locator().groupId());
                }

                throw new ReplicationException(req.locator().groupId(), throwable);
            }

            if (response instanceof ErrorReplicaResponse) {
                var errResp = (ErrorReplicaResponse) response;

                throw ExceptionUtils.error(errResp.errorTraceId(), errResp.errorCode(), errResp.errorClassName(), errResp.errorMessage(),
                        errResp.errorStackTrace());
            }
        });
    }

    /**
     * Passes a request to replication. The result future is completed with instant response. The completion means the requested node
     * received the request, but the full process of replication might not complete yet. Use {@link ReplicaResponse#operationId()} to wait
     * of entire process.
     *
     * @param node    Replica node.
     * @param request Request.
     * @return A future holding the response.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     */
    public CompletableFuture<ReplicaResponse> invoke(ClusterNode node, ReplicaRequest request) throws NodeStoppingException {
        return sendToReplica(node, request);
    }

    /**
     * Passes a request to replication. The result future is completed with instant response. The completion means the requested node
     * received the request, but the full process of replication might not complete yet. Use {@link ReplicaResponse#operationId()} to wait
     * of entire process.
     *
     * @param node      Replica node.
     * @param request   Request.
     * @param storageId Storage id.
     * @return A future holding the response.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     */
    public CompletableFuture<ReplicaResponse> invoke(ClusterNode node, ReplicaRequest request, String storageId)
            throws NodeStoppingException {
        return sendToReplica(node, request);

    }

    /**
     * Waits of replication operations.
     *
     * @param node                Replica node.
     * @param requestGroupLocator Locator is determining a set of operation over one replica group.
     * @param operationIds        Ids operation to wait.
     * @return Future to the map of an operation id to its result.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     */
    public CompletableFuture<Map<UUID, Object>> waitForResult(ClusterNode node, ReplicaRequestLocator requestGroupLocator,
            UUID... operationIds) throws NodeStoppingException {
        if (operationIds == null || operationIds.length == 0) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        return sendToReplica(node, REPLICA_MESSAGES_FACTORY
                .waiteOperationsResultRequest()
                .locator(requestGroupLocator)
                .operationIds(Arrays.asList(operationIds))
                .build())
                .thenApply(replicaResponse -> {
                    assert replicaResponse instanceof WaiteOperationsResultResponse : IgniteStringFormatter.format(
                            "Response type is incorrect [type={}, expectedType={}]", replicaResponse.getClass().getSimpleName(),
                            WaiteOperationsResultResponse.class.getSimpleName());

                    var waitOpsResp = (WaiteOperationsResultResponse) replicaResponse;

                    return waitOpsResp.results();
                });
    }

    /**
     * Clears results on the replica node. After the method is invoked, waiting of replication operations through {@link
     * ReplicaService#waitForResult(ClusterNode, ReplicaRequestLocator, UUID...)} with the same {@link ReplicaRequestLocator} are not
     * possible.
     *
     * @param node                Replica node.
     * @param requestGroupLocator Locator is determining a set of operation over one replica group.
     * @return Response future.
     * @throws NodeStoppingException Is thrown when the node is stopping.
     */
    public CompletableFuture<Void> clearResult(ClusterNode node, ReplicaRequestLocator requestGroupLocator) throws NodeStoppingException {
        return sendToReplica(node, REPLICA_MESSAGES_FACTORY
                .cleanupRequest()
                .locator(requestGroupLocator)
                .build())
                .thenApply(replicaResponse -> {
                    assert replicaResponse instanceof CleanupResponse : IgniteStringFormatter.format(
                            "Response type is incorrect [type={}, expectedType={}]", replicaResponse.getClass().getSimpleName(),
                            WaiteOperationsResultResponse.class.getSimpleName());

                    return null;
                });
    }
}
