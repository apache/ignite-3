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
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.listener.ListenerCompoundResponse;
import org.apache.ignite.internal.replicator.listener.ListenerFutureResponse;
import org.apache.ignite.internal.replicator.listener.ListenerInstantResponse;
import org.apache.ignite.internal.replicator.listener.ListenerResponse;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.CleanupRequest;
import org.apache.ignite.internal.replicator.message.InstantResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaRequestLocator;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.replicator.message.WaiteOperationsResultRequest;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.MessagingService;

/**
 * Replica server.
 * TODO:IGNITE-17257 Implement Replica server-side logic.
 */
public class Replica {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(Replica.class);

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /** Replica group identity, this id is the same as the considered partition's id. */
    private final String replicaGrpId;

    /** Replica listener. */
    private final ReplicaListener listener;

    private final MessagingService messagingSvc;

    /**
     * The map matches an operation id to the future of operation result.
     * The first id is a business transaction id within which the operation is handled.
     * The second one is an id of operation (the id is locally generated when the replication request is received).
     * Operation future is a leaf element for waiting the operation completion and receiving a result.
     */
    private final ConcurrentHashMap<UUID, ConcurrentHashMap<UUID, CompletableFuture>> ops = new ConcurrentHashMap<>();

    /**
     * The constructor of replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     */
    public Replica(
            String replicaGrpId,
            ReplicaListener listener,
            MessagingService messagingSvc
    ) {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
        this.messagingSvc = messagingSvc;
    }

    /**
     * Process a replication request on the replica.
     *
     * @param request Request to replication.
     * @return Response.
     */
    public CompletableFuture<Object> processRequest(ReplicaRequest request) { // define proper set of exceptions that might be thrown.
        assert replicaGrpId.equals(request.groupId()) : IgniteStringFormatter.format(
                "Partition mismatch: request does not match the replica [reqReplicaGrpId={}, replicaGrpId={}]", request.groupId(),
                replicaGrpId);

        //TODO:IGNITE-17378 Check replica is alive.

        try {
            if (request instanceof WaiteOperationsResultRequest) {
                return handleWaitOperationsResultRequest((WaiteOperationsResultRequest) request);
            }

            if (request instanceof CleanupRequest) {
                return handleCleanupRequest((CleanupRequest) request);
            }

            return listener.invoke(request);





            if (listResp instanceof ListenerCompoundResponse) {
                var listCompResp = (ListenerCompoundResponse) listResp;

                ops.computeIfAbsent(listCompResp.operationLocator().operationId(), uuid -> new ConcurrentHashMap<>())
                        .put(listCompResp.operationLocator().operationId(), listCompResp.resultFuture());

                return REPLICA_MESSAGES_FACTORY.compoundResponse()
                        .operationId(listCompResp.operationLocator().operationId())
                        .result(listCompResp.result())
                        .build();
            }

            if (listResp instanceof ListenerFutureResponse) {
                var listFutResp = (ListenerFutureResponse) listResp;

                ops.computeIfAbsent(listFutResp.operationLocator().operationContextId(), uuid -> new ConcurrentHashMap<>())
                        .put(listFutResp.operationLocator().operationId(), listFutResp.resultFuture());

                return REPLICA_MESSAGES_FACTORY.futureResponse()
                        .operationId(listFutResp.operationLocator().operationId())
                        .build();
            }

            assert listResp instanceof ListenerInstantResponse : IgniteStringFormatter.format(
                    "Unexpected listener response type [type={}]", listResp.getClass().getSimpleName());

            var listInstResp = (ListenerInstantResponse) listResp;

            return REPLICA_MESSAGES_FACTORY.instantResponse()
                    .result(listInstResp.result())
                    .build();

        } catch (Exception ex) {
            var traceId = UUID.randomUUID();

            LOG.warn("Exception was thrown [traceId={}]", ex, traceId);

            return REPLICA_MESSAGES_FACTORY
                    .errorReplicaResponse()
                    .errorMessage(IgniteStringFormatter.format("Process replication response finished with exception "
                            + "[replicaGrpId={}, msg={}]", replicaGrpId, ex.getMessage()))
                    .errorCode(Replicator.REPLICA_COMMON_ERR)
                    .errorClassName(ex.getClass().getName())
                    .errorTraceId(traceId)
                    .build();
        }
    }

    /**
     * Handles a cleanup request.
     *
     * @param cleanRequest Cleanup request.
     * @return Cleanup response.
     */
    private InstantResponse handleCleanupRequest(CleanupRequest cleanRequest) {
        ops.remove(cleanRequest.operationContextId());

        ListenerResponse listResp = listener.invoke(cleanRequest);

        assert listResp instanceof ListenerInstantResponse : IgniteStringFormatter.format(
                "Unexpected listener response type [type={}]", listResp.getClass().getSimpleName());

        var listInstResp = (ListenerInstantResponse) listResp;

        return REPLICA_MESSAGES_FACTORY.instantResponse()
                .result(listInstResp.result())
                .build();
    }

    /**
     * Handles a wait operation request.
     *
     * @param opsRequest Wait operation request.
     * @return Wait operations result.
     */
    private InstantResponse handleWaitOperationsResultRequest(WaiteOperationsResultRequest opsRequest) {
        Collection<ReplicaRequestLocator> opIds = opsRequest.operationLocators();

        HashMap<UUID, Object> opRes = null;

        if (opIds != null && !opIds.isEmpty()) {
            opRes = new HashMap<>(opIds.size());

            for (ReplicaRequestLocator locator : opIds) {
                ConcurrentHashMap<UUID, CompletableFuture> prefixOps = ops.get(locator.operationContextId());

                if (prefixOps != null) {
                    CompletableFuture opFut = prefixOps.get(locator.operationId());

                    if (opFut != null) {
                        opRes.put(locator.operationId(), opFut.join());
                    }
                }
            }
        }

        return REPLICA_MESSAGES_FACTORY
                .instantResponse()
                .result(opRes)
                .build();
    }
}
