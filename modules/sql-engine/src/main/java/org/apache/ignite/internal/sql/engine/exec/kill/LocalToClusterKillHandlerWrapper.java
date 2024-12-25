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

package org.apache.ignite.internal.sql.engine.exec.kill;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.message.CancelOperationRequest;
import org.apache.ignite.internal.sql.engine.message.CancelOperationResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.ErrorGroups.Common;

/**
 * Wrapper for {@link OperationKillHandler} that calls a local handler on each node in the cluster.
 */
class LocalToClusterKillHandlerWrapper implements OperationKillHandler {
    /** Messages factory. */
    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LocalToClusterKillHandlerWrapper.class);

    /** Maximum time to wait for a remote response. */
    private static final long RESPONSE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);

    private final OperationKillHandler localHandler;
    private final String localNodeName;
    private final LogicalTopologyService logicalTopologyService;
    private final MessagingService messageService;

    LocalToClusterKillHandlerWrapper(
            OperationKillHandler localHandler,
            String localNodeName,
            LogicalTopologyService logicalTopologyService,
            MessagingService messageService
    ) {
        assert localHandler.local() : "handler must be local";

        this.localHandler = localHandler;
        this.localNodeName = localNodeName;
        this.logicalTopologyService = logicalTopologyService;
        this.messageService = messageService;
    }

    @Override
    public boolean local() {
        return false;
    }

    @Override
    public CancellableOperationType type() {
        return localHandler.type();
    }

    @Override
    public CompletableFuture<Boolean> cancelAsync(String operationId) {
        return localHandler.cancelAsync(operationId)
                .thenCompose((result) -> {
                    if (Boolean.TRUE.equals(result)) {
                        return CompletableFuture.completedFuture(Boolean.TRUE);
                    }

                    CancelOperationRequest request = FACTORY.cancelOperationRequest()
                            .operationId(operationId)
                            .typeId(localHandler.type().id())
                            .build();

                    return broadcastCancel(request);
                });
    }

    private CompletableFuture<Boolean> broadcastCancel(CancelOperationRequest request) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        List<CompletableFuture<?>> futures = logicalTopologyService.localLogicalTopology().nodes()
                .stream()
                .filter(node -> !node.name().equals(localNodeName))
                .map(node -> messageService.invoke(node, request, RESPONSE_TIMEOUT_MS)
                        .whenComplete((msg, invokeErr) -> {
                            if (msg != null) {
                                CancelOperationResponse response = (CancelOperationResponse) msg;
                                Throwable remoteErr = response.error();

                                if (remoteErr != null) {
                                    String err = format("Remote node returned an error while canceling the operation ["
                                            + "operationId={}, typeId={}, node={}].", request.operationId(), request.typeId(), node.name());

                                    throw new IgniteInternalException(Common.INTERNAL_ERR, err, remoteErr);
                                }

                                Boolean res = response.result();

                                if (Boolean.TRUE.equals(res)) {
                                    result.complete(true);
                                }
                            }

                            if (invokeErr != null) {
                                String err = format("Failed to send a request to cancel the operation to the remote node "
                                        + "[operationId={}, typeId={}, node={}].", request.operationId(), request.typeId(), node.name());

                                throw new IgniteInternalException(Common.INTERNAL_ERR, err, invokeErr);
                            }
                        })
                )
                .collect(Collectors.toList());

        CompletableFutures.allOf(futures)
                .whenComplete((unused, firstErr) -> {
                    if (firstErr == null) {
                        if (!result.isDone()) {
                            result.complete(Boolean.FALSE);
                        }

                        return;
                    }

                    Throwable resultErr = Commons.deriveExceptionFromListOfFutures(futures);

                    if (result.isDone()) {
                        // If the cancellation was successful, just print a warning.
                        LOG.warn("Distributed cancel operation succeeded, but the request failed on some nodes "
                                + "[operationId={}, typeId={}]", resultErr, request.operationId(), request.typeId());

                        return;
                    }

                    result.completeExceptionally(resultErr);
                });

        return result;
    }
}
