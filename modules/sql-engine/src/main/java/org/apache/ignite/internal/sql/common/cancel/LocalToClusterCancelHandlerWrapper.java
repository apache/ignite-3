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

package org.apache.ignite.internal.sql.common.cancel;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.sql.common.cancel.api.CancellableOperationType;
import org.apache.ignite.internal.sql.common.cancel.api.OperationCancelHandler;
import org.apache.ignite.internal.sql.common.cancel.messages.CancelOperationRequest;
import org.apache.ignite.internal.sql.common.cancel.messages.CancelOperationResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;

/**
 * Wrapper for {@link OperationCancelHandler} that calls a local cancel handler on each node in the cluster.
 */
class LocalToClusterCancelHandlerWrapper implements OperationCancelHandler {
    /** Messages factory. */
    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LocalToClusterCancelHandlerWrapper.class);

    /** Maximum time to wait for a remote response. */
    private static final long RESPONSE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);

    private final OperationCancelHandler localHandler;
    private final CancellableOperationType type;
    private final TopologyService topologyService;
    private final MessagingService messageService;

    LocalToClusterCancelHandlerWrapper(
            OperationCancelHandler localHandler,
            CancellableOperationType type,
            TopologyService topologyService,
            MessagingService messageService
    ) {
        this.localHandler = localHandler;
        this.type = type;
        this.topologyService = topologyService;
        this.messageService = messageService;
    }

    @Override
    public CompletableFuture<Boolean> cancelAsync(UUID operationId) {
        return localHandler.cancelAsync(operationId)
                .thenCompose((result) -> {
                    if (Boolean.TRUE.equals(result)) {
                        return CompletableFuture.completedFuture(Boolean.TRUE);
                    }

                    CancelOperationRequest request = FACTORY.cancelOperationRequest()
                            .id(operationId)
                            .type(type.name())
                            .build();

                    return broadcastCancel(request);
                });
    }

    private CompletableFuture<Boolean> broadcastCancel(CancelOperationRequest request) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        CompletableFuture<?>[] futures = topologyService.allMembers()
                .stream()
                .filter(node -> !node.name().equals(topologyService.localMember().name()))
                .map(node -> messageService.invoke(node, request, RESPONSE_TIMEOUT_MS)
                        .thenAccept(msg -> {
                            CancelOperationResponse response = (CancelOperationResponse) msg;
                            Throwable remoteErr = response.error();

                            if (remoteErr != null) {
                                LOG.warn("Remote node returned an error while canceling the operation "
                                        + "[operationId={}, type={}, node={}].", remoteErr, request.id(), request.type(), node.name());
                            }

                            if (response.error() != null) {
                                throw new CompletionException(response.error());
                            }

                            Boolean res = response.result();

                            if (Boolean.TRUE.equals(res)) {
                                result.complete(true);
                            }
                        })
                        .whenComplete((ignore, err) -> {
                            if (err != null) {
                                LOG.warn("Failed to send a request to cancel the operation to the remote node "
                                        + "[operationId={}, type={}, node={}].", err, request.id(), request.type(), node.name());
                            }
                        })
                )
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures)
                .whenComplete((unused, throwable) -> {
                    if (result.isDone()) {
                        return;
                    }

                    if (throwable == null) {
                        result.complete(Boolean.FALSE);

                        return;
                    }

                    result.completeExceptionally(throwable);
                });

        return result;
    }
}
