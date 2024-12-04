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

package org.apache.ignite.internal.sql.engine.kill;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.kill.messages.CancelOperationRequest;
import org.apache.ignite.internal.sql.engine.kill.messages.CancelOperationResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;

/**
 * Wrapper for {@link OperationKillHandler} that calls a local kill handler on each node in the cluster.
 */
class LocalToClusterCancelHandlerWrapper implements OperationKillHandler {
    /** Messages factory. */
    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LocalToClusterCancelHandlerWrapper.class);

    /** Maximum time to wait for a remote response. */
    private static final long RESPONSE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);

    private final OperationKillHandler localHandler;
    private final CancellableOperationType type;
    private final TopologyService topologyService;
    private final MessagingService messageService;

    LocalToClusterCancelHandlerWrapper(
            OperationKillHandler localHandler,
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
    public boolean local() {
        return false;
    }

    @Override
    public CancellableOperationType type() {
        return type;
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
                            .typeId(type.id())
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
                                                + "[operationId={}, typeId={}, node={}].", remoteErr,
                                        request.operationId(), request.typeId(), node.name());
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
                                LOG.warn("Failed to send a request to kill the operation to the remote node "
                                        + "[operationId={}, type={}, node={}].", err, request.operationId(), request.typeId(), node.name());
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
