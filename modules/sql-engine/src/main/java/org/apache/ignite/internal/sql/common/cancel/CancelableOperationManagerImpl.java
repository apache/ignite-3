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

import java.util.EnumMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.sql.common.cancel.api.CancelableOperationManager;
import org.apache.ignite.internal.sql.common.cancel.api.CancelableOperationType;
import org.apache.ignite.internal.sql.common.cancel.api.ClusterWideOperationCancelHandler;
import org.apache.ignite.internal.sql.common.cancel.api.NodeOperationCancelHandler;
import org.apache.ignite.internal.sql.common.cancel.api.OperationCancelHandler;
import org.apache.ignite.internal.sql.common.cancel.messages.CancelOperationRequest;
import org.apache.ignite.internal.sql.common.cancel.messages.CancelOperationResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Manager for cancellable operations.
 */
public class CancelableOperationManagerImpl implements CancelableOperationManager {
    private static final long REMOTE_CANCEL_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);

    private static final IgniteLogger LOG = Loggers.forClass(CancelableOperationManagerImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final EnumMap<CancelableOperationType, OperationCancelHandler> handlers = new EnumMap<>(CancelableOperationType.class);

    private final TopologyService topologyService;

    private final MessagingService messageService;

    /**
     * Constructor.
     */
    public CancelableOperationManagerImpl(TopologyService topologyService, MessagingService messageService) {
        this.topologyService = topologyService;
        this.messageService = messageService;

        messageService.addMessageHandler(SqlQueryMessageGroup.class, this::onMessage);
    }

    @Override
    public void register(OperationCancelHandler handler, CancelableOperationType type) {
        OperationCancelHandler prevHandler = handlers.putIfAbsent(type, handler);

        if (prevHandler != null) {
            throw new IllegalArgumentException("A handler for the specified type has already been registered "
                    + "[type=" + type + ", prev=" + prevHandler + "].");
        }
    }

    @Override
    public ClusterWideOperationCancelHandler handler(CancelableOperationType type) {
        OperationCancelHandler handler = handlerInternal(type);

        if (handler == null) {
            throw new IllegalArgumentException("No registered handler for type: " + type);
        }

        if (handler instanceof NodeOperationCancelHandler) {
            return new NodeToClusterHandlerWrapper((NodeOperationCancelHandler) handler, type);
        }

        assert handler instanceof ClusterWideOperationCancelHandler : handler;

        return (ClusterWideOperationCancelHandler) handler;
    }

    private OperationCancelHandler handlerInternal(CancelableOperationType type) {
        OperationCancelHandler handler = handlers.get(type);

        if (handler == null) {
            throw new IllegalArgumentException("No registered handler for type: " + type);
        }

        return handler;
    }

    private void onMessage(NetworkMessage networkMessage, ClusterNode clusterNode, @Nullable Long correlationId) {
        assert correlationId != null;

        if (networkMessage instanceof CancelOperationRequest) {
            try {
                CancelOperationRequest request = (CancelOperationRequest) networkMessage;
                CancelableOperationType type = CancelableOperationType.valueOf(request.type());
                OperationCancelHandler handler = handlerInternal(type);
                UUID operationId = request.id();

                assert handler instanceof NodeOperationCancelHandler : handler;

                handler.cancelAsync(operationId).whenComplete(
                        (result, throwable) -> {
                            CancelOperationResponse response;

                            if (throwable != null) {
                                response = errorResponse(throwable);
                            } else {
                                response = FACTORY.cancelOperationResponse().result(result).build();
                            }

                            messageService.respond(clusterNode, response, correlationId);
                        }
                );
            } catch (Throwable t) {
                messageService.respond(clusterNode, errorResponse(t), correlationId);
            }
        }
    }

    private static CancelOperationResponse errorResponse(Throwable t) {
        return FACTORY.cancelOperationResponse().throwable(t).build();
    }

    class NodeToClusterHandlerWrapper implements ClusterWideOperationCancelHandler {
        private final NodeOperationCancelHandler handler;
        private final CancelableOperationType type;

        NodeToClusterHandlerWrapper(NodeOperationCancelHandler handler, CancelableOperationType type) {
            this.handler = handler;
            this.type = type;
        }

        @Override
        public CompletableFuture<Boolean> cancelAsync(UUID operationId) {
            return handler.cancelAsync(operationId)
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

            ClusterNode localMember = topologyService.localMember();

            CompletableFuture<?>[] futures = topologyService.allMembers()
                    .stream()
                    .filter(node -> !node.name().equals(localMember.name()))
                    .map(node -> messageService.invoke(node, request, REMOTE_CANCEL_TIMEOUT_MS)
                            .thenAccept(msg -> {
                                CancelOperationResponse response = (CancelOperationResponse) msg;
                                Throwable remoteErr = response.throwable();

                                if (remoteErr != null) {
                                    LOG.warn("Remote node returned an error while canceling the operation "
                                            + "[operationId={}, type={}, node={}].", remoteErr, request.id(), request.type(), node.name());
                                }

                                if (response.throwable() != null) {
                                    throw new CompletionException(response.throwable());
                                }

                                Boolean res = response.result();

                                if (Boolean.TRUE.equals(res)) {
                                    result.complete(true);
                                }
                            }))
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
}
