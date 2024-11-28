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
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.sql.common.cancel.api.CancelHandlerRegistry;
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
 * Implementation of {@link CancelHandlerRegistry}.
 */
public class CancelHandlerRegistryImpl implements CancelHandlerRegistry {
    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final EnumMap<CancelableOperationType, OperationCancelHandler> handlers = new EnumMap<>(CancelableOperationType.class);

    private final TopologyService topologyService;

    private final MessagingService messageService;

    /**
     * Constructor.
     */
    public CancelHandlerRegistryImpl(TopologyService topologyService, MessagingService messageService) {
        this.topologyService = topologyService;
        this.messageService = messageService;

        messageService.addMessageHandler(SqlQueryMessageGroup.class, this::onMessage);
    }

    @Override
    public void register(OperationCancelHandler handler, CancelableOperationType type) {
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(type, "type");

        if (!(handler instanceof NodeOperationCancelHandler) && !(handler instanceof ClusterWideOperationCancelHandler)) {
            throw new IllegalArgumentException("Unsupported handler type [cls=" + handler.getClass().getName() + "].");
        }

        OperationCancelHandler prevHandler = handlers.putIfAbsent(type, handler);

        if (prevHandler != null) {
            throw new IllegalArgumentException("A handler for the specified type has already been registered "
                    + "[type=" + type + ", prev=" + prevHandler + "].");
        }
    }

    @Override
    public ClusterWideOperationCancelHandler handler(CancelableOperationType type) {
        OperationCancelHandler handler = handlerOrThrow(type);

        if (handler instanceof NodeOperationCancelHandler) {
            return new NodeToClusterCancelHandlerWrapper(
                    (NodeOperationCancelHandler) handler,
                    type,
                    topologyService,
                    messageService
            );
        }

        assert handler instanceof ClusterWideOperationCancelHandler : handler;

        return (ClusterWideOperationCancelHandler) handler;
    }

    private OperationCancelHandler handlerOrThrow(CancelableOperationType type) {
        Objects.requireNonNull(type, "type");
        OperationCancelHandler handler = handlers.get(type);

        if (handler == null) {
            throw new IllegalArgumentException("No registered handler [type=" + type + "].");
        }

        return handler;
    }

    private static CancelOperationResponse errorResponse(Throwable t) {
        return FACTORY.cancelOperationResponse().throwable(t).build();
    }

    private void onMessage(NetworkMessage networkMessage, ClusterNode clusterNode, @Nullable Long correlationId) {
        if (networkMessage instanceof CancelOperationRequest) {
            assert correlationId != null;

            try {
                CancelOperationRequest request = (CancelOperationRequest) networkMessage;
                CancelableOperationType type = CancelableOperationType.valueOf(request.type());
                OperationCancelHandler handler = handlerOrThrow(type);
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
}
