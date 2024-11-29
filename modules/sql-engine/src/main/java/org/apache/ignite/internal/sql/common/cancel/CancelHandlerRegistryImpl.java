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
import org.apache.ignite.internal.sql.common.cancel.api.CancellableOperationType;
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

    private final EnumMap<CancellableOperationType, CancelHandlerDescriptor> handlers = new EnumMap<>(CancellableOperationType.class);

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
    public void register(CancellableOperationType type, OperationCancelHandler handler, boolean local) {
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(type, "type");

        CancelHandlerDescriptor prevHandler = handlers.putIfAbsent(type, new CancelHandlerDescriptor(handler, local));

        if (prevHandler != null) {
            throw new IllegalArgumentException("A handler for the specified type has already been registered "
                    + "[type=" + type + ", prev=" + prevHandler.handler + "].");
        }
    }

    @Override
    public OperationCancelHandler handler(CancellableOperationType type) {
        CancelHandlerDescriptor handlerDescriptor = handlerOrThrow(type);

        OperationCancelHandler handler = handlerDescriptor.handler;

        if (handlerDescriptor.local) {
            return new LocalToClusterCancelHandlerWrapper(
                    handler,
                    type,
                    topologyService,
                    messageService
            );
        }

        return handler;
    }

    private CancelHandlerDescriptor handlerOrThrow(CancellableOperationType type) {
        Objects.requireNonNull(type, "type");
        CancelHandlerDescriptor handlerDescriptor = handlers.get(type);

        if (handlerDescriptor == null) {
            throw new IllegalArgumentException("No registered handler [type=" + type + "].");
        }

        return handlerDescriptor;
    }

    private static CancelOperationResponse errorResponse(Throwable t) {
        return FACTORY.cancelOperationResponse().error(t).build();
    }

    private void onMessage(NetworkMessage networkMessage, ClusterNode clusterNode, @Nullable Long correlationId) {
        if (networkMessage instanceof CancelOperationRequest) {
            assert correlationId != null;

            try {
                CancelOperationRequest request = (CancelOperationRequest) networkMessage;
                CancellableOperationType type = CancellableOperationType.valueOf(request.type());
                CancelHandlerDescriptor handlerDescriptor = handlerOrThrow(type);
                OperationCancelHandler handler = handlerDescriptor.handler;
                UUID operationId = request.id();

                assert handlerDescriptor.local : "Handler must be local " + handler;

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

    private static class CancelHandlerDescriptor {
        private final OperationCancelHandler handler;
        private final boolean local;

        private CancelHandlerDescriptor(OperationCancelHandler handler, boolean local) {
            this.handler = handler;
            this.local = local;
        }
    }
}
