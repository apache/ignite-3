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

import java.util.EnumMap;
import java.util.Objects;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.KillHandlerRegistry;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.kill.messages.CancelOperationRequest;
import org.apache.ignite.internal.sql.engine.kill.messages.CancelOperationResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link KillHandlerRegistry}.
 */
public class KillHandlerRegistryImpl implements KillHandlerRegistry {
    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final EnumMap<CancellableOperationType, OperationKillHandler> localHandlers = new EnumMap<>(CancellableOperationType.class);

    private final EnumMap<CancellableOperationType, OperationKillHandler> clusterHandlers = new EnumMap<>(CancellableOperationType.class);

    private final TopologyService topologyService;

    private final MessagingService messageService;

    /**
     * Constructor.
     */
    public KillHandlerRegistryImpl(TopologyService topologyService, MessagingService messageService) {
        this.topologyService = topologyService;
        this.messageService = messageService;

        messageService.addMessageHandler(SqlQueryMessageGroup.class, this::onMessage);
    }

    @Override
    public void register(OperationKillHandler handler) {
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(handler.type(), "handler type cannot be null");

        OperationKillHandler clusterWideHandler;

        if (handler.local()) {
            localHandlers.putIfAbsent(handler.type(), handler);

            clusterWideHandler = new LocalToClusterCancelHandlerWrapper(
                    handler,
                    handler.type(),
                    topologyService,
                    messageService
            );
        } else {
            clusterWideHandler = handler;
        }

        OperationKillHandler prevHandler = clusterHandlers.putIfAbsent(handler.type(), clusterWideHandler);

        if (prevHandler != null) {
            throw new IllegalArgumentException("A handler for the specified type has already been registered "
                    + "[type=" + handler.type() + ", prev=" + handler + "].");
        }
    }

    /**
     * Returns a handler that can abort an operation of the specified type across the entire cluster.
     *
     * @param type Type of the cancellable operation.
     * @return Handler that can abort an operation of the specified type across the entire cluster.
     */
    public OperationKillHandler handler(CancellableOperationType type) {
        return handlerOrThrow(type, false);
    }

    private OperationKillHandler handlerOrThrow(CancellableOperationType type, boolean local) {
        Objects.requireNonNull(type, "type");

        EnumMap<CancellableOperationType, OperationKillHandler> handlers = local ? localHandlers : clusterHandlers;

        OperationKillHandler handler = handlers.get(type);

        if (handler == null) {
            throw new IllegalArgumentException("No handler is registered for the specified type "
                    + "[type=" + type + ", local=" + local + "].");
        }

        return handler;
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
                OperationKillHandler handler = handlerOrThrow(type, true);
                String operationId = request.id();

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
