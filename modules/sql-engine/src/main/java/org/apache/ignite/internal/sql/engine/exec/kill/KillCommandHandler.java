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

import java.util.EnumMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.KillHandlerRegistry;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.message.CancelOperationRequest;
import org.apache.ignite.internal.sql.engine.message.CancelOperationResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Handler of SQL KILL command.
 */
public class KillCommandHandler implements KillHandlerRegistry {
    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final EnumMap<CancellableOperationType, OperationKillHandler> localHandlers = new EnumMap<>(CancellableOperationType.class);

    private final EnumMap<CancellableOperationType, OperationKillHandler> clusterHandlers = new EnumMap<>(CancellableOperationType.class);

    private final String localNodeName;

    private final LogicalTopologyService logicalTopologyService;

    private final MessagingService messageService;

    /**
     * Constructor.
     */
    public KillCommandHandler(String localNodeName, LogicalTopologyService logicalTopologyService, MessagingService messageService) {
        this.localNodeName = localNodeName;
        this.logicalTopologyService = logicalTopologyService;
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

            clusterWideHandler = new LocalToClusterKillHandlerWrapper(
                    handler,
                    localNodeName,
                    logicalTopologyService,
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

    @Override
    public OperationKillHandler handler(CancellableOperationType type) {
        return handlerOrThrow(type, false);
    }

    /**
     * Handles the SQL KILL command.
     *
     * @param cmd Kill command.
     * @return Future representing the result of the command execution.
     */
    public CompletableFuture<Boolean> handle(KillCommand cmd) {
        OperationKillHandler handler = handlerOrThrow(cmd.type(), false);

        CompletableFuture<Boolean> killFut = invokeCancel(handler, cmd.operationId());

        if (cmd.noWait()) {
            // Despite the `NO WAIT` flag, we will return an error to the user if it
            // was received immediately. For example, if an incorrect identifier was specified.
            return killFut.isCompletedExceptionally()
                    ? killFut
                    : CompletableFutures.trueCompletedFuture();
        }

        return killFut;
    }

    OperationKillHandler handlerOrThrow(CancellableOperationType type, boolean local) {
        Objects.requireNonNull(type, "type");

        EnumMap<CancellableOperationType, OperationKillHandler> handlers = local ? localHandlers : clusterHandlers;

        OperationKillHandler handler = handlers.get(type);

        if (handler == null) {
            throw new IllegalArgumentException("No handler is registered for the specified type "
                    + "[type=" + type + ", local=" + local + "].");
        }

        return handler;
    }

    private void onMessage(NetworkMessage networkMessage, InternalClusterNode clusterNode, @Nullable Long correlationId) {
        if (networkMessage instanceof CancelOperationRequest) {
            assert correlationId != null;

            try {
                CancelOperationRequest request = (CancelOperationRequest) networkMessage;
                CancellableOperationType type = CancellableOperationType.fromId(request.typeId());
                OperationKillHandler handler = handlerOrThrow(type, true);
                String operationId = request.operationId();

                invokeCancel(handler, operationId).whenComplete(
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

    private static CompletableFuture<Boolean> invokeCancel(OperationKillHandler handler, String operationId) {
        try {
            return handler.cancelAsync(operationId);
        } catch (IllegalArgumentException e) {
            String errMessage = IgniteStringFormatter.format(
                    "Invalid operation ID format [operationId={}, type={}].", operationId, handler.type());

            return CompletableFuture.failedFuture(new SqlException(Sql.RUNTIME_ERR, errMessage, e));
        }
    }

    private static CancelOperationResponse errorResponse(Throwable t) {
        return FACTORY.cancelOperationResponse().error(t).build();
    }
}
