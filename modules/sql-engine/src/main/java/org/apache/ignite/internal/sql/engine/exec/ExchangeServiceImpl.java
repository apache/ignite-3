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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.SqlExceptionMapperUtil;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryTupleMessage;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionEvaluationException;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryBatchMessage;
import org.apache.ignite.internal.sql.engine.message.QueryBatchRequestMessage;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.jetbrains.annotations.Nullable;

/**
 * Message-based implementation of {@link ExchangeService} interface.
 *
 * <p>Provides simple methods of interaction with the mailbox, hiding all the machinery to send and receive messages.
 */
public class ExchangeServiceImpl implements ExchangeService {
    private static final IgniteLogger LOG = Loggers.forClass(ExchangeServiceImpl.class);
    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final MailboxRegistry mailboxRegistry;
    private final MessageService messageService;
    private final ClockService clockService;

    /**
     * Creates the object.
     *
     * @param mailboxRegistry A registry of mailboxes created on the node.
     * @param messageService A messaging service to exchange messages between mailboxes.
     * @param clockService Clock service.
     */
    public ExchangeServiceImpl(
            MailboxRegistry mailboxRegistry,
            MessageService messageService,
            ClockService clockService
    ) {
        this.mailboxRegistry = mailboxRegistry;
        this.messageService = messageService;
        this.clockService = clockService;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        messageService.register((n, m) -> onMessage(n, (QueryBatchRequestMessage) m), SqlQueryMessageGroup.QUERY_BATCH_REQUEST);
        messageService.register((n, m) -> onMessage(n, (QueryBatchMessage) m), SqlQueryMessageGroup.QUERY_BATCH_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> sendBatch(String nodeName, ExecutionId executionId, long fragmentId, long exchangeId, int batchId,
            boolean last, List<BinaryTupleMessage> rows) {

        return messageService.send(
                nodeName,
                FACTORY.queryBatchMessage()
                        .queryId(executionId.queryId())
                        .executionToken(executionId.executionToken())
                        .fragmentId(fragmentId)
                        .exchangeId(exchangeId)
                        .batchId(batchId)
                        .last(last)
                        .rows(rows)
                        .timestamp(clockService.now())
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> request(String nodeName, ExecutionId executionId, long fragmentId, long exchangeId, int amountOfBatches,
            @Nullable SharedState state) {
        return messageService.send(
                nodeName,
                FACTORY.queryBatchRequestMessage()
                        .queryId(executionId.queryId())
                        .executionToken(executionId.executionToken())
                        .fragmentId(fragmentId)
                        .exchangeId(exchangeId)
                        .amountOfBatches(amountOfBatches)
                        .sharedState(state)
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> sendError(String nodeName, ExecutionId executionId, long fragmentId, Throwable error) {
        Throwable traceableErr = ExceptionUtils.unwrapCause(error);

        if (!(traceableErr instanceof TraceableException)) {
            traceableErr = new IgniteInternalException(INTERNAL_ERR, error);
        }

        if (((TraceableException) traceableErr).code() == INTERNAL_ERR) {
            LOG.info(format("Failed to execute query fragment: traceId={}, executionId={}, fragmentId={}",
                    ((TraceableException) traceableErr).traceId(), executionId, fragmentId), error);
        } else if (LOG.isDebugEnabled()) {
            LOG.debug(format("Failed to execute query fragment: traceId={}, executionId={}, fragmentId={}",
                    ((TraceableException) traceableErr).traceId(), executionId, fragmentId), error);
        }

        return messageService.send(
                nodeName,
                FACTORY.errorMessage()
                        .queryId(executionId.queryId())
                        .executionToken(executionId.executionToken())
                        .fragmentId(fragmentId)
                        .traceId(((TraceableException) traceableErr).traceId())
                        .code(((TraceableException) traceableErr).code())
                        .message(traceableErr.getMessage())
                        .build()
        );
    }

    private void onMessage(InternalClusterNode node, QueryBatchRequestMessage msg) {
        ExecutionId executionId = new ExecutionId(msg.queryId(), msg.executionToken());
        CompletableFuture<Outbox<?>> outboxFut = mailboxRegistry.outbox(executionId, msg.exchangeId());

        Consumer<Outbox<?>> onRequestHandler = outbox -> {
            try {
                SharedState state = msg.sharedState();
                if (state != null) {
                    outbox.onRewindRequest(node.name(), state, msg.amountOfBatches());
                } else {
                    outbox.onRequest(node.name(), msg.amountOfBatches());
                }
            } catch (Throwable e) {
                Throwable toUse = convertEvaluationException(e);

                outbox.onError(toUse);

                throw new IgniteInternalException(INTERNAL_ERR, "Unexpected exception", toUse);
            }
        };

        if (outboxFut.isDone()) {
            onRequestHandler.accept(outboxFut.join());
        } else {
            outboxFut.thenAccept(onRequestHandler);
        }
    }

    private void onMessage(InternalClusterNode node, QueryBatchMessage msg) {
        ExecutionId executionId = new ExecutionId(msg.queryId(), msg.executionToken());
        Inbox<?> inbox = mailboxRegistry.inbox(executionId, msg.exchangeId());

        if (inbox != null) {
            try {
                inbox.onBatchReceived(node.name(), msg.batchId(), msg.last(), msg.rows());
            } catch (Throwable e) {
                Throwable toUse = convertEvaluationException(e);

                inbox.onError(toUse);

                if (toUse instanceof IgniteException) {
                    return;
                }

                LOG.warn("Unexpected exception", toUse);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Stale batch message received: [nodeName={}, executionId={}, fragmentId={}, exchangeId={}, batchId={}]",
                    node.name(), executionId, msg.fragmentId(), msg.exchangeId(), msg.batchId());
        }
    }

    private static Throwable convertEvaluationException(Throwable e) {
        if (e instanceof ExpressionEvaluationException) {
            return SqlExceptionMapperUtil.mapToPublicSqlException(e);
        }

        return e;
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        // No-op.
    }
}
