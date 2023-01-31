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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Common.UNEXPECTED_ERR;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.message.InboxCloseMessage;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryBatchMessage;
import org.apache.ignite.internal.sql.engine.message.QueryBatchRequestMessage;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;

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

    /**
     * Creates the object.
     *
     * @param mailboxRegistry A registry of mailboxes created on the node.
     * @param messageService A messaging service to exchange messages between mailboxes.
     */
    public ExchangeServiceImpl(
            MailboxRegistry mailboxRegistry,
            MessageService messageService
    ) {
        this.mailboxRegistry = mailboxRegistry;
        this.messageService = messageService;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        messageService.register((n, m) -> onMessage(n, (InboxCloseMessage) m), SqlQueryMessageGroup.INBOX_CLOSE_MESSAGE);
        messageService.register((n, m) -> onMessage(n, (QueryBatchRequestMessage) m), SqlQueryMessageGroup.QUERY_BATCH_REQUEST);
        messageService.register((n, m) -> onMessage(n, (QueryBatchMessage) m), SqlQueryMessageGroup.QUERY_BATCH_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> void sendBatch(String nodeName, UUID qryId, long fragmentId, long exchangeId, int batchId,
            boolean last, List<RowT> rows) throws IgniteInternalCheckedException {
        messageService.send(
                nodeName,
                FACTORY.queryBatchMessage()
                        .queryId(qryId)
                        .fragmentId(fragmentId)
                        .exchangeId(exchangeId)
                        .batchId(batchId)
                        .last(last)
                        .rows(Commons.cast(rows))
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public void request(String nodeName, UUID queryId, long fragmentId, long exchangeId, int amountOfBatches)
            throws IgniteInternalCheckedException {
        messageService.send(
                nodeName,
                FACTORY.queryBatchRequestMessage()
                        .queryId(queryId)
                        .fragmentId(fragmentId)
                        .exchangeId(exchangeId)
                        .amountOfBatches(amountOfBatches)
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public void closeQuery(String nodeName, UUID qryId) throws IgniteInternalCheckedException {
        messageService.send(
                nodeName,
                FACTORY.queryCloseMessage()
                        .queryId(qryId)
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public void closeInbox(String nodeName, UUID qryId, long fragmentId, long exchangeId) throws IgniteInternalCheckedException {
        messageService.send(
                nodeName,
                FACTORY.inboxCloseMessage()
                        .queryId(qryId)
                        .fragmentId(fragmentId)
                        .exchangeId(exchangeId)
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public void sendError(String nodeName, UUID qryId, long fragmentId, Throwable err) throws IgniteInternalCheckedException {
        messageService.send(
                nodeName,
                FACTORY.errorMessage()
                        .queryId(qryId)
                        .fragmentId(fragmentId)
                        .error(err)
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public boolean alive(String nodeName) {
        return messageService.alive(nodeName);
    }

    private void onMessage(String nodeName, InboxCloseMessage msg) {
        Collection<Inbox<?>> inboxes = mailboxRegistry.inboxes(msg.queryId(), msg.fragmentId(), msg.exchangeId());

        if (!nullOrEmpty(inboxes)) {
            for (Inbox<?> inbox : inboxes) {
                inbox.context().execute(inbox::close, inbox::onError);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Stale inbox cancel message received [nodeName={}, queryId={}, fragmentId={}, exchangeId={}]",
                    nodeName, msg.queryId(), msg.fragmentId(), msg.exchangeId());
        }
    }

    private void onMessage(String nodeName, QueryBatchRequestMessage msg) {
        CompletableFuture<Outbox<?>> outboxFut = mailboxRegistry.outbox(msg.queryId(), msg.exchangeId());

        Consumer<Outbox<?>> onRequestHandler = outbox -> {
            try {
                outbox.onRequest(nodeName, msg.amountOfBatches());
            } catch (Throwable e) {
                outbox.onError(e);

                throw new IgniteInternalException(UNEXPECTED_ERR, "Unexpected exception", e);
            }
        };

        if (outboxFut.isDone()) {
            onRequestHandler.accept(outboxFut.join());
        } else {
            outboxFut.thenAccept(onRequestHandler);
        }
    }

    private void onMessage(String nodeName, QueryBatchMessage msg) {
        Inbox<?> inbox = mailboxRegistry.inbox(msg.queryId(), msg.exchangeId());

        if (inbox != null) {
            try {
                inbox.onBatchReceived(nodeName, msg.batchId(), msg.last(), Commons.cast(msg.rows()));
            } catch (Throwable e) {
                inbox.onError(e);

                throw new IgniteInternalException(UNEXPECTED_ERR, "Unexpected exception", e);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Stale batch message received: [nodeName={}, queryId={}, fragmentId={}, exchangeId={}, batchId={}]",
                    nodeName, msg.queryId(), msg.fragmentId(), msg.exchangeId(), msg.batchId());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        // No-op.
    }
}
