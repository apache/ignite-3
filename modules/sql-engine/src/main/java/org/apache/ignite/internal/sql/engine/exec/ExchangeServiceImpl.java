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

import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.message.InboxCloseMessage;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryBatchAcknowledgeMessage;
import org.apache.ignite.internal.sql.engine.message.QueryBatchMessage;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;

/**
 * ExchangeServiceImpl. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ExchangeServiceImpl implements ExchangeService {
    private static final IgniteLogger LOG = Loggers.forClass(ExchangeServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final ClusterNode localNode;

    private final QueryTaskExecutor taskExecutor;

    private final MailboxRegistry mailboxRegistry;

    private final MessageService msgSrvc;

    /**
     * Constructor. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ExchangeServiceImpl(
            ClusterNode localNode,
            QueryTaskExecutor taskExecutor,
            MailboxRegistry mailboxRegistry,
            MessageService msgSrvc
    ) {
        this.localNode = localNode;
        this.taskExecutor = taskExecutor;
        this.mailboxRegistry = mailboxRegistry;
        this.msgSrvc = msgSrvc;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        msgSrvc.register((n, m) -> onMessage(n, (InboxCloseMessage) m), SqlQueryMessageGroup.INBOX_CLOSE_MESSAGE);
        msgSrvc.register((n, m) -> onMessage(n, (QueryBatchAcknowledgeMessage) m), SqlQueryMessageGroup.QUERY_BATCH_ACK);
        msgSrvc.register((n, m) -> onMessage(n, (QueryBatchMessage) m), SqlQueryMessageGroup.QUERY_BATCH_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> void sendBatch(String nodeName, UUID qryId, long fragmentId, long exchangeId, int batchId,
            boolean last, List<RowT> rows) throws IgniteInternalCheckedException {
        msgSrvc.send(
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
    public void acknowledge(String nodeName, UUID qryId, long fragmentId, long exchangeId, int batchId)
            throws IgniteInternalCheckedException {
        msgSrvc.send(
                nodeName,
                FACTORY.queryBatchAcknowledgeMessage()
                        .queryId(qryId)
                        .fragmentId(fragmentId)
                        .exchangeId(exchangeId)
                        .batchId(batchId)
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public void closeQuery(String nodeName, UUID qryId) throws IgniteInternalCheckedException {
        msgSrvc.send(
                nodeName,
                FACTORY.queryCloseMessage()
                        .queryId(qryId)
                        .build()
        );
    }

    /** {@inheritDoc} */
    @Override
    public void closeInbox(String nodeName, UUID qryId, long fragmentId, long exchangeId) throws IgniteInternalCheckedException {
        msgSrvc.send(
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
        msgSrvc.send(
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
        return msgSrvc.alive(nodeName);
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

    private void onMessage(String nodeName, QueryBatchAcknowledgeMessage msg) {
        Outbox<?> outbox = mailboxRegistry.outbox(msg.queryId(), msg.exchangeId());

        if (outbox != null) {
            try {
                outbox.onAcknowledge(nodeName, msg.batchId());
            } catch (Throwable e) {
                outbox.onError(e);

                throw new IgniteInternalException(UNEXPECTED_ERR, "Unexpected exception", e);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Stale acknowledge message received: [nodeName={}, queryId={}, fragmentId={}, exchangeId={}, batchId={}]",
                    nodeName, msg.queryId(), msg.fragmentId(), msg.exchangeId(), msg.batchId());
        }
    }

    private void onMessage(String nodeName, QueryBatchMessage msg) {
        Inbox<?> inbox = mailboxRegistry.inbox(msg.queryId(), msg.exchangeId());

        if (inbox == null && msg.batchId() == 0) {
            // first message sent before a fragment is built
            // note that an inbox source fragment id is also used as an exchange id
            Inbox<?> newInbox = new Inbox<>(baseInboxContext(nodeName, msg.queryId(), msg.fragmentId()),
                    this, mailboxRegistry, msg.exchangeId(), msg.exchangeId());

            inbox = mailboxRegistry.register(newInbox);
        }

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

    /**
     * Get minimal execution context to meet Inbox needs.
     */
    private ExecutionContext<?> baseInboxContext(String nodeName, UUID qryId, long fragmentId) {
        return new ExecutionContext<>(
                BaseQueryContext.builder()
                        .logger(LOG)
                        .build(),
                taskExecutor,
                qryId,
                localNode,
                nodeName,
                new FragmentDescription(
                        fragmentId,
                        null,
                        null,
                        Long2ObjectMaps.emptyMap()),
                null,
                Map.of(),
                null);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        // No-op.
    }
}
