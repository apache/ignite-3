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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.TestOnly;

/**
 * Embedded implementation of the Ignite SQL query facade.
 */
public class IgniteSqlImpl implements IgniteSql, IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteSqlImpl.class);

    /** Session expiration check period in milliseconds. */
    private static final long SESSION_EXPIRE_CHECK_PERIOD = TimeUnit.SECONDS.toMillis(1);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final QueryProcessor queryProcessor;

    private final IgniteTransactions transactions;

    /** Session expiration worker. */
    private final ScheduledExecutorService executor;

    private final ConcurrentMap<SessionId, SessionImpl> sessions = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param queryProcessor Query processor.
     * @param transactions Transactions facade.
     */
    public IgniteSqlImpl(
            String nodeName,
            QueryProcessor queryProcessor,
            IgniteTransactions transactions
    ) {
        this.queryProcessor = queryProcessor;
        this.transactions = transactions;

        executor = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "sql-session-cleanup", true, LOG)
        );
    }

    /** {@inheritDoc} */
    @Override
    public Session createSession() {
        return sessionBuilder().build();
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder sessionBuilder() {
        return new SessionBuilderImpl(
                busyLock, sessions, queryProcessor, transactions, System::currentTimeMillis, new HashMap<>()
        );
    }

    /** {@inheritDoc} */
    @Override
    public Statement createStatement(String query) {
        return new StatementImpl(query);
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder statementBuilder() {
        return new StatementBuilderImpl();
    }

    @Override
    public CompletableFuture<Void> start() {
        executor.scheduleWithFixedDelay(
                () -> {
                    for (SessionImpl session : sessions.values()) {
                        if (session.expired()) {
                            session.closeAsync();
                        }
                    }
                },
                SESSION_EXPIRE_CHECK_PERIOD,
                SESSION_EXPIRE_CHECK_PERIOD,
                TimeUnit.MILLISECONDS
        );

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        executor.shutdownNow();

        sessions.values().forEach(SessionImpl::closeAsync);
        sessions.clear();
    }

    @TestOnly
    public List<Session> sessions() {
        return List.copyOf(sessions.values());
    }
}
