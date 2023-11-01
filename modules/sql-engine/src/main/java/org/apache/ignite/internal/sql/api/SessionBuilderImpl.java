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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.AbstractSession;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.session.SessionProperty;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;

/**
 * Session builder implementation.
 */
public class SessionBuilderImpl implements SessionBuilder {

    public static final long DEFAULT_QUERY_TIMEOUT = 0;
    public static final long DEFAULT_SESSION_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

    private final QueryProcessor qryProc;

    private final Map<String, Object> props;

    private IgniteTransactions transactions;

    private long queryTimeout = DEFAULT_QUERY_TIMEOUT;

    private long sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    private String schema = AbstractSession.DEFAULT_SCHEMA;

    private int pageSize = AbstractSession.DEFAULT_PAGE_SIZE;

    /**
     * Session builder constructor.
     *
     * @param qryProc SQL query processor.
     * @param transactions Transactions facade.
     * @param props Initial properties.
     */
    SessionBuilderImpl(QueryProcessor qryProc, IgniteTransactions transactions, Map<String, Object> props) {
        this.qryProc = qryProc;
        this.transactions = transactions;
        this.props = props;
    }

    /**
     * Gets an Ignite transactions facade.
     *
     * @return Ignite transactions.
     */
    public IgniteTransactions igniteTransactions() {
        return transactions;
    }

    /**
     * Sets an Ignite transactions facade.
     *
     * @param transactions Ignite transactions.
     * @return {@code this} for chaining.
     */
    public SessionBuilder igniteTransactions(IgniteTransactions transactions) {
        this.transactions = transactions;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public long defaultQueryTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(queryTimeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder defaultQueryTimeout(long timeout, TimeUnit timeUnit) {
        this.queryTimeout = timeUnit.toMillis(timeout);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public long idleTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(sessionTimeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder idleTimeout(long timeout, TimeUnit timeUnit) {
        this.sessionTimeout = timeUnit.toMillis(timeout);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder defaultSchema(String schema) {
        this.schema = schema;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public int defaultPageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder defaultPageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(String name) {
        return props.get(name);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder property(String name, @Nullable Object value) {
        props.put(name, value);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public Session build() {
        PropertiesHolder propsHolder = PropertiesHelper.newBuilder()
                .set(SessionProperty.IDLE_TIMEOUT, sessionTimeout)
                .set(QueryProperty.QUERY_TIMEOUT, queryTimeout)
                .set(QueryProperty.DEFAULT_SCHEMA, schema)
                .build();

        SessionId sessionId = qryProc.createSession(propsHolder);

        return new SessionImpl(
                sessionId,
                qryProc,
                transactions,
                pageSize,
                propsHolder
        );
    }
}
