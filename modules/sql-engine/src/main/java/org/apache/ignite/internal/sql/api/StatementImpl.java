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

import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Statement;

/**
 * Statement.
 */
class StatementImpl implements Statement {
    /** Query. */
    private final String query;

    /** Default schema. */
    private final String defaultSchema;

    /** Query timeout. */
    private final Long queryTimeoutMs;

    /** Page size. */
    private final Integer pageSize;

    /** Time zone. */
    private final ZoneId timeZone;

    /**
     * Constructor.
     *
     * @param query Query.
     */
    StatementImpl(String query) {
        this(query, null, null, null, null);
    }

    /**
     * Constructor.
     *
     * @param query Query.
     * @param defaultSchema Default schema.
     * @param queryTimeoutMs Query timeout.
     * @param pageSize Page size.
     * @param timeZone Time zone.
     */
    StatementImpl(
            String query,
            String defaultSchema,
            Long queryTimeoutMs,
            Integer pageSize,
            ZoneId timeZone
    ) {
        this.query = Objects.requireNonNull(query, "Parameter 'query' cannot be null");
        this.defaultSchema = defaultSchema;
        this.queryTimeoutMs = queryTimeoutMs;
        this.pageSize = pageSize;
        this.timeZone = timeZone;
    }

    /** {@inheritDoc} */
    @Override
    public String query() {
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public long queryTimeout(TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return queryTimeoutMs == null ? 0 : timeUnit.convert(queryTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return defaultSchema;
    }

    /** {@inheritDoc} */
    @Override
    public int pageSize() {
        return pageSize == null ? 0 : pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public ZoneId timeZone() {
        return timeZone;
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder toBuilder() {
        var builder = new StatementBuilderImpl()
                .query(query)
                .defaultSchema(defaultSchema);

        if (timeZone != null) {
            builder.timeZone(timeZone);
        }

        if (pageSize != null) {
            builder.pageSize(pageSize);
        }

        if (queryTimeoutMs != null) {
            builder.queryTimeout(queryTimeoutMs, TimeUnit.MILLISECONDS);
        }

        return builder;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        // No-op.
    }
}
