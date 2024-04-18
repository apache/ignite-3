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

package org.apache.ignite.internal.client.sql;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Statement;

/**
 * Client SQL statement.
 */
public class ClientStatement implements Statement {
    /** Query. */
    private final String query;

    /** Default schema. */
    private final String defaultSchema;

    /** Query timeout. */
    private final Long queryTimeoutMs;

    /** Page size. */
    private final Integer pageSize;

    /**
     * Constructor.
     *
     * @param query Query.
     * @param defaultSchema Default schema.
     * @param queryTimeoutMs Timeout
     * @param pageSize Page size.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ClientStatement(
            String query,
            String defaultSchema,
            Long queryTimeoutMs,
            Integer pageSize) {
        Objects.requireNonNull(query);

        this.query = query;
        this.defaultSchema = defaultSchema;
        this.queryTimeoutMs = queryTimeoutMs;
        this.pageSize = pageSize;
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

    /**
     * Gets the query timeout as nullable.
     *
     * @return Query timeout.
     */
    public Long queryTimeoutNullable() {
        return queryTimeoutMs;
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return defaultSchema;
    }

    /** {@inheritDoc} */
    @Override
    public ZoneId timeZone() {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-21568
        return ZoneOffset.UTC;
    }

    /** {@inheritDoc} */
    @Override
    public int pageSize() {
        return pageSize == null ? 0 : pageSize;
    }

    /**
     * Gets the page size as nullable.
     *
     * @return Page size.
     */
    public Integer pageSizeNullable() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder toBuilder() {
        var builder = new ClientStatementBuilder()
                .query(query)
                .defaultSchema(defaultSchema);

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
