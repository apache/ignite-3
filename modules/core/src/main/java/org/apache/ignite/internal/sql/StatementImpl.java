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

package org.apache.ignite.internal.sql;

import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Statement;

/**
 * Statement.
 */
public class StatementImpl implements Statement {
    /** Default query timeout is not limited. */
    private static final long DEFAULT_QUERY_TIMEOUT = 0;

    /** Query. */
    private final String query;

    /** Default schema. */
    private final String defaultSchema;

    /** Query timeout. */
    private final long queryTimeoutMs;

    /** Page size. */
    private final int pageSize;

    /** Time-zone ID. */
    private final ZoneId timeZoneId;

    /**
     * Constructor.
     *
     * @param query Query.
     */
    public StatementImpl(String query) {
        this(query, null, null, null, null);
    }

    /**
     * Constructor.
     *
     * @param query Query.
     * @param defaultSchema Default schema.
     * @param queryTimeoutMs Query timeout.
     * @param pageSize Page size.
     * @param timeZoneId Time-zone ID.
     */
    StatementImpl(
            String query,
            String defaultSchema,
            Long queryTimeoutMs,
            Integer pageSize,
            ZoneId timeZoneId
    ) {
        this.query = Objects.requireNonNull(query, "Parameter 'query' cannot be null");
        this.defaultSchema = Objects.requireNonNullElse(defaultSchema, SqlCommon.DEFAULT_SCHEMA_NAME);
        this.queryTimeoutMs = Objects.requireNonNullElse(queryTimeoutMs, DEFAULT_QUERY_TIMEOUT);
        this.pageSize =  Objects.requireNonNullElse(pageSize, SqlCommon.DEFAULT_PAGE_SIZE);

        // If the user has not explicitly specified a time zone, then we use the system default value.
        this.timeZoneId = Objects.requireNonNullElse(timeZoneId, ZoneId.systemDefault());
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

        return timeUnit.convert(queryTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return defaultSchema;
    }

    /** {@inheritDoc} */
    @Override
    public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public ZoneId timeZoneId() {
        return timeZoneId;
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder toBuilder() {
        return new StatementBuilderImpl()
                .query(query)
                .defaultSchema(defaultSchema)
                .timeZoneId(timeZoneId)
                .pageSize(pageSize)
                .queryTimeout(queryTimeoutMs, TimeUnit.MILLISECONDS);
    }
}
