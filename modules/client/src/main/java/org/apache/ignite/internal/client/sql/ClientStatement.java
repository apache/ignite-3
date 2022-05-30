/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Statement;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL statement.
 */
class ClientStatement implements Statement {
    /** Query. */
    private final String query;

    /** Default schema. */
    private final String defaultSchema;

    /** Prepared flag. */
    private final boolean prepared;

    /** Query timeout. */
    private final Long queryTimeoutMs;

    /** Page size. */
    private final Integer pageSize;

    /** Properties. */
    private final Map<String, Object> properties;

    /**
     * Constructor.
     *
     * @param query Query.
     * @param defaultSchema Default schema.
     * @param prepared Prepared.
     * @param queryTimeoutMs Timeout
     * @param pageSize Page size.
     * @param properties Properties.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ClientStatement(
            String query,
            String defaultSchema,
            boolean prepared,
            Long queryTimeoutMs,
            Integer pageSize,
            Map<String, Object> properties) {
        Objects.requireNonNull(query);

        this.query = query;
        this.defaultSchema = defaultSchema;
        this.prepared = prepared;
        this.queryTimeoutMs = queryTimeoutMs;
        this.pageSize = pageSize;
        this.properties = properties;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String query() {
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public long queryTimeout(@NotNull TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return timeUnit.convert(queryTimeoutMs, TimeUnit.MILLISECONDS);
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
    public int pageSize() {
        return pageSize;
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
    public boolean prepared() {
        return prepared;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(@NotNull String name) {
        return properties.get(name);
    }

    /**
     * Gets the properties map.
     *
     * @return Properties.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<String, Object> properties() {
        return properties;
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder toBuilder() {
        var builder = new ClientStatementBuilder()
                .query(query)
                .defaultSchema(defaultSchema)
                .prepared(prepared);

        if (pageSize != null) {
            builder.pageSize(pageSize);
        }

        if (queryTimeoutMs != null) {
            builder.queryTimeout(queryTimeoutMs, TimeUnit.MILLISECONDS);
        }

        if (properties != null) {
            for (var entry : properties.entrySet()) {
                builder.property(entry.getKey(), entry.getValue());
            }
        }

        return builder;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        // No-op.
    }
}
