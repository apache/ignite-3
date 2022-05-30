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
    /** */
    private final String query;

    /** */
    private final String defaultSchema;

    /** */
    private final boolean prepared;

    /** */
    private final long queryTimeoutMs;

    /** */
    private final int pageSize;

    /** */
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
            long queryTimeoutMs,
            int pageSize,
            Map<String, Object> properties) {
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
    public boolean prepared() {
        return prepared;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(@NotNull String name) {
        return properties.get(name);
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder toBuilder() {
        var builder = new ClientStatementBuilder()
                .query(query)
                .defaultSchema(defaultSchema)
                .prepared(prepared)
                .queryTimeout(queryTimeoutMs, TimeUnit.MILLISECONDS)
                .pageSize(pageSize);

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
