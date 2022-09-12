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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client SQL statement builder.
 */
public class ClientStatementBuilder implements Statement.StatementBuilder {
    /** Properties. */
    private final Map<String, Object> properties = new HashMap<>();

    /** Query. */
    private String query;

    /** Default schema. */
    private String defaultSchema;

    /** Query timeout. */
    private Long queryTimeoutMs;

    /** Page size. */
    private Integer pageSize;

    /** {@inheritDoc} */
    @Override
    public @NotNull String query() {
        return query;
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder query(String sql) {
        query = sql;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public long queryTimeout(@NotNull TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        return timeUnit.convert(queryTimeoutMs == null ? 0 : queryTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder queryTimeout(long timeout, @NotNull TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit);

        queryTimeoutMs = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public String defaultSchema() {
        return defaultSchema;
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder defaultSchema(@NotNull String schema) {
        defaultSchema = schema;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public int pageSize() {
        return pageSize == null ? 0 : pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder pageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object property(@NotNull String name) {
        return properties.get(name);
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder property(@NotNull String name, @Nullable Object value) {
        properties.put(name, value);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public Statement build() {
        return new ClientStatement(
                query,
                defaultSchema,
                queryTimeoutMs,
                pageSize,
                new HashMap<>(properties));
    }
}
