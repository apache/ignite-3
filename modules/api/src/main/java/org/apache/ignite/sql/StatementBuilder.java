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

package org.apache.ignite.sql;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Statement builder.
 */
public interface StatementBuilder {
    /**
     * Set SQL statement string.
     */
    StatementBuilder query(String sql);

    /**
     * Marks current statement as prepared.
     */
    StatementBuilder prepared();

    /**
     * Sets query timeout.
     *
     * @param timeout Query timeout value.
     * @param timeUnit Timeunit.
     */
    StatementBuilder withQueryTimeout(long timeout, @NotNull TimeUnit timeUnit);

    /**
     * Sets default schema for the statement, which the queries will be executed with.
     *
     * @param schema Default schema.
     */
    StatementBuilder withDefaultSchema(@NotNull String schema);

    /**
     * Sets page size, which is a maximal amount of results rows that can be fetched once at a time.
     *
     * @param pageSize Maximal amount of rows in a page.
     * @return {@code this} for chaining.
     */
    StatementBuilder withPageSize(int pageSize);

    /**
     * Sets statement property value that overrides the session property value. If {@code null} is passed, then a session property value
     * will be used.
     *
     * @param name Property name.
     * @param value Property value or {@code null} to a session property value.
     * @return {@code this} for chaining.
     */
    StatementBuilder withProperty(@NotNull String name, @Nullable Object value);

    /**
     * Creates an SQL statement abject, which represents a query and holds a query-specific settings that overrides the session default
     * settings.
     *
     * @return Statement.
     */
    Statement build();
}
