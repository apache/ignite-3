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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.async.AsyncSession;
import org.apache.ignite.sql.reactive.ReactiveSession;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * SQL Session provides methods for query execution.
 *
 * <p>Session is a stateful object and holds setting that intended to be used as defaults for the new queries. Session is thread-safe and
 * can be reused and shared between threads, which is normal for asynchronous and reactive flow, however, modifying session state from
 * different threads may lead to the race condition.
 */
public interface Session extends AsyncSession, ReactiveSession {
    /** Default schema name. */
    String DEFAULT_SCHEMA = "PUBLIC";

    /**
     * Sets default query timeout.
     *
     * @param timeout Query timeout value.
     * @param timeUnit Timeunit.
     */
    void defaultTimeout(long timeout, @NotNull TimeUnit timeUnit);

    /**
     * Return default query timeout.
     *
     * @param timeUnit Timeunit to convert timeout to.
     * @return Default query timeout in the given timeunit.
     */
    long defaultTimeout(@NotNull TimeUnit timeUnit);

    /**
     * Sets default schema for the session, which the queries will be executed with. The default schema is used for query planning to
     * resolve table names to their canonical names.
     *
     * @param schema Default schema.
     */
    void defaultSchema(@NotNull String schema);

    /**
     * Returns session default schema.
     *
     * <p>Default value is {@link #DEFAULT_SCHEMA}.
     *
     * @return Session default schema.
     * @see #defaultSchema(String)
     */
    @NotNull String defaultSchema();

    /**
     * Executes single SQL query.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return SQL query results set.
     * @throws SqlException If failed.
     */
    @NotNull ResultSet execute(@Nullable Transaction transaction, @NotNull String query, @Nullable Object... arguments);

    /**
     * Executes single SQL statement.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @return SQL query results set.
     */
    @NotNull ResultSet execute(@Nullable Transaction transaction, @NotNull Statement statement, @Nullable Object... arguments);

    /**
     * Executes batched SQL query.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param query SQL query template.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return SQL query results set.
     */
    @NotNull int[] executeBatch(@Nullable Transaction transaction, @NotNull String query, @NotNull List<List<@Nullable Object>> batch);

    /**
     * Executes batched SQL query.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return SQL query results set.
     */
    @NotNull int[] executeBatch(
            @Nullable Transaction transaction,
            @NotNull Statement statement,
            @NotNull List<List<@Nullable Object>> batch
    );

    /**
     * Executes multi-statement SQL query.
     *
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @throws SqlException If failed.
     */
    void executeScript(@NotNull String query, @Nullable Object... arguments);

    /**
     * Sets session property.
     *
     * @param name Property name.
     * @param value Property value.
     * @return {@code this} for chaining.
     */
    Session property(@NotNull String name, @Nullable Object value);

    /**
     * Returns session property.
     *
     * @param name Property name.
     * @return Property value.
     */
    @Nullable Object property(@NotNull String name);
}
