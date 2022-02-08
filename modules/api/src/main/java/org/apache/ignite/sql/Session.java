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
import org.apache.ignite.sql.async.AsyncSession;
import org.apache.ignite.sql.reactive.ReactiveSession;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * SQL Session provides methods for query execution.
 *
 * <p>Session is a stateful object and holds setting that intended to be used as defaults for the new queries. Modifying the session state
 * may affect the queries that are already started within this session. Thus, modifying session state from concurrent threads may lead to
 * unexpected behaviour.
 *
 * <p>Session "execute*" methods are thread-safe and can be called from different threads.
 *
 * <p>Statement created via a current session can't be used in different sessions. Prepared statement forces performance optimizations,
 * such as query plan caching on the server side, which is useful for frequently executed queries or for queries when low-latency is
 * critical. However, prepared statement execution flow may switch to a normal flow for short time automatically, when the server side state
 * is lost and has to be recovered (e.g. due to client reconnect, cluster reconfiguration, or any other).
 */
public interface Session extends AsyncSession, ReactiveSession, AutoCloseable {
    /** Default schema name. */
    String DEFAULT_SCHEMA = "PUBLIC";

    /**
     * Creates an SQL statement abject, which represents a query and holds a query-specific settings that overrides the session default
     * settings.
     *
     * @param query SQL query template.
     * @return A new statement.
     */
    Statement createStatement(@NotNull String query);

    /**
     * Creates an SQL statement abject, which represents a prepared query and holds a query-specific settings that overrides the session
     * default settings.
     *
     * @param query SQL query template.
     * @return A new statement.
     * @throws SqlException If parsing failed.
     */
    Statement createPreparedStatement(String query);

    /**
     * Create a prepared copy of given statement.
     *
     * @param statement SQL query statement.
     * @return A new statement.
     * @throws SqlException If parsing failed.
     */
    Statement prepare(Statement statement);

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
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     */
    @NotNull int[] executeBatch(@Nullable Transaction transaction, @NotNull String query, @NotNull Arguments batch);

    /**
     * Executes batched SQL query.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     */
    @NotNull int[] executeBatch(
            @Nullable Transaction transaction,
            @NotNull Statement statement,
            @NotNull Arguments batch
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
     * Closes session, cleanup remote session resources, and stops all queries that are running within the current session.
     */
    @Override
    void close();

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
     * Sets default schema for the session, which the queries will be executed with.
     *
     * <p>Default schema is used to resolve schema objects by their simple names, those for which schema is not specified in the query
     * text, to their canonical names.
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
