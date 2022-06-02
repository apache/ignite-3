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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * SQL Session provides methods for query execution.
 *
 * <p>Session is a stateful object and holds setting that intended to be used as defaults for the new queries.
 * Session object is immutable and thread-safe.
 */
public interface Session extends AutoCloseable {
    /** Default schema name. */
    String DEFAULT_SCHEMA = "PUBLIC";

    /** Default maximal number of rows in a single page. */
    int DEFAULT_PAGE_SIZE = 1024;

    /**
     * Executes single SQL query.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return SQL query results set.
     * @throws SqlException If failed.
     */
    ResultSet execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments);

    /**
     * Executes single SQL statement.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @return SQL query results set.
     */
    ResultSet execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments);

    /**
     * Executes SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, String query, @Nullable Object... arguments);

    /**
     * Executes SQL statement in an asynchronous way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments);

    /**
     * Executes SQL query in a reactive way.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return Reactive result.
     * @throws SqlException If failed.
     */
    ReactiveResultSet executeReactive(@Nullable Transaction transaction, String query, @Nullable Object... arguments);

    /**
     * Executes SQL query in a reactive way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement.
     * @param arguments Arguments for the statement.
     * @return Reactive result.
     * @throws SqlException If failed.
     */
    ReactiveResultSet executeReactive(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments);

    /**
     * Executes batched SQL query. Only DML queries are supported.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param dmlQuery DML query template.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     */
    int[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch);

    /**
     * Executes batched SQL query. Only DML queries are supported.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param dmlStatement DML statement to execute.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     */
    int[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch);

    /**
     * Executes batched SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param query SQL query template.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<int[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch);

    /**
     * Executes batched SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<int[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch);

    /**
     * Executes batched SQL query in a reactive way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param query SQL query template.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Publisher for the number of rows affected by the query.
     * @throws SqlException If failed.
     */
    Flow.Publisher<Integer> executeBatchReactive(@Nullable Transaction transaction, String query, BatchedArguments batch);

    /**
     * Executes batched SQL query in a reactive way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Publisher for the number of rows affected by the query.
     * @throws SqlException If failed.
     */
    Flow.Publisher<Integer> executeBatchReactive(@Nullable Transaction transaction, Statement statement, BatchedArguments batch);

    /**
     * Executes multi-statement SQL query.
     *
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @throws SqlException If failed.
     */
    void executeScript(String query, @Nullable Object... arguments);

    /**
     * Executes multi-statement SQL query.
     *
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments);

    /**
     * Return default query timeout.
     *
     * @param timeUnit Timeunit to convert timeout to.
     * @return Default query timeout in the given timeunit.
     */
    long defaultTimeout(TimeUnit timeUnit);

    /**
     * Returns session default schema.
     *
     * @return Session default schema.
     */
    String defaultSchema();

    /**
     * Returns default page size, which is a maximal amount of results rows that can be fetched once at a time.
     *
     * @return Maximal amount of rows in a page.
     */
    int defaultPageSize();

    /**
     * Returns session property.
     *
     * @param name Property name.
     * @return Property value or {@code null} if wasn't set.
     */
    @Nullable Object property(String name);

    /**
     * Invalidates session, cleans up remote session resources, and stops all queries that are running within the current session.
     */
    @Override
    void close();

    /**
     * Invalidates session, cleans up remote session resources, and stops all queries that are running within the current session.
     *
     * @return Operation future.
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Invalidates session, cleans up remote session resources, and stops all queries that are running within the current session.
     *
     * @return Publisher.
     */
    Flow.Publisher<Void> closeReactive();

    /**
     * Creates a new session builder from current session.
     *
     * @return Session builder based on the current session.
     */
    SessionBuilder toBuilder();

    /**
     * Session builder.
     */
    interface SessionBuilder {
        /**
         * Return default query timeout.
         *
         * @param timeUnit Timeunit to convert timeout to.
         * @return Default query timeout in the given timeunit.
         */
        long defaultTimeout(TimeUnit timeUnit);

        /**
         * Sets default query timeout.
         *
         * @param timeout Query timeout value.
         * @param timeUnit Timeunit.
         * @return {@code this} for chaining.
         */
        SessionBuilder defaultTimeout(long timeout, TimeUnit timeUnit);

        /**
         * Returns session default schema.
         *
         * @return Session default schema.
         */
        String defaultSchema();

        /**
         * Sets default schema for the session, which the queries will be executed with.
         *
         * <p>Default schema is used to resolve schema objects by their simple names, those for which schema is not specified in the query
         * text, to their canonical names.
         *
         * @param schema Default schema.
         * @return {@code this} for chaining.
         */
        SessionBuilder defaultSchema(String schema);

        /**
         * Returns default page size, which is a maximal amount of results rows that can be fetched once at a time.
         *
         * @return Maximal amount of rows in a page.
         */
        int defaultPageSize();

        /**
         * Sets default page size, which is a maximal amount of results rows that can be fetched once at a time.
         *
         * @param pageSize Maximal amount of rows in a page.
         * @return {@code this} for chaining.
         */
        SessionBuilder defaultPageSize(int pageSize);

        /**
         * Returns session property.
         *
         * @param name Property name.
         * @return Property value or {@code null} if wasn't set.
         */
        @Nullable Object property(String name);

        /**
         * Sets session property.
         *
         * @param name Property name.
         * @param value Property value.
         * @return {@code this} for chaining.
         */
        SessionBuilder property(String name, @Nullable Object value);

        /**
         * Creates an SQL session object that provides methods for executing SQL queries and holds settings with which queries will be
         * executed.
         *
         * @return Session.
         */
        Session build();
    }
}
