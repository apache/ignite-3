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

package org.apache.ignite.sql;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.sql.reactive.ReactiveResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * SQL Session provides methods for query execution.
 *
 * <p>Session is a stateful object that holds default settings for the new queries.
 * The session object is immutable and thread-safe.
 */
public interface Session extends AutoCloseable {
    /**
     * Executes a single SQL query.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return SQL query result set.
     * @throws SqlException If failed.
     */
    ResultSet<SqlRow> execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments);

    /**
     * Executes a single SQL statement.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @return SQL query result set.
     */
    ResultSet<SqlRow> execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments);

    /**
     * Executes single SQL statement and maps results to objects with the provided mapper.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param mapper Mapper that defines the row type and the way to map columns to the type members. See {@link Mapper#of}.
     * @param query SQL query template.
     * @param arguments Arguments for the statement.
     * @param <T> A type of object contained in result set.
     * @return SQL query results set.
     */
    <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments);

    /**
     * Executes single SQL statement and maps results to objects with the provided mapper.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param mapper Mapper that defines the row type and the way to map columns to the type members. See {@link Mapper#of}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @param <T> A type of object contained in result set.
     * @return SQL query results set.
     */
    <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments);

    /**
     * Executes SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(@Nullable Transaction transaction, String query, @Nullable Object... arguments);

    /**
     * Executes an SQL statement asynchronously.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<AsyncResultSet<SqlRow>> executeAsync(
            @Nullable Transaction transaction,
            Statement statement,
            @Nullable Object... arguments);

    /**
     * Executes SQL statement in an asynchronous way and maps results to objects with the provided mapper.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param mapper Mapper that defines the row type and the way to map columns to the type members. See {@link Mapper#of}.
     * @param query SQL query template.
     * @param arguments Arguments for the statement.
     * @param <T> A type of object contained in result set.
     * @return Operation future.
     */
    <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments);

    /**
     * Executes SQL statement in an asynchronous way and maps results to objects with the provided mapper.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param mapper Mapper that defines the row type and the way to map columns to the type members. See {@link Mapper#of}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @param <T> A type of object contained in result set.
     * @return Operation future.
     */
    <T> CompletableFuture<AsyncResultSet<T>> executeAsync(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments);

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
     * Executes an SQL statement reactively.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement.
     * @param arguments Arguments for the statement.
     * @return Reactive result.
     * @throws SqlException If failed.
     */
    ReactiveResultSet executeReactive(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments);

    /**
     * Executes a batched SQL query. Only DML queries are supported.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param dmlQuery DML query template.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     * @throws SqlBatchException If the batch fails.
     */
    long[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch);

    /**
     * Executes a batched SQL statement. Only DML queries are supported.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param dmlStatement DML statement to execute.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     * @throws SqlBatchException If the batch fails.
     */
    long[] executeBatch(@Nullable Transaction transaction, Statement dmlStatement, BatchedArguments batch);

    /**
     * Executes a batched SQL query asynchronously.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Operation Future completed with the number of rows affected by each query in the batch 
     *         (if the batch succeeds), future completed with the {@link SqlBatchException} (if the batch fails).
     */
    CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, String query, BatchedArguments batch);

    /**
     * Executes a batched SQL statement asynchronously.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Operation Future completed with the number of rows affected by each query in the batch 
     *         (if the batch succeeds), future completed with the {@link SqlBatchException} (if the batch fails).
     */
    CompletableFuture<long[]> executeBatchAsync(@Nullable Transaction transaction, Statement statement, BatchedArguments batch);

    /**
     * Executes a batched SQL query reactively.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Publisher for the number of rows affected by the query.
     * @throws SqlException If failed.
     */
    Flow.Publisher<Long> executeBatchReactive(@Nullable Transaction transaction, String query, BatchedArguments batch);

    /**
     * Executes a batched SQL statement reactively.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Publisher for the number of rows affected by the query.
     * @throws SqlException If failed.
     */
    Flow.Publisher<Long> executeBatchReactive(@Nullable Transaction transaction, Statement statement, BatchedArguments batch);

    /**
     * Executes a multi-statement SQL query.
     *
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @throws SqlException If failed.
     */
    void executeScript(String query, @Nullable Object... arguments);

    /**
     * Executes a multi-statement SQL query.
     *
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<Void> executeScriptAsync(String query, @Nullable Object... arguments);

    /**
     * Returns a default query timeout. If a query takes more time than specified, it will be interrupted.
     *
     * @param timeUnit Timeunit to convert timeout to.
     * @return Default query timeout in the given timeunit.
     */
    long defaultQueryTimeout(TimeUnit timeUnit);

    /**
     * Return default idle session timeout.
     *
     * <p>The maximum idle time (that is, time when no requests are performed on behalf the session) in milliseconds, after which this
     * session will be considered expired.
     *
     * @param timeUnit Timeunit to convert timeout to.
     * @return Session timeout in the given timeunit.
     */
    long idleTimeout(TimeUnit timeUnit);

    /**
     * Returns a session default schema.
     *
     * @return Session default schema.
     */
    String defaultSchema();

    /**
     * Returns a default page size, which is a maximum number of result rows that can be fetched at a time.
     *
     * @return Maximum number of rows per page.
     */
    int defaultPageSize();

    /**
     * Returns a session property.
     *
     * @param name Property name.
     * @return Property value or {@code null} if wasn't set.
     */
    @Nullable Object property(String name);

    /**
     * Invalidates a session, cleans up remote session resources, and stops all queries that are running within the current session.
     */
    @Override
    void close();

    /**
     * Invalidates a session, cleans up remote session resources, and stops all queries that are running within the current session.
     *
     * @return Operation future.
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Invalidates a session, cleans up remote session resources, and stops all queries that are running within the current session.
     *
     * @return Publisher.
     */
    Flow.Publisher<Void> closeReactive();

    /**
     * Creates a new session builder from the current session.
     *
     * @return Session builder based on the current session.
     */
    SessionBuilder toBuilder();

    /**
     * Session builder.
     */
    interface SessionBuilder {
        /**
         * Returns ignite transaction.
         *
         * @return Ignite transaction.
         */
        IgniteTransactions igniteTransactions();

        /**
         * Sets ignite transactions.
         *
         * @param transactions Ignite transactions.
         * @return {@code this} for chaining.
         */
        SessionBuilder igniteTransactions(IgniteTransactions transactions);

        /**
         * Returns a default query timeout.
         *
         * @param timeUnit Timeunit to convert timeout to.
         * @return Default query timeout in the given timeunit.
         */
        long defaultQueryTimeout(TimeUnit timeUnit);

        /**
         * Sets a default query timeout.
         *
         * @param timeout Query timeout value.
         * @param timeUnit Timeunit.
         * @return {@code this} for chaining.
         */
        SessionBuilder defaultQueryTimeout(long timeout, TimeUnit timeUnit);

        /**
         * Return the idle timeout.
         *
         * <p>The maximum idle time (that is, time when no requests are performed on behalf the session) in milliseconds, after which this
         * session will be considered expired.
         *
         * @param timeUnit Timeunit to convert timeout to.
         * @return Session timeout in the given timeunit.
         */
        long idleTimeout(TimeUnit timeUnit);

        /**
         * Sets idle timeout.
         *
         * <p>The maximum idle time (that is, time when no requests are performed on behalf the session) in milliseconds, after which this
         * session will be considered expired.
         *
         * @param timeout Session timeout value.
         * @param timeUnit Timeunit.
         * @return {@code this} for chaining.
         */
        SessionBuilder idleTimeout(long timeout, TimeUnit timeUnit);

        /**
         * Returns a session default schema.
         *
         * @return Session default schema.
         */
        String defaultSchema();

        /**
         * Sets a default schema for the queries to be executed within a session.
         *
         * <p>Default schema is used to resolve schema objects by their simple names or,
         * if the schema is not specified in the query text, by their canonical names.
         *
         * @param schema Default schema.
         * @return {@code this} for chaining.
         */
        SessionBuilder defaultSchema(String schema);

        /**
         * Returns a default page size, which is the maximum number of result rows that can be fetched at a time.
         *
         * @return Maximum number of rows per page.
         */
        int defaultPageSize();

        /**
         * Sets a default page size, which is the maximum number of result rows that can be fetched at a time.
         *
         * @param pageSize Maximum number of rows per page.
         * @return {@code this} for chaining.
         */
        SessionBuilder defaultPageSize(int pageSize);

        /**
         * Returns a session property.
         *
         * @param name Property name.
         * @return Property value or {@code null} if wasn't set.
         */
        @Nullable Object property(String name);

        /**
         * Sets a session property.
         *
         * @param name Property name.
         * @param value Property value.
         * @return {@code this} for chaining.
         */
        SessionBuilder property(String name, @Nullable Object value);

        /**
         * Creates an SQL session object that provides methods for executing SQL queries and holds query execution settings.
         *
         * @return Session.
         */
        Session build();
    }
}
