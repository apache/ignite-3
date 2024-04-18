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
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite SQL query facade.
 */
public interface IgniteSql {
    /**
     * Creates an SQL statement from a given query string.
     *
     * @param query SQL query string.
     * @return A new statement.
     */
    Statement createStatement(String query);

    /**
     * Creates an SQL statement builder, which provides query-specific settings. 
     * These settings override the session defaults when the statement is executed.
     *
     * @return A new statement builder.
     */
    Statement.StatementBuilder statementBuilder();

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

}
