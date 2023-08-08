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

import java.util.Objects;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * SQL Session provides methods for query execution.
 *
 * <p>Session is a stateful object that holds default settings for the new queries.
 * The session object is immutable and thread-safe.
 */
public interface AbstractSession extends Session {
    /** Default schema name. */
    String DEFAULT_SCHEMA = "PUBLIC";

    /** Default maximal number of rows in a single page. */
    int DEFAULT_PAGE_SIZE = 1024;

    /**
     * Executes a single SQL query.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return SQL query result set.
     * @throws SqlException If failed.
     */
    @Override
    default ResultSet<SqlRow> execute(@Nullable Transaction transaction, String query, @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, query, arguments).join());
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }

    /**
     * Executes a single SQL statement.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param arguments Arguments for the statement.
     * @return SQL query result set.
     */
    @Override
    default ResultSet<SqlRow> execute(@Nullable Transaction transaction, Statement statement, @Nullable Object... arguments) {
        Objects.requireNonNull(statement);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, statement, arguments).join());
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }

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
    @Override
    default <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            String query,
            @Nullable Object... arguments) {
        Objects.requireNonNull(query);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, mapper, query, arguments).join());
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }

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
    @Override
    default <T> ResultSet<T> execute(
            @Nullable Transaction transaction,
            @Nullable Mapper<T> mapper,
            Statement statement,
            @Nullable Object... arguments) {
        Objects.requireNonNull(statement);

        try {
            return new SyncResultSetAdapter<>(executeAsync(transaction, mapper, statement, arguments).join());
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }

    /**
     * Executes a batched SQL query. Only DML queries are supported.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param dmlQuery DML query template.
     * @param batch Batch of query arguments.
     * @return Number of rows affected by each query in the batch.
     * @throws SqlBatchException If the batch fails.
     */
    @Override
    default long[] executeBatch(@Nullable Transaction transaction, String dmlQuery, BatchedArguments batch) {
        try {
            return executeBatchAsync(transaction, dmlQuery, batch).join();
        } catch (CompletionException e) {
            throw ExceptionUtils.wrap(e);
        }
    }
}
