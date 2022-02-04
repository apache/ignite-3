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

package org.apache.ignite.sql.async;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Async Session provides methods for asynchronous query execution.
 */
public interface AsyncSession {
    /** Default maximal number of rows in a single page. */
    int DEFAULT_PAGE_SIZE = 1024;

    /**
     * Executes SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, @NotNull String query,
            @Nullable Object... arguments);

    /**
     * Executes SQL statement in an asynchronous way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, @NotNull Statement statement);

    /**
     * Executes batched SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param query SQL query template.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    @NotNull CompletableFuture<Integer> executeBatchAsync(
            @Nullable Transaction transaction,
            @NotNull String query,
            @NotNull List<List<@Nullable Object>> batch
    );

    /**
     * Executes batched SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    @NotNull CompletableFuture<Integer> executeBatchAsync(
            @Nullable Transaction transaction,
            @NotNull Statement statement,
            @NotNull List<List<@Nullable Object>> batch
    );

    /**
     * Sets default page size for asynchronous queries.
     *
     * @param pageSize Maximal number of rows in a page.
     * @return {@code this} for chaining.
     */
    AsyncSession pageSize(int pageSize);

    /**
     * Returns default page size for asynchronous queries.
     *
     * <p>Default value is {@link #DEFAULT_PAGE_SIZE}.
     *
     * @return Maximal number of rows in a page.
     */
    int pageSize();
}
