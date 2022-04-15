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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Async Session provides methods for asynchronous query execution.
 *
 * @see org.apache.ignite.sql.Session
 */
public interface AsyncSession {
    /**
     * Executes SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, String query,
            @Nullable Object... arguments);

    /**
     * Executes SQL statement in an asynchronous way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param statement SQL statement to execute.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<AsyncResultSet> executeAsync(@Nullable Transaction transaction, Statement statement);

    /**
     * Executes batched SQL query in an asynchronous way.
     *
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @param query SQL query template.
     * @param batch List of batch rows, where each row is a list of statement arguments.
     * @return Operation future.
     * @throws SqlException If failed.
     */
    CompletableFuture<Integer> executeBatchAsync(
            @Nullable Transaction transaction,
            String query,
            BatchedArguments batch
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
    CompletableFuture<Integer> executeBatchAsync(
            @Nullable Transaction transaction,
            Statement statement,
            BatchedArguments batch
    );
}
