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

package org.apache.ignite.query.sql.async;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.query.sql.SQLException;
import org.apache.ignite.query.sql.SqlResultSet;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Async Session provides methods for asynchronous query execution.
 */
public interface AsyncSqlSession {
    /**
     * Executes SQL query in async way.
     *
     * @param sql SQL query template.
     * @param tx Transaction (optional).
     * @param args Arguments for template (optional).
     * @return Operation future.
     * @throws SQLException If failed.
     */
    CompletableFuture<SqlResultSet> executeQueryAsync(String sql, @Nullable Transaction tx, Object... args);

    /**
     * Executes DML SQL query in async way.
     *
     * @param sql SQL query template.
     * @param tx Transaction (optional).
     * @param args Arguments for template (optional).
     * @return Operation future.
     * @throws SQLException If failed.
     */
    CompletableFuture<Integer> executeUpdateAsync(String sql, @Nullable Transaction tx, Object... args);

    /**
     * Executes DDL SQL query in async way.
     *
     * @param sql SQL query template.
     * @param tx Transaction (optional).
     * @param args Arguments for template (optional).
     * @return Operation future.
     * @throws SQLException If failed.
     */
    CompletableFuture<Boolean> executeAsync(String sql, @Nullable Transaction tx, Object... args);
}
