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
 */
public interface Session extends AsyncSession, ReactiveSession {
    /**
     * Sets default query timeout.
     *
     * @param timeout  Query timeout value.
     * @param timeUnit Timeunit.
     */
    void defaultTimeout(long timeout, TimeUnit timeUnit);

    /**
     * Gets default query timeout.
     *
     * @param timeUnit Timeunit.
     * @return Default query timeout.
     */
    long defaultTimeout(TimeUnit timeUnit);

    /**
     * Sets default query schema.
     *
     * @param schema Default schema.
     */
    void defaultSchema(@NotNull String schema);

    /**
     * Gets default query schema.
     *
     * @return Default query schema.
     */
    String defaultSchema();

    /**
     * Executes single SQL query.
     *
     * @param query       SQL query template.
     * @param arguments   Arguments for the template (opti
     * @param transaction Transaction to execute the query within or {@code null}.onal).
     * @return SQL query results set.
     * @throws SqlException If failed.
     */
    ResultSet execute(@NotNull String query, @Nullable Transaction transaction, Object... arguments);

    /**
     * Executes single SQL statement.
     *
     * @param statement   SQL statement to execute.
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @return SQL query results set.
     */
    ResultSet execute(@NotNull Statement statement, @Nullable Transaction transaction);

    /**
     * Executes multi-statement non-transactional SQL query.
     *
     * @param query     SQL query template.
     * @param arguments Arguments for the template (optional).
     * @throws SqlException If failed.
     */
    void executeScript(@NotNull String query, Object... arguments);

    /**
     * Sets session property.
     *
     * @param name  Property name.
     * @param value Property value.
     * @return {@code this} for chaining.
     */
    Session property(@NotNull String name, Object value);
}
