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

package org.apache.ignite.sql.reactive;

import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reactive Session provides methods for reactive query execution.
 */
public interface ReactiveSession {
    /**
     * Executes SQL query in reactive way.
     *
     * @param query       SQL query template.
     * @param transaction Transaction to execute the query within or {@code null}.
     * @param arguments   Arguments for the template (optional).
     * @return Reactive result.
     * @throws SqlException If failed.
     */
    ReactiveResultSet executeReactive(@NotNull String query, @Nullable Transaction transaction, Object... arguments);

    /**
     * Executes SQL query in reactive way.
     *
     * @param statement   SQL statement.
     * @param transaction Transaction to execute the statement within or {@code null}.
     * @return Reactive result.
     * @throws SqlException If failed.
     */
    ReactiveResultSet executeReactive(@NotNull Statement statement, @Nullable Transaction transaction);
}
