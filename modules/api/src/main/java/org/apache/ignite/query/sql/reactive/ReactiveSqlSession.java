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

package org.apache.ignite.query.sql.reactive;

import java.util.UUID;
import java.util.concurrent.Flow;
import org.apache.ignite.query.sql.SQLException;
import org.apache.ignite.query.sql.SqlStatement;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Reactive Session provides methods for reactive query execution.
 */
public interface ReactiveSqlSession {
    /**
     * Executes SQL query in reactive way.
     *
     * @param sql SQL query template.
     * @param args Arguments for the template (optional).
     * @return Reactive result.
     * @throws SQLException If failed.
     */
    ReactiveSqlResultSet executeReactive(String sql, Object... args);

    /**
     * Executes SQL query in reactive way.
     *
     * @param statement SQL statement.
     * @return Reactive result.
     * @throws SQLException If failed.
     */
    ReactiveSqlResultSet executeReactive(SqlStatement statement);

    /**
     * Executes multi-statement SQL query in reactive way.
     *
     * @param sql SQL query template.
     * @param args Arguments for the template (optional).
     * @return Reactive result.
     * @throws SQLException If failed.
     */
    ReactiveSqlMultiResultSet executeMultiStatementReactive(String sql, Object... args);

    interface ReactiveSqlMultiResultSet extends Flow.Publisher<ReactiveSqlResultSet> {
        /**
         * Returns query`s unique identifier.
         *
         * @return Query id.
         */
        Flow.Publisher<UUID> queryId();
    }
}
