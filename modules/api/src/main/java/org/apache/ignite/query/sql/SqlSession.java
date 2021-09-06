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

package org.apache.ignite.query.sql;

import org.apache.ignite.query.sql.async.AsyncSqlSession;
import org.apache.ignite.query.sql.reactive.ReactiveSqlSession;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;

/**
 * SQL Session provides methods for query execution.
 */
public interface SqlSession extends AsyncSqlSession, ReactiveSqlSession {
    /**
     * Creates transactional SQL session projection with given transaction.
     *
     * @param tx Transaction.
     * @return Transactional projection for the session.
     */
    SqlTx withTransaction(@NotNull Transaction tx);

    /**
     * Creates transactional SQL session projection with a new transaction.
     *
     * @return Transactional projection for the session.
     */
    SqlTx withNewTransaction();

    /**
     * Sets default query timeout.
     * TODO: add getters.
     *
     * @return {@code this} for chaining.
     */
    SqlSession setTimeout(int timeout);

    /**
     * Sets default query schema.
     *
     * @param schema Default schema.
     * @return {@code this} for chaining.
     */
    SqlSession setSchema(@NotNull String schema);

    /**
     * Executes single SQL query.
     *
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return SQL query results set.
     * @throws SQLException If failed.
     */
    SqlResultSet execute(@NotNull String query, Object... arguments);

    /**
     * Executes single SQL statement.
     *
     * @param statement SQL statement to execute.
     * @return SQL query results set.
     */
    SqlResultSet execute(@NotNull SqlStatement statement);

    /**
     * Executes multi-statement SQL query.
     *
     * @param query SQL query template.
     * @param arguments Arguments for the template (optional).
     * @return SQL query results set.
     * @throws SQLException If failed.
     */
    SqlMultiResultSet executeMulti(@NotNull String query, Object... arguments);

    /**
     * Sets query session parameter.
     *
     * @param name Parameter name.
     * @param value Parameter value.
     * @return {@code this} for chaining.
     */
    SqlSession setParameter(@NotNull String name, Object value);
    //TODO: User can set e.g. queryTimeout or force join order or whatever.
    //TODO: This is similar to SQL "SET" operator which is used in JDBC/ODBC clients for session state manipulation.
}
