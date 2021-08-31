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
import org.jetbrains.annotations.Nullable;

/**
 * SQL Session provides methods for query execution.
 */
public interface SqlSession extends AsyncSqlSession, ReactiveSqlSession {
    /** */
    SqlSession withTransaction(@Nullable Transaction tx);

    /** */
    SqlSession setTimeout(@NotNull int timeout);

    /** */
    SqlSession setQuota(@NotNull Object quota);

    /** */
    SqlSession setSchema(@NotNull String schema);

    /**
     * Executes SQL query.
     *
     * @param sql SQL query template.
     * @param args Arguments for template (optional).
     * @return SQL query results set.
     * @throws SQLException If failed.
     */
    SqlResultSet execute(@NotNull String sql, Object... args);

    /** */
    SqlResultSet execute(@NotNull SqlPrepared prep);

    /**
     * Executes SQL query.
     *
     * @param sql SQL query template.
     * @param args Arguments for template (optional).
     * @return SQL query results set.
     * @throws SQLException If failed.
     */
    SqlMultiResultSet executeMulti(@NotNull String sql, Object... args);

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
