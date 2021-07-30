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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.query.sql.reactive.ReactiveSqlResultSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite SQL query facade.
 */
// TODO: Do we wand a separate IgniteQuery facade for non-sql (index/scan/full-text) queries?
public interface IgniteSql {
    ////////////// Query execution.
    /**
     * Shortcut method for running a query.
     *
     * @param sql SQL query template.
     * @param args Arguments for template (optional).
     * @return SQL query resultset.
     * @throws SQLException If failed.
     */
    SqlResultSet execute(@NotNull String sql, Object... args);

    /**
     * Shortcut method for running a query in asynchronously.
     *
     * @param sql SQL query template.
     * @param args Arguments for template (optional)
     * @return Query future.
     * @throws SQLException If failed.
     */
    CompletableFuture<SqlResultSet> executeAsync(String sql, Object... args);
    //TODO: May fut.cancel() cancels a query? If so, describe behavior in Javadoc.
    //TODO: Cassandra API offers pagination API here, for manual fetching control. Is their AsyncResultSet ever usefull?

    /**
     * Shortcut method for running a query in asynchronously.
     *
     * @param sql SQL query template.
     * @param args Arguments for template (optional)
     * @return Reactive result.
     * @throws SQLException If failed.
     */
    ReactiveSqlResultSet executeReactive(String sql, Object... args);

    /**
     * Shortcut method for running a non-query statement.
     *
     * @param sql SQL statement template.
     * @param args Agruments for template (optional).
     * @return Number of updated rows.
     */
    int executeNonQuery(@NotNull String sql, @Nullable Object... args);
    //TODO: useful for bulk DML query, when we don't care of results.
    //TODO: in contrary, execute() method may return inserted rows IDs that looks useful if AutoIncrement ID column is used.

    //TODO: same methods for Statement.

    /**
     * Sets query session parameter.
     *
     * @param name Parameter name.
     * @param value Parameter value.
     */
    void setParameter(String name, Object value);
    //TODO: User can set e.g. queryTimeout or force join order or whatever.
    //TODO: This is similar to SQL "SET" operator which is used in JDBC/ODBC clients for session state manipulation.


    //TODO: Move all of this to Session. Maybe facade instance could incapsulate a session implicitely?

    /**
     * Kills query by its' id.
     *
     * @param queryID Query id.
     */
    void killQuery(UUID queryID);

    /**
     * Returns statistics facade for table statistics management.
     *
     * Table statistics are used by SQL engine for SQL queries planning.
     *
     * @return Statistics facade.
     */
    IgniteTableStatistics statistics();
    // TODO: Do we need this here or move to Table facade?


    void registerUserFunction(Class type, String... methodNames); //TODO: Get function details from method annotations.
    void registerUserFunction(Class type);
    void unregistedUserFunction(String functionName);
    //TODO: Custom function registration. Do we need a view and unregister functionality?
}

