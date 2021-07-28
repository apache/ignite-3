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

import java.util.Collection;
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
     * @param args Arguments for template (optional)
     * @return SQL query resultset.
     * @throws SQLException If failed.
     */
    SqlResultSet execute(@NotNull String sql, @Nullable Object... args);

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

    //TODO: Do we need a separate methods for DML/DDL that do not return the resultset? or extend a resultset?
    //TODO: same methods for Statement.

    ///////////////// Query monitoring and management.
    /**
     * Returns SQL views.
     */
    Object views(); //TODO: To be described.
    //TODO: Add view for runnin queries.

    /**
     * Return info of the queries. //TODO: "running on the node locally"? or "started on the node"? or both?
     *
     * @return Running queries infos.
     */
    Collection<Object> runningQueries(); //TODO: Is it needed here or to be moved into the Views facade?

    /**
     * Kills query by its' id.
     *
     * @param queryID Query id.
     */
    void killQuery(UUID queryID);


    ////////////// Statistics management.
    Collection<Object> tableStatistics(String table); //TODO: Local or global? Ready or in-progress? TBD.

    CompletableFuture<Void> gatherStatistics(String table, Object statisticsConfiguration);
    //TODO: Creates new statistics in addition, or drop old and replaces with new?
    //TODO: Should the existed one be refreshed?

    CompletableFuture<Void> refreshStatistics(String table, @Nullable String... optionalStatisticName);

    void dropStatistics(String table, @Nullable String... optionalStatisticName);

    void refreshLocalStatistics(String table, @Nullable String... optionalStatisticName); //TODO: Actually, drops local statistics to be automatically refreshed.


    //////////////////// DDL

    //TODO: Do we expect any SQL DDL pragrammatical API here? Why we have tables().create(), but not here?
    //TODO: alterTable()?
}

