/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * Object represents an SQL statement.
 *
 * <p>The statement object is thread-safe.
 *
 * <p>Statement parameters and query plan can be cached on the server side. The server-side resources are managed automatically.
 * If the server-side state does not exist due to any reason - the very first run, current client node reconnect, cache
 * invalidation, etc. - this state is restored automatically. This may cause the user to experience a slightly increased latency.
 */
public interface Statement {
    /**
     * Returns a string representation of an SQL statement.
     *
     * @return SQL statement string.
     */
    String query();

    /**
     * Returns a query timeout.
     *
     * @param timeUnit Timeunit to convert timeout to.
     * @return Query timeout in the given timeunit.
     */
    long queryTimeout(TimeUnit timeUnit);

    /**
     * Returns a statement's default schema.
     *
     * @return Default schema for the statement.
     */
    String defaultSchema();

    /**
     * Returns a page size - the maximum number of result rows that can be fetched at a time.
     *
     * @return Maximum number of rows per page.
     */
    int pageSize();

    /**
     * Returns time zone used for this statement.
     *
     * @return Time zone used for this statement.
     *
     * @see StatementBuilder#timeZoneId(ZoneId)
     */
    ZoneId timeZoneId();

    /**
     * Creates a statement builder from the current statement.
     *
     * @return Statement builder based on the current statement.
     */
    StatementBuilder toBuilder();

    /**
     * Statement builder provides methods for building a statement object, which represents a query and holds query-specific 
     * settings that overrides the session defaults.
     */
    interface StatementBuilder {
        /**
         * Sets an SQL statement string.
         *
         * @param query SQL query.
         * @return {@code this} for chaining.
         */
        StatementBuilder query(String query);

        /**
         * Sets a query timeout.
         *
         * @param timeout Query timeout value. Must be positive.
         * @param timeUnit Timeunit.
         * @return {@code this} for chaining.
         */
        StatementBuilder queryTimeout(long timeout, TimeUnit timeUnit);

        /**
         * Sets a default schema for the statement.
         *
         * @param schema Default schema.
         * @return {@code this} for chaining.
         */
        StatementBuilder defaultSchema(String schema);

        /**
         * Sets a page size - the maximum number of result rows that can be fetched at a time.
         *
         * @param pageSize Maximum number of rows per page. Must be positive.
         * @return {@code this} for chaining.
         */
        StatementBuilder pageSize(int pageSize);

        /**
         * Sets a time zone for this statement.
         *
         * <p>This time zone is used in the following cases:
         * <ol>
         *     <li>When using SQL functions to obtain the current time (for example {@code SELECT CURRENT_TIME})</li>
         *     <li>When converting a string literal to/from a TIMESTAMP WITH LOCAL TIME ZONE column
         *     (for example {@code SELECT TIMESTAMP WITH LOCAL TIME ZONE '1992-01-18 02:30:00.123'}</li>
         * </ol>
         *
         * <p>If the time zone has not been set explicitly, the current JVM default time zone will be used.
         *
         * @param timeZoneId Time-zone ID.
         * @return {@code this} for chaining.
         */
        StatementBuilder timeZoneId(ZoneId timeZoneId);

        /**
         * Creates an SQL statement abject.
         *
         * @return Statement.
         */
        Statement build();
    }
}
