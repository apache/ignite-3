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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The object represents SQL statement.
 *
 * <p>Statement object is thread-safe.
 *
 * <p>Statement parameters and query plan can be cached on the server-side. The server-side resources are managed automatically.
 * Basically, if the server-side state is not exists due to any reason: very first run, current client node reconnect or cache invalidation
 * or any other - then it will be restored automatically as if statement is run at the first time. So, sometimes the user may experience a
 * slightly increased latency.
 */
public interface Statement extends AutoCloseable {
    /**
     * Returns SQL statement string representation.
     *
     * @return SQL statement string.
     */
    @NotNull String query();

    /**
     * Returns query timeout.
     *
     * @param timeUnit Timeunit to convert timeout to.
     * @return Query timeout in the given timeunit.
     */
    long queryTimeout(@NotNull TimeUnit timeUnit);

    /**
     * Returns statement default schema.
     *
     * @return Default schema for the statement.
     */
    String defaultSchema();

    /**
     * Returns page size, which is a maximal amount of results rows that can be fetched once at a time.
     *
     * @return Maximal amount of rows in a page.
     */
    int pageSize();

    /**
     * Returns statement property value that overrides the session property value or {@code null} if session property value should be used.
     *
     * @param name Property name.
     * @return Property value or {@code null} if not set.
     */
    @Nullable Object property(@NotNull String name);

    /**
     * Creates a new statement builder from current statement.
     *
     * @return Statement builder based on the current statement.
     */
    StatementBuilder toBuilder();

    /**
     * Statement builder provides methods for building statement object, which represents a query and holds a query-specific settings that
     * overrides the session defaults.
     */
    interface StatementBuilder {
        /**
         * Returns SQL statement string representation.
         *
         * @return SQL statement string.
         */
        @NotNull String query();

        /**
         * Set SQL statement string.
         *
         * @param sql SQL query.
         * @return {@code this} for chaining.
         */
        StatementBuilder query(String sql);

        /**
         * Returns query timeout.
         *
         * @param timeUnit Timeunit to convert timeout to.
         * @return Query timeout in the given timeunit.
         */
        long queryTimeout(@NotNull TimeUnit timeUnit);

        /**
         * Sets query timeout.
         *
         * @param timeout Query timeout value.
         * @param timeUnit Timeunit.
         * @return {@code this} for chaining.
         */
        StatementBuilder queryTimeout(long timeout, @NotNull TimeUnit timeUnit);

        /**
         * Returns statement default schema.
         *
         * @return Default schema for the statement.
         */
        String defaultSchema();

        /**
         * Sets default schema for the statement, which the queries will be executed with.
         *
         * @param schema Default schema.
         * @return {@code this} for chaining.
         */
        StatementBuilder defaultSchema(@NotNull String schema);

        /**
         * Returns page size, which is a maximal amount of results rows that can be fetched once at a time.
         *
         * @return Maximal amount of rows in a page.
         */
        int pageSize();

        /**
         * Sets page size, which is a maximal amount of results rows that can be fetched once at a time.
         *
         * @param pageSize Maximal amount of rows in a page.
         * @return {@code this} for chaining.
         */
        StatementBuilder pageSize(int pageSize);

        /**
         * Returns statement property value that overrides the session property value or {@code null} if session property value should be
         * used.
         *
         * @param name Property name.
         * @return Property value or {@code null} if not set.
         */
        @Nullable Object property(@NotNull String name);

        /**
         * Sets statement property value that overrides the session property value. If {@code null} is passed, then a session property value
         * will be used.
         *
         * @param name Property name.
         * @param value Property value or {@code null} to use a value defined in a session.
         * @return {@code this} for chaining.
         */
        StatementBuilder property(@NotNull String name, @Nullable Object value);

        /**
         * Creates an SQL statement abject.
         *
         * @return Statement.
         */
        Statement build();
    }
}
