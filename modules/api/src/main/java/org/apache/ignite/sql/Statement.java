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
 * <p>Modifying session state from multiple threads may lead to unexpected behaviour. However, the statement object can be reused unless it
 * wss closed and can be executed from concurrent threads.
 *
 * <p>Prepared statement forces performance optimizations, such as query plan caching on the server side, which is useful for frequently
 * executed queries or for queries when low-latency is critical. However, statement execution flow may fallback to a normal flow for a shot
 * time in some situations, e.g. when cached state on the server-side was lost, or the query has to be re-planned due to statement
 * properties was changes.
 */
public interface Statement extends AutoCloseable {
    /**
     * Returns SQL statement string representation.
     *
     * @return SQL statement string.
     */
    @NotNull String query();

    /**
     * Returns a flag indicating whether it is prepared statement or not.
     *
     * @return {@code true} if this is prepared statement, {@code false} otherwise.
     */
    boolean prepared();

    /**
     * Marks current statement as prepared and prepare it on next statement execution.
     */
    void prepare();

    /**
     * Sets query timeout.
     *
     * @param timeout Query timeout value.
     * @param timeUnit Timeunit.
     */
    void queryTimeout(long timeout, @NotNull TimeUnit timeUnit);

    /**
     * Returns query timeout.
     *
     * @param timeUnit Timeunit to convert timeout to.
     * @return Query timeout in the given timeunit.
     */
    long queryTimeout(@NotNull TimeUnit timeUnit);

    /**
     * Sets default schema for the statement, which the queries will be executed with.
     *
     * @param schema Default schema.
     */
    void defaultSchema(@NotNull String schema);

    /**
     * Returns statement default schema.
     *
     * @return Session default schema.
     * @see Session#defaultSchema()
     */
    @NotNull String defaultSchema();

    /**
     * Sets page size, which is a maximal amount of results rows that can be fetched once at a time.
     *
     * @param pageSize Maximal amount of rows in a page.
     * @return {@code this} for chaining.
     */
    Session pageSize(int pageSize);

    /**
     * Returns page size, which is a maximal amount of results rows that can be fetched once at a time.
     *
     * <p>Default value is {@link Session#DEFAULT_PAGE_SIZE}.
     *
     * @return Maximal amount of rows in a page.
     */
    int pageSize();

    /**
     * Sets statement property value that overrides the session property value. If {@code null} is passed, then a session property value
     * will be used.
     *
     * @param name Property name.
     * @param value Property value or {@code null} to a session property value.
     * @return {@code this} for chaining.
     */
    Session property(@NotNull String name, @Nullable Object value);

    /**
     * Returns statement property value that overrides the session property value or {@code null} if session property value should be used.
     *
     * @param name Property name.
     * @return Property value or {@code null} if not set.
     */
    @Nullable Object property(@NotNull String name);

    /**
     * Closes statement and releases remote resources.
     */
    @Override
    void close();
}
