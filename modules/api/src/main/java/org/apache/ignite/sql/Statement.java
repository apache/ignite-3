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
 * The object represents parameterized SQL query statement that supports batched query, and provides methods for managing it`s state.
 */
public interface Statement {
    /**
     * Returns SQL statement string representation.
     *
     * @return SQL statement string.
     */
    @NotNull String query();

    /**
     * Returns current statement parameters.
     *
     * @return Current statement parameters.
     */
    @Nullable Object[] parameters();

    /**
     * Sets SQL statement parameters.
     *
     * @param parameters SQL statement parameters.
     * @return {@code this} for chaining.
     */
    @NotNull Statement parameters(@Nullable Object... parameters);

    /**
     * Sets SQL statement parameter value by the parameter index.
     *
     * @param index Parameter index.
     * @param value Parameter value.
     * @return {@code this} for chaining.
     */
    Statement parameter(int index, @Nullable Object value);

    /**
     * Clears current query parameters. Also, reset batch state if it is a batched query.
     *
     * @return {@code this} for chaining.
     */
    Statement resetState();

    /**
     * Adds a copy of current set of parameters to this statement object's batch of commands, then clears current parameters.
     *
     * @return {@code this} for chaining.
     */
    Statement addToBatch() throws SqlException;

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
}
