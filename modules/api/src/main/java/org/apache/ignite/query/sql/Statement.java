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

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * SQL statement.
 */
public interface Statement {
    /**
     * @return Statement SQL query.
     */
    String query();

    /**
     * @return Query parameters.
     */
    Object[] parameters();

    /**
     * @param parameters Query parameters.
     * @return {@code this} for chaining.
     */
    Statement parameters(Object... parameters);

    /**
     * @param parameter Query parameter.
     * @return {@code this} for chaining.
     */
    Statement parameter(int parameterIndex, Object parameter);

    /**
     * Clears query parameters.
     *
     * @return {@code this} for chaining.
     */
    Statement clearParameters();

    /**
     * Adds a set of parameters to this <code>SqlStatement</code> object's batch of commands.
     *
     * @return {@code this} for chaining.
     */
    Statement addBatch() throws SQLException;

    /**
     * Sets query timeout.
     *
     * @param timeout Query timeout value.
     * @param timeUnit Timeunit.
     */
    void queryTimeout(int timeout, TimeUnit timeUnit);

    /**
     * Gets query timeout.
     *
     * @param timeUnit Timeunit.
     * @return Query timeout.
     */
    long queryTimeout(TimeUnit timeUnit);

    /**
     * Sets statement property.
     *
     * @param name Property name.
     * @param value Property value.
     * @return {@code this} for chaining.
     */
    Statement property(@NotNull String name, Object value);
}
