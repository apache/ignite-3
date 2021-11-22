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

/**
 * The object represents parameterized SQL query statement that supports batched query, and provides methods for managing it`s state.
 */
public interface Statement {
    /**
     * Returns SQL statement string representation.
     *
     * @return SQL statement string.
     */
    String query();
    
    /**
     * Returns SQL statement parameters.
     *
     * @return SQL statement parameters.
     */
    Object[] parameters();
    
    /**
     * Sets SQL statement parameters.
     *
     * @param parameters SQL statement parameters.
     * @return {@code this} for chaining.
     */
    Statement parameters(Object... parameters);
    
    /**
     * Sets SQL statement parameter value by the parameter index.
     *
     * @param index Parameter index.
     * @param value Parameter value.
     * @return {@code this} for chaining.
     */
    Statement parameter(int index, Object value);
    
    /**
     * Resets batch state and clears query parameters.
     *
     * @return {@code this} for chaining.
     */
    Statement resetState();
    
    /**
     * Adds a set of parameters to this statement object's batch of commands.
     *
     * @return {@code this} for chaining.
     */
    Statement addBatch() throws SqlException;
    
    /**
     * Sets query timeout.
     *
     * @param timeout  Query timeout value.
     * @param timeUnit Timeunit.
     */
    void queryTimeout(long timeout, TimeUnit timeUnit);
    
    /**
     * Gets query timeout.
     *
     * @param timeUnit Timeunit.
     * @return Query timeout.
     */
    long queryTimeout(TimeUnit timeUnit);
}
