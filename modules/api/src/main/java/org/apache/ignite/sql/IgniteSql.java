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

/**
 * Ignite SQL query facade.
 */
public interface IgniteSql {
    /**
     * Creates an SQL session object that provides methods for executing SQL queries and holds settings with which queries will be executed.
     *
     * @return A new session.
     */
    Session createSession();

    /**
     * Creates an SQL statement abject, which represents a query and holds a query-specific settings that overrides the session default
     * settings.
     *
     * @param query SQL query template.
     * @return A new statement.
     */
    Statement createStatement(String query);
}
