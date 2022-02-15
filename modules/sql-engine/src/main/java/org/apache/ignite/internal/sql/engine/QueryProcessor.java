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

package org.apache.ignite.internal.sql.engine;

import java.util.List;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteException;

/**
 * QueryProcessor interface.
 */
public interface QueryProcessor extends IgniteComponent {
    /**
     * Execute the query with given schema name and parameters.
     *
     * @param schemaName Schema name.
     * @param qry Sql query.
     * @param params Query parameters.
     * @return List of sql cursors.
     *
     * @throws IgniteException in case of an error.
     * */
    List<SqlCursor<List<?>>> query(String schemaName, String qry, Object... params);

    /**
     * Execute the query with given schema name and parameters.
     *
     * @param context User query context.
     * @param schemaName Schema name.
     * @param qry Sql query.
     * @param params Query parameters.
     * @return List of sql cursors.
     *
     * @throws IgniteException in case of an error.
     * */
    List<SqlCursor<List<?>>> query(QueryContext context, String schemaName, String qry, Object... params);
}
