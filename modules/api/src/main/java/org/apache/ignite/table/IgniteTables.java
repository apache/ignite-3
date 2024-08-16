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

package org.apache.ignite.table;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface that provides methods for managing tables.
 */
public interface IgniteTables {
    /**
     * Gets a list of all tables. The order of tables in the returned list is not defined.
     *
     * @return List of tables.
     */
    List<Table> tables();

    /**
     * Gets a list of all tables. The order of tables in the returned list is not defined.
     *
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<List<Table>> tablesAsync();

    /**
     * Gets a table with the specified name if that table exists.
     *
     * @param name Name of the table with SQL-parser style quotation, e.g.
     *             "tbl0" - the table "TBL0" will be looked up, "\"Tbl0\"" - "Tbl0", etc.
     * @return Table identified by name or {@code null} if table doesn't exist.
     */
    Table table(String name);

    /**
     * Gets a table with the specified name if that table exists.
     *
     * @param name Name of the table with SQL-parser style quotation, e.g.
     *             "tbl0" - the table "TBL0" will be looked up, "\"Tbl0\"" - "Tbl0", etc.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<Table> tableAsync(String name);
}
