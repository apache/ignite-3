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

package org.apache.ignite.table.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.table.Table;

/**
 * Interface that provides methods for managing tables.
 */
public interface IgniteTables {
    /**
     * Creates a new table with the given {@code name}. If a table with the same name already exists, an exception is thrown.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) in SQL-parser style notation, e.g.,
     *             "public.tbl0" - the "PUBLIC.TBL0" table is created,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param tableInitChange Table changer.
     * @return The created table.
     * @throws TableAlreadyExistsException If a table with teh given name already exists.
     * @throws IgniteException             If an unspecified platform exception has occurred internally. Is thrown when:
     *                                     <ul>
     *                                         <li>the node is stopping.</li>
     *                                     </ul>
     */
    Table createTable(String name, Consumer<TableChange> tableInitChange);

    /**
     * Creates a new table with the given {@code name} asynchronously. If a table with the same name already exists, a future is
     * completed with the {@link TableAlreadyExistsException}.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) in SQL-parser style notation, e.g.,
     *             "public.tbl0" - the "PUBLIC.TBL0" table is created,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param tableInitChange Table changer.
     * @return Future that represents the pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableAlreadyExistsException
     */
    CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange);

    /**
     * Alters a cluster table. If the specified table does not exist, an exception is thrown.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) in SQL-parser style notation, e.g.,
     *             "public.tbl0" - the "PUBLIC.TBL0" table is changed,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param tableChange Table changer.
     * @throws TableNotFoundException If a table with the {@code name} does not exist.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    void alterTable(String name, Consumer<TableChange> tableChange);

    /**
     * Alters a cluster table. If the specified table does not exist, a future is completed with the {@link TableNotFoundException}.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) in SQL-parser style notation, e.g.,
     *             "public.tbl0" - the "PUBLIC.TBL0" table is changed,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @param tableChange Table changer.
     * @return Future that represents the pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableNotFoundException
     */
    CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange);

    /**
     * Drops a table with the specified name. If the specified table does not exist, an exception is thrown.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) in SQL-parser style notation, e.g.,
     *             "public.tbl0" - the "PUBLIC.TBL0" table is dropped,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @throws TableNotFoundException If a table with the {@code name} does not exist.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    void dropTable(String name);

    /**
     * Drops a table with the specified name. If the specified table does not exist, a future is completed with the {@link TableNotFoundException}.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) in SQL-parser style notation, e.g.,
     *             "public.tbl0" - the "PUBLIC.TBL0" table is dropped,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Future that represents the pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableNotFoundException
     */
    CompletableFuture<Void> dropTableAsync(String name);

    /**
     * Gets a list of all started tables.
     *
     * @return List of tables.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    List<Table> tables();

    /**
     * Gets a list of all started tables.
     *
     * @return Future that represents the pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    CompletableFuture<List<Table>> tablesAsync();

    /**
     * Gets a table with the specified name if that table exists.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) in SQL-parser style notation, e.g.,
     *             "public.tbl0" - the "PUBLIC.TBL0" table is looked up,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Table identified by name or {@code null} if table doesn't exist.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    Table table(String name);

    /**
     * Gets a table with the specified name if that table exists.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) in SQL-parser style notation, e.g.,
     *             "public.tbl0" - the "PUBLIC.TBL0" table is looked up,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Future that represents the pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has occurred internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    CompletableFuture<Table> tableAsync(String name);
}
