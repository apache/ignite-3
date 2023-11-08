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

package org.apache.ignite.internal.table;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.manager.IgniteTables;

/**
 * Internal tables facade provides low-level methods for table operations.
 */
public interface IgniteTablesInternal extends IgniteTables {
    /**
     * Gets a table by id.
     *
     * @param id Table ID.
     * @return Table or {@code null} when not exists.
     * @throws NodeStoppingException If an implementation stopped before the method was invoked.
     */
    TableViewInternal table(int id) throws NodeStoppingException;

    /**
     * Gets a table future by id. If the table exists, the future will point to it, otherwise to {@code null}.
     *
     * @param id Table id.
     * @return Future representing pending completion of the operation.
     * @throws NodeStoppingException If an implementation stopped before the method was invoked.
     */
    CompletableFuture<TableViewInternal> tableAsync(int id) throws NodeStoppingException;

    // TODO: IGNITE-16750 - the following two methods look a bit ugly, separation of public/internal Table aspects should help

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be looked up,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Tables with corresponding name or {@code null} if table isn't created.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    TableViewInternal tableView(String name);

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Canonical name of the table ([schemaName].[tableName]) with SQL-parser style quotation, e.g.
     *             "public.tbl0" - the table "PUBLIC.TBL0" will be looked up,
     *             "PUBLIC.\"Tbl0\"" - "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" - "MySchema.Tbl0", etc.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    CompletableFuture<TableViewInternal> tableViewAsync(String name);

    /**
     * Gets a list of the current table assignments.
     *
     * <p>Returns a list where on the i-th place resides a node id that considered as a leader for
     * the i-th partition on the moment of invocation.
     *
     * @param tableId Unique id of a table.
     * @return List of the current assignments.
     */
    CompletableFuture<List<PrimaryReplica>> primaryReplicasAsync(int tableId) throws NodeStoppingException;

    /**
     * Adds a listener to track changes in {@link #primaryReplicasAsync}.
     *
     * @param listener Listener.
     */
    void addAssignmentsChangeListener(Consumer<IgniteTablesInternal> listener);

    /**
     * Removes assignments change listener.
     *
     * @param listener Listener.
     */
    boolean removeAssignmentsChangeListener(Consumer<IgniteTablesInternal> listener);
}
