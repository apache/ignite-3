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

package org.apache.ignite.internal.catalog;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * The catalog manager provides schema manipulation methods and is responsible for managing distributed operations.
 */
public interface CatalogManager extends IgniteComponent, CatalogService {
    /**
     * Executes given command.
     *
     * <p>Accepts only those commands provided by builders returned by this very {@link CatalogManager}.
     * Otherwise will throw {@link IllegalArgumentException}.
     *
     * @param command Command to execute.
     * @return Future representing result of execution.
     * @throws IllegalArgumentException If given command was created not by this manager.
     * @see #createTableCommandBuilder()
     */
    CompletableFuture<Void> execute(CatalogCommand command) throws IllegalArgumentException;

    /**
     * Returns builder to create a command to create a new table.
     */
    CreateTableCommandBuilder createTableCommandBuilder();

    /**
     * Drops table.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> dropTable(DropTableParams params);

    /**
     * Add columns to a table.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> addColumn(AlterTableAddColumnParams params);

    /**
     * Drops columns from table.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> dropColumn(AlterTableDropColumnParams params);

    /**
     * Changes a table column.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> alterColumn(AlterColumnParams params);

    /**
     * Creates new sorted index.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> createIndex(CreateSortedIndexParams params);

    /**
     * Creates new hash index.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> createIndex(CreateHashIndexParams params);

    /**
     * Drops index.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> dropIndex(DropIndexParams params);

    /**
     * Creates new distribution zone.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> createZone(CreateZoneParams params);

    /**
     * Drops distribution zone.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> dropZone(DropZoneParams params);

    /**
     * Alter distribution zone.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> alterZone(AlterZoneParams params);

    /**
     * Rename distribution zone.
     *
     * @param params Parameters.
     * @return Operation future.
     */
    CompletableFuture<Void> renameZone(RenameZoneParams params);
}
