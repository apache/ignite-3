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

package org.apache.ignite.internal.catalog.commands;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.jetbrains.annotations.Nullable;

/**
 * Builder of a command that adds a new table to the catalog.
 */
public interface CreateTableCommandBuilder {
    /** A name of the schema to add new table to. Should not be null or blank. */
    CreateTableCommandBuilder schemaName(String schemaName);

    /** A name of the table to add. Should not be null or blank. */
    CreateTableCommandBuilder tableName(String tableName);

    /** List of columns a new table should be created with. There must be at least one column. */
    CreateTableCommandBuilder columns(List<ColumnParams> columns);

    /**
     * List of columns representing primary key. There must be at list one column. All columns must
     * be presented in {@link #columns(List) list of columns}.
     */
    CreateTableCommandBuilder primaryKeyColumns(List<String> primaryKeyColumns);

    /**
     * List of colocation columns. Must not be empty, but may be null. All columns, if any,
     * must be presented in {@link #primaryKeyColumns(List) list of PK columns}.
     */
    CreateTableCommandBuilder colocationColumns(@Nullable List<String> colocationColumns);

    /** A name of the zone to create new table in. Should not be null or blank. */
    CreateTableCommandBuilder zone(@Nullable String zoneName);

    /** Returns a command with specified parameters. */
    CatalogCommand build();
}
