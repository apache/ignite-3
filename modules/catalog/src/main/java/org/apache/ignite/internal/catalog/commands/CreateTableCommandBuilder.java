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
import org.jetbrains.annotations.Nullable;

/**
 * Builder of a command that adds a new table to the catalog.
 *
 * <p>A builder is considered to be reusable, thus implementation have
 * to make sure invocation of {@link #build()} method doesn't cause any
 * side effects on builder's state or any object created by the same builder.
 */
public interface CreateTableCommandBuilder extends AbstractTableCommandBuilder<CreateTableCommandBuilder> {
    /** List of columns a new table should be created with. There must be at least one column. */
    CreateTableCommandBuilder columns(List<ColumnParams> columns);

    /**
     * Primary key. All columns of a primary key must be present in {@link #columns(List) list of columns}.
     */
    CreateTableCommandBuilder primaryKey(TablePrimaryKey primaryKey);

    /**
     * List of colocation columns. Must not be empty, but may be null. All columns, if any,
     * must be presented in {@link #primaryKey(TablePrimaryKey) primary key}.
     */
    CreateTableCommandBuilder colocationColumns(@Nullable List<String> colocationColumns);

    /** A name of the zone to create new table in. Should not be blank. */
    CreateTableCommandBuilder zone(@Nullable String zoneName);

    /** A name of the table's storage profile. Table's zone must contain this storage profile. */
    CreateTableCommandBuilder storageProfile(@Nullable String storageProfile);

    /** Validate if system schemas are used. */
    CreateTableCommandBuilder validateSystemSchemas(boolean validateSystemSchemas);

    /** A fraction of a partition to be modified before the data is considered to be "stale". Should be in range [0, 1]. */
    CreateTableCommandBuilder staleRowsFraction(double staleRowsFraction);

    /** Minimal number of rows in partition to be modified before the data is considered to be "stale". Should be non-negative. */
    CreateTableCommandBuilder minStaleRowsCount(long minStaleRowsCount);
}
