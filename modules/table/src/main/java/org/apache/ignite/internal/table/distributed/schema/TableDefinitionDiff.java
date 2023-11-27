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

package org.apache.ignite.internal.table.distributed.schema;

import static java.util.Collections.emptyList;

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;

/**
 * Captures the difference between two versions of the same table definition.
 */
public class TableDefinitionDiff {
    private static final TableDefinitionDiff EMPTY = new TableDefinitionDiff(
            -1, -1, "name", "name", emptyList(), emptyList(), emptyList()
    );

    private final int oldSchemaVersion;
    private final int newSchemaVersion;

    private final boolean nameDiffers;
    private final List<CatalogTableColumnDescriptor> addedColumns;
    private final List<CatalogTableColumnDescriptor> removedColumns;
    private final List<ColumnDefinitionDiff> changedColumns;

    /**
     * Returns an empty diff (meaning there is no difference).
     *
     * @return Empty diff (meaning there is no difference).
     */
    public static TableDefinitionDiff empty() {
        return EMPTY;
    }

    /**
     * Constructor.
     */
    public TableDefinitionDiff(
            int oldSchemaVersion,
            int newSchemaVersion,
            String oldName,
            String newName,
            List<CatalogTableColumnDescriptor> addedColumns,
            List<CatalogTableColumnDescriptor> removedColumns,
            List<ColumnDefinitionDiff> changedColumns
    ) {
        this.oldSchemaVersion = oldSchemaVersion;
        this.newSchemaVersion = newSchemaVersion;

        nameDiffers = !oldName.equals(newName);
        this.addedColumns = List.copyOf(addedColumns);
        this.removedColumns = List.copyOf(removedColumns);
        this.changedColumns = List.copyOf(changedColumns);
    }

    /**
     * Returns old schema version.
     */
    public int oldSchemaVersion() {
        return oldSchemaVersion;
    }

    /**
     * Returns new schema version.
     */
    public int newSchemaVersion() {
        return newSchemaVersion;
    }

    /**
     * Returns whether name of the table has been changed.
     */
    public boolean nameDiffers() {
        return nameDiffers;
    }

    /**
     * Returns columns that were added.
     */
    public List<CatalogTableColumnDescriptor> addedColumns() {
        return addedColumns;
    }

    /**
     * Returns columns that were removed.
     */
    public List<CatalogTableColumnDescriptor> removedColumns() {
        return removedColumns;
    }

    /**
     * Returns columns that were changed.
     */
    public List<ColumnDefinitionDiff> changedColumns() {
        return changedColumns;
    }
}
