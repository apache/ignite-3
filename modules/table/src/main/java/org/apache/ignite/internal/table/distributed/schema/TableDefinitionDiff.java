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
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;

/**
 * Captures the difference between two versions of the same table definition.
 */
public class TableDefinitionDiff {
    private static final TableDefinitionDiff EMPTY = new TableDefinitionDiff(
            emptyList(), emptyList(), emptyList(), emptyList(), emptyList()
    );

    private final List<CatalogTableColumnDescriptor> addedColumns;
    private final List<CatalogTableColumnDescriptor> removedColumns;
    private final List<ColumnDefinitionDiff> changedColumns;

    private final List<CatalogIndexDescriptor> addedIndexes;
    private final List<CatalogIndexDescriptor> removedIndexes;

    // TODO: IGNITE-19229 - other change types

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
            List<CatalogTableColumnDescriptor> addedColumns,
            List<CatalogTableColumnDescriptor> removedColumns,
            List<ColumnDefinitionDiff> changedColumns,
            List<CatalogIndexDescriptor> addedIndexes,
            List<CatalogIndexDescriptor> removedIndexes
    ) {
        this.addedColumns = List.copyOf(addedColumns);
        this.removedColumns = List.copyOf(removedColumns);
        this.changedColumns = List.copyOf(changedColumns);
        this.addedIndexes = List.copyOf(addedIndexes);
        this.removedIndexes = List.copyOf(removedIndexes);
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

    /**
     * Returns indexes that were added.
     */
    public List<CatalogIndexDescriptor> addedIndexes() {
        return addedIndexes;
    }

    /**
     * Returns indexes that were removed.
     */
    public List<CatalogIndexDescriptor> removedIndexes() {
        return removedIndexes;
    }

    /**
     * Returns whether this diff is empty (so no difference at all).
     *
     * @return Whether this diff is empty (so no difference at all).
     */
    public boolean isEmpty() {
        return addedColumns.isEmpty()
                && removedColumns.isEmpty()
                && changedColumns.isEmpty()
                && addedIndexes.isEmpty()
                && removedIndexes.isEmpty();
    }
}
