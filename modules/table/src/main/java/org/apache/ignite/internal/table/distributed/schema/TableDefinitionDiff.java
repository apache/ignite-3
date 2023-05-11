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
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;

/**
 * Captures the difference between two versions of the same table definition.
 */
public class TableDefinitionDiff {
    private static final TableDefinitionDiff EMPTY = new TableDefinitionDiff(
            emptyList(), emptyList(), emptyList(), emptyList(), emptyList()
    );

    private final List<TableColumnDescriptor> addedColumns;
    private final List<TableColumnDescriptor> removedColumns;
    private final List<ColumnDefinitionDiff> changedColumns;

    private final List<IndexDescriptor> addedIndexes;
    private final List<IndexDescriptor> removedIndexes;

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
            List<TableColumnDescriptor> addedColumns,
            List<TableColumnDescriptor> removedColumns,
            List<ColumnDefinitionDiff> changedColumns,
            List<IndexDescriptor> addedIndexes,
            List<IndexDescriptor> removedIndexes
    ) {
        this.addedColumns = addedColumns;
        this.removedColumns = removedColumns;
        this.changedColumns = changedColumns;
        this.addedIndexes = addedIndexes;
        this.removedIndexes = removedIndexes;
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
