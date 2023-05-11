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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;

/**
 * Represents a full table schema: that is, the definition of the table and all objects (indexes, constraints, etc)
 * that belong to the table.
 */
public class FullTableSchema {
    private final int schemaVersion;
    private final int tableId;

    private final List<TableColumnDescriptor> columns;

    private final List<IndexDescriptor> indexes;

    /**
     * Constructor.
     */
    public FullTableSchema(
            int schemaVersion,
            int tableId,
            List<TableColumnDescriptor> columns,
            List<IndexDescriptor> indexes
    ) {
        this.schemaVersion = schemaVersion;
        this.tableId = tableId;
        this.columns = columns;
        this.indexes = indexes;
    }

    /**
     * Returns version of the table definition.
     *
     * @return Version of the table definition.
     */
    public int schemaVersion() {
        return schemaVersion;
    }

    /**
     * Returns ID of the table.
     *
     * @return ID of the table
     */
    public int tableId() {
        return tableId;
    }

    /**
     * Returns definitions of the columns of the table.
     *
     * @return Definitions of the columns of the table.
     */
    public List<TableColumnDescriptor> columns() {
        return columns;
    }

    /**
     * Returns definitions of indexes belonging to the table.
     *
     * @return Definitions of indexes belonging to the table.
     */
    public List<IndexDescriptor> indexes() {
        return indexes;
    }

    /**
     * Computes a diff between this and a previous schema.
     *
     * @param prevSchema Previous table schema.
     * @return Difference between the schemas.
     */
    public TableDefinitionDiff diffFrom(FullTableSchema prevSchema) {
        Map<String, TableColumnDescriptor> prevColumnsByName = prevSchema.columns.stream()
                .collect(toMap(TableColumnDescriptor::name, identity()));
        Map<String, TableColumnDescriptor> thisColumnsByName = this.columns.stream()
                .collect(toMap(TableColumnDescriptor::name, identity()));

        Set<String> addedColumnNames = subtract(thisColumnsByName.keySet(), prevColumnsByName.keySet());
        Set<String> removedColumnNames = subtract(prevColumnsByName.keySet(), thisColumnsByName.keySet());

        List<TableColumnDescriptor> addedColumns = thisColumnsByName.values().stream()
                .filter(col -> addedColumnNames.contains(col.name()))
                .collect(toList());
        List<TableColumnDescriptor> removedColumns = prevColumnsByName.values().stream()
                .filter(col -> removedColumnNames.contains(col.name()))
                .collect(toList());

        Set<String> intersectionColumnNames = intersect(thisColumnsByName.keySet(), prevColumnsByName.keySet());
        List<ColumnDefinitionDiff> changedColumns = new ArrayList<>();
        for (String commonColumnName : intersectionColumnNames) {
            TableColumnDescriptor prevColumn = prevColumnsByName.get(commonColumnName);
            TableColumnDescriptor thisColumn = thisColumnsByName.get(commonColumnName);

            if (columnChanged(prevColumn, thisColumn)) {
                changedColumns.add(new ColumnDefinitionDiff(prevColumn, thisColumn));
            }
        }

        Map<String, IndexDescriptor> prevIndexesByName = prevSchema.indexes.stream()
                .collect(toMap(IndexDescriptor::name, identity()));
        Map<String, IndexDescriptor> thisIndexesByName = this.indexes.stream()
                .collect(toMap(IndexDescriptor::name, identity()));

        Set<String> addedIndexNames = subtract(thisIndexesByName.keySet(), prevIndexesByName.keySet());
        Set<String> removedIndexNames = subtract(prevIndexesByName.keySet(), thisIndexesByName.keySet());

        List<IndexDescriptor> addedIndexes = thisIndexesByName.values().stream()
                .filter(col -> addedIndexNames.contains(col.name()))
                .collect(toList());
        List<IndexDescriptor> removedIndexes = prevIndexesByName.values().stream()
                .filter(col -> removedIndexNames.contains(col.name()))
                .collect(toList());

        return new TableDefinitionDiff(addedColumns, removedColumns, changedColumns, addedIndexes, removedIndexes);
    }

    private static Set<String> subtract(Set<String> minuend, Set<String> subtrahend) {
        Set<String> result = new HashSet<>(minuend);
        result.removeAll(subtrahend);
        return result;
    }

    private static Set<String> intersect(Set<String> set1, Set<String> set2) {
        Set<String> result = new HashSet<>(set1);
        result.retainAll(set2);
        return result;
    }

    private static boolean columnChanged(TableColumnDescriptor prevColumn, TableColumnDescriptor newColumn) {
        return !prevColumn.equals(newColumn);
    }
}
