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
import static org.apache.ignite.internal.util.CollectionUtils.intersect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
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
        Map<String, TableColumnDescriptor> prevColumnsByName = toMapByName(prevSchema.columns, TableColumnDescriptor::name);
        Map<String, TableColumnDescriptor> thisColumnsByName = toMapByName(this.columns, TableColumnDescriptor::name);

        List<TableColumnDescriptor> addedColumns = subtractKeyed(thisColumnsByName, prevColumnsByName);
        List<TableColumnDescriptor> removedColumns = subtractKeyed(prevColumnsByName, thisColumnsByName);

        Set<String> intersectionColumnNames = intersect(thisColumnsByName.keySet(), prevColumnsByName.keySet());
        List<ColumnDefinitionDiff> changedColumns = new ArrayList<>();
        for (String commonColumnName : intersectionColumnNames) {
            TableColumnDescriptor prevColumn = prevColumnsByName.get(commonColumnName);
            TableColumnDescriptor thisColumn = thisColumnsByName.get(commonColumnName);

            if (columnChanged(prevColumn, thisColumn)) {
                changedColumns.add(new ColumnDefinitionDiff(prevColumn, thisColumn));
            }
        }

        Map<String, IndexDescriptor> prevIndexesByName = toMapByName(prevSchema.indexes, IndexDescriptor::name);
        Map<String, IndexDescriptor> thisIndexesByName = toMapByName(this.indexes, IndexDescriptor::name);

        List<IndexDescriptor> addedIndexes = subtractKeyed(thisIndexesByName, prevIndexesByName);
        List<IndexDescriptor> removedIndexes = subtractKeyed(prevIndexesByName, thisIndexesByName);

        return new TableDefinitionDiff(addedColumns, removedColumns, changedColumns, addedIndexes, removedIndexes);
    }

    private static <T> Map<String, T> toMapByName(List<T> elements, Function<T, String> nameExtractor) {
        return elements.stream().collect(toMap(nameExtractor, identity()));
    }

    private static <T> List<T> subtractKeyed(Map<String, T> minuend, Map<String, T> subtrahend) {
        return minuend.entrySet().stream()
                .filter(entry -> !subtrahend.containsKey(entry.getKey()))
                .map(Entry::getValue)
                .collect(toList());
    }

    private static boolean columnChanged(TableColumnDescriptor prevColumn, TableColumnDescriptor newColumn) {
        return !prevColumn.equals(newColumn);
    }
}
