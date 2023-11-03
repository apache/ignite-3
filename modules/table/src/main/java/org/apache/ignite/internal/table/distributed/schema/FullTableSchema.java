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
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;

/**
 * Represents a full table schema: that is, the definition of the table and all objects (constraints, etc)
 * that belong to the table *that might affect schema compatibility* (so, indices are not included as they
 * don't affect such compatibility).
 */
public class FullTableSchema {
    private final int schemaVersion;
    private final int tableId;

    private final List<CatalogTableColumnDescriptor> columns;

    private final List<CatalogIndexDescriptor> indexes;

    /**
     * Constructor.
     */
    public FullTableSchema(
            int schemaVersion,
            int tableId,
            List<CatalogTableColumnDescriptor> columns,
            List<CatalogIndexDescriptor> indexes
    ) {
        this.schemaVersion = schemaVersion;
        this.tableId = tableId;
        this.columns = List.copyOf(columns);
        this.indexes = List.copyOf(indexes);
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
    public List<CatalogTableColumnDescriptor> columns() {
        return columns;
    }

    /**
     * Returns definitions of indexes belonging to the table.
     *
     * @return Definitions of indexes belonging to the table.
     */
    public List<CatalogIndexDescriptor> indexes() {
        return indexes;
    }

    /**
     * Computes a diff between this and a previous schema.
     *
     * @param prevSchema Previous table schema.
     * @return Difference between the schemas.
     */
    public TableDefinitionDiff diffFrom(FullTableSchema prevSchema) {
        Map<String, CatalogTableColumnDescriptor> prevColumnsByName = toMapByName(prevSchema.columns, CatalogTableColumnDescriptor::name);
        Map<String, CatalogTableColumnDescriptor> thisColumnsByName = toMapByName(this.columns, CatalogTableColumnDescriptor::name);

        List<CatalogTableColumnDescriptor> addedColumns = subtractKeyed(thisColumnsByName, prevColumnsByName);
        List<CatalogTableColumnDescriptor> removedColumns = subtractKeyed(prevColumnsByName, thisColumnsByName);

        Set<String> intersectionColumnNames = intersect(thisColumnsByName.keySet(), prevColumnsByName.keySet());
        List<ColumnDefinitionDiff> changedColumns = new ArrayList<>();
        for (String commonColumnName : intersectionColumnNames) {
            CatalogTableColumnDescriptor prevColumn = prevColumnsByName.get(commonColumnName);
            CatalogTableColumnDescriptor thisColumn = thisColumnsByName.get(commonColumnName);

            if (columnChanged(prevColumn, thisColumn)) {
                changedColumns.add(new ColumnDefinitionDiff(prevColumn, thisColumn));
            }
        }

        Map<String, CatalogIndexDescriptor> prevIndexesByName = toMapByName(prevSchema.indexes, CatalogIndexDescriptor::name);
        Map<String, CatalogIndexDescriptor> thisIndexesByName = toMapByName(this.indexes, CatalogIndexDescriptor::name);

        List<CatalogIndexDescriptor> addedIndexes = subtractKeyed(thisIndexesByName, prevIndexesByName);
        List<CatalogIndexDescriptor> removedIndexes = subtractKeyed(prevIndexesByName, thisIndexesByName);

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

    private static boolean columnChanged(CatalogTableColumnDescriptor prevColumn, CatalogTableColumnDescriptor newColumn) {
        return !prevColumn.equals(newColumn);
    }
}
