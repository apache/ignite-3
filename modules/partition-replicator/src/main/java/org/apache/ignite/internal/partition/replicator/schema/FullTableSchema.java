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

package org.apache.ignite.internal.partition.replicator.schema;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.util.CollectionUtils.intersect;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.jetbrains.annotations.TestOnly;

/**
 * Represents a full table schema: that is, the definition of the table and all objects (constraints, etc)
 * that belong to the table *that might affect schema compatibility* (so, indices are not included as they
 * don't affect such compatibility).
 */
public class FullTableSchema {
    private final int catalogVersion;
    private final int schemaVersion;
    private final int tableId;
    private final String tableName;

    private final List<CatalogTableColumnDescriptor> columns;

    private final Int2IntMap indexesJustStartedBeingBuilt;

    /**
     * Constructor.
     */
    public FullTableSchema(
            int catalogVersion,
            int schemaVersion,
            int tableId,
            String tableName,
            List<CatalogTableColumnDescriptor> columns,
            Int2IntMap indexesJustStartedBeingBuilt
    ) {
        this.catalogVersion = catalogVersion;
        this.schemaVersion = schemaVersion;
        this.tableId = tableId;
        this.tableName = tableName;
        this.columns = List.copyOf(columns);
        this.indexesJustStartedBeingBuilt = indexesJustStartedBeingBuilt.isEmpty() ? Int2IntMaps.EMPTY_MAP
                : new Int2IntOpenHashMap(indexesJustStartedBeingBuilt);
    }

    /**
     * Version of the catalog in which this schema was defined.
     */
    public int catalogVersion() {
        return catalogVersion;
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
     * Returns name of the table.
     */
    public String tableName() {
        return tableName;
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
     * Computes a diff between this and a previous schema.
     *
     * @param prevSchema Previous table schema.
     * @return Difference between the schemas.
     */
    public TableDefinitionDiff diffFrom(FullTableSchema prevSchema) {
        List<CatalogTableColumnDescriptor> addedColumns = List.of();
        List<CatalogTableColumnDescriptor> removedColumns = List.of();
        List<ColumnDefinitionDiff> changedColumns = List.of();

        if (prevSchema.schemaVersion != schemaVersion) {
            Map<String, CatalogTableColumnDescriptor> prevColumnsByName
                    = toMapByName(prevSchema.columns, CatalogTableColumnDescriptor::name);

            Map<String, CatalogTableColumnDescriptor> thisColumnsByName
                    = toMapByName(this.columns, CatalogTableColumnDescriptor::name);

            addedColumns = subtractKeyed(thisColumnsByName, prevColumnsByName);
            removedColumns = subtractKeyed(prevColumnsByName, thisColumnsByName);
            changedColumns = new ArrayList<>();

            Set<String> intersectionColumnNames = intersect(thisColumnsByName.keySet(), prevColumnsByName.keySet());

            for (String commonColumnName : intersectionColumnNames) {
                CatalogTableColumnDescriptor prevColumn = prevColumnsByName.get(commonColumnName);
                CatalogTableColumnDescriptor thisColumn = thisColumnsByName.get(commonColumnName);

                if (columnChanged(prevColumn, thisColumn)) {
                    changedColumns.add(new ColumnDefinitionDiff(prevColumn, thisColumn));
                }
            }
        }

        return new TableDefinitionDiff(
                prevSchema.schemaVersion(),
                this.schemaVersion,
                prevSchema.tableName(),
                this.tableName(),
                addedColumns,
                removedColumns,
                changedColumns,
                indexesJustStartedBeingBuilt
        );
    }

    boolean hasValidatableChangeFrom(FullTableSchema prev) {
        if (this == prev) {
            return false;
        }

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19484 Remove the following condition.
        if (!Objects.equals(tableName, prev.tableName())) {
            return true;
        }

        // Table column related-changes only differ when the schema version is different
        return schemaVersion != prev.schemaVersion()
                // Index build-related changes can differ even if the schema version is the same.
                || someKeyAdded(prev.indexesJustStartedBeingBuilt, indexesJustStartedBeingBuilt);
    }

    private static boolean someKeyAdded(Int2IntMap prevMap, Int2IntMap newMap) {
        for (int key : newMap.keySet()) {
            if (!prevMap.containsKey(key)) {
                return true;
            }
        }
        return false;
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

    @TestOnly
    Int2IntMap indexesJustStartedBeingBuilt() {
        return indexesJustStartedBeingBuilt;
    }
}
