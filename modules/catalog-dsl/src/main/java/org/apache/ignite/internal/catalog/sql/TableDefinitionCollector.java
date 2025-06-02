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

package org.apache.ignite.internal.catalog.sql;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition.Builder;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.QualifiedName;

/**
 * Table definition information collector from system views.
 */
class TableDefinitionCollector {
    private final QualifiedName tableName;

    private final IgniteSql sql;

    TableDefinitionCollector(QualifiedName tableName, IgniteSql sql) {
        this.tableName = tableName;
        this.sql = sql;
    }

    /**
     * Collect all table info from corresponding system views.
     *
     * @return Future with table definition build or {@code null} if table doesn't exist.
     */
    CompletableFuture<TableDefinition> collectDefinition() {
        return collectTableInfo()
                .thenCompose(builder -> {
                    if (builder == null) {
                        return nullCompletedFuture();
                    } else {
                        return collectIndexes(builder).thenApply(Builder::build);
                    }
                });
    }

    private CompletableFuture<TableDefinitionBuilderWithIndexId> collectTableInfo() {
        String query = "SELECT "
                + "pk_index_id, zone_name, column_name, column_type, column_precision, column_scale, column_length, is_nullable_column, "
                + "colocation_column_ordinal "
                + "FROM system.tables t "
                + "JOIN system.table_columns USING (table_id) "
                + "WHERE t.schema_name=? AND t.table_name=? "
                + "ORDER BY column_ordinal";

        return SelectFromView.collectResults(sql, query, Function.identity(), tableName.schemaName(), tableName.objectName())
                .thenApply(list -> {
                    if (list.isEmpty()) {
                        return null;
                    }

                    List<ColumnDefinition> columns = new ArrayList<>();
                    int indexId = list.get(0).intValue("PK_INDEX_ID");
                    String zoneName = list.get(0).stringValue("ZONE_NAME");
                    TreeMap<Integer, String> colocationColumns = new TreeMap<>();

                    for (SqlRow row : list) {
                        String columnName = row.stringValue("COLUMN_NAME");
                        String type = row.stringValue("COLUMN_TYPE");
                        int length = row.intValue("COLUMN_LENGTH");
                        int precision = row.intValue("COLUMN_PRECISION");
                        int scale = row.intValue("COLUMN_SCALE");
                        boolean nullable = row.booleanValue("IS_NULLABLE_COLUMN");
                        Integer colocationOrd = row.value("COLOCATION_COLUMN_ORDINAL");

                        if (colocationOrd != null) {
                            colocationColumns.put(colocationOrd, columnName);
                        }

                        Class<?> typeClass = org.apache.ignite.sql.ColumnType.valueOf(type).javaClass();
                        ColumnType<?> columnType = ColumnType.of(typeClass, length, precision, scale, nullable);
                        ColumnDefinition column = ColumnDefinition.column(columnName, columnType);
                        columns.add(column);
                    }

                    List<String> colocationColumnList = new ArrayList<>(colocationColumns.values());

                    Builder builder = TableDefinition.builder(tableName)
                            .zone(zoneName)
                            .columns(columns)
                            .colocateBy(colocationColumnList);

                    return new TableDefinitionBuilderWithIndexId(builder, indexId);
                });
    }

    private CompletableFuture<Builder> collectIndexes(TableDefinitionBuilderWithIndexId definition) {
        String query = "SELECT i.index_id, i.index_type, i.index_name, column_name, column_ordinal, column_collation "
                + "FROM system.indexes i "
                + "JOIN system.tables t USING (table_id) "
                + "JOIN system.index_columns ic USING (index_id) "
                + "WHERE t.schema_name=? AND t.table_name=?"
                + "ORDER BY index_id, column_ordinal";

        return SelectFromView.collectResults(sql, query, Function.identity(), tableName.schemaName(), tableName.objectName())
                .thenApply(list -> {
                    LinkedHashMap<Integer, List<ColumnSorted>> indexIdColumns = new LinkedHashMap<>();
                    Map<Integer, String> indexNames = new HashMap<>();
                    Map<Integer, IndexType> indexTypes = new HashMap<>();

                    for (SqlRow row : list) {
                        int indexId = row.intValue("INDEX_ID");
                        String indexName = row.stringValue("INDEX_NAME");
                        String indexType = row.stringValue("INDEX_TYPE");
                        String columnName = row.stringValue("COLUMN_NAME");
                        String columnCollation = row.stringValue("COLUMN_COLLATION");

                        List<ColumnSorted> columns = indexIdColumns.computeIfAbsent(indexId, key -> new ArrayList<>());
                        indexNames.put(indexId, indexName);
                        indexTypes.put(indexId, IndexType.valueOf(indexType));

                        if (columnCollation == null) {
                            columns.add(ColumnSorted.column(columnName));
                        } else {
                            columns.add(ColumnSorted.column(columnName, SortOrder.valueOf(columnCollation)));
                        }
                    }

                    for (Map.Entry<Integer, List<ColumnSorted>> entry : indexIdColumns.entrySet()) {
                        String name = indexNames.get(entry.getKey());
                        IndexType indexType = indexTypes.get(entry.getKey());

                        if (Objects.equals(entry.getKey(), definition.indexId)) {
                            definition.builder.primaryKey(indexType, entry.getValue());
                        } else {
                            definition.builder.index(name, indexType, entry.getValue());
                        }
                    }

                    return definition.builder;
                });
    }

    private static class TableDefinitionBuilderWithIndexId {
        private final TableDefinition.Builder builder;

        private final int indexId;

        private TableDefinitionBuilderWithIndexId(TableDefinition.Builder builder, int indexId) {
            this.builder = builder;
            this.indexId = indexId;
        }
    }
}
