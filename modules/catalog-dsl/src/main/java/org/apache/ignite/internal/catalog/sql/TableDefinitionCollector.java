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

import static org.apache.ignite.internal.catalog.sql.Option.schemaName;
import static org.apache.ignite.internal.catalog.sql.Option.tableName;
import static org.apache.ignite.internal.catalog.sql.QueryUtils.splitByComma;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private static final List<String> TABLE_VIEW_COLUMNS = List.of("SCHEMA", "PK_INDEX_ID", "ZONE", "COLOCATION_KEY_INDEX");

    private static final List<String> TABLE_COLUMNS_VIEW_COLUMNS = List.of("COLUMN_NAME", "TYPE", "LENGTH", "PREC", "SCALE",
            "NULLABLE");

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
    CompletableFuture<TableDefinition.Builder> collectDefinition() {
        return collectTableInfo()
                .thenCompose(builder -> {
                    if (builder == null) {
                        return nullCompletedFuture();
                    } else {
                        return collectIndexes(builder).thenCompose(this::collectColumns);
                    }
                });
    }

    private CompletableFuture<TableDefinitionBuilderWithIndexId> collectTableInfo() {
        List<Option> options = List.of(
                schemaName(tableName.schemaName()),
                tableName(tableName.objectName())
        );

        return new SelectFromView<>(sql, TABLE_VIEW_COLUMNS, "TABLES", options, row -> {
            String schema = row.stringValue("SCHEMA");
            int indexId = row.intValue("PK_INDEX_ID");
            String zone = row.stringValue("ZONE");
            String colocationColumns = row.stringValue("COLOCATION_KEY_INDEX");
            Builder builder = TableDefinition.builder(tableName.objectName()).schema(schema)
                    .zone(zone)
                    .colocateBy(splitByComma(colocationColumns));
            return new TableDefinitionBuilderWithIndexId(builder, indexId);
        }).executeAsync().thenApply(definitions -> {
            if (definitions.isEmpty()) {
                return null;
            }

            assert definitions.size() == 1;

            return definitions.get(0);
        });
    }

    private CompletableFuture<Builder> collectIndexes(TableDefinitionBuilderWithIndexId definition) {
        String query = "SELECT i.index_id, i.index_type, i.index_name, column_name, column_ordinal, column_collation "
                + "FROM system.indexes i "
                + "JOIN system.index_columns ic USING (index_id) "
                + "ORDER BY index_id, column_ordinal";

        return SelectFromView.collectResults(sql, query, Function.identity())
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

    private CompletableFuture<TableDefinition.Builder> collectColumns(TableDefinition.Builder builder) {
        List<Option> options = List.of(
                schemaName(tableName.schemaName()),
                tableName(tableName.objectName())
        );

        return new SelectFromView<>(sql, TABLE_COLUMNS_VIEW_COLUMNS, "TABLE_COLUMNS", options,
                row -> {
                    String columnName = row.stringValue("COLUMN_NAME");
                    String type = row.stringValue("TYPE");
                    int length = row.intValue("LENGTH");
                    int precision = row.intValue("PREC");
                    int scale = row.intValue("SCALE");
                    boolean nullable = row.booleanValue("NULLABLE");

                    Class<?> typeClass = org.apache.ignite.sql.ColumnType.valueOf(type).javaClass();
                    return ColumnDefinition.column(columnName, ColumnType.of(typeClass, length, precision, scale, nullable));
                }).executeAsync().thenApply(builder::columns);
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
