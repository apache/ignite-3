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

import static org.apache.ignite.internal.catalog.sql.Option.name;
import static org.apache.ignite.internal.catalog.sql.Option.tableName;
import static org.apache.ignite.internal.catalog.sql.QueryUtils.splitByComma;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition.Builder;
import org.apache.ignite.sql.IgniteSql;

/**
 * Table definition information collector from system views.
 */
public class TableDefinitionCollector {
    private static final List<String> TABLE_VIEW_COLUMNS = List.of("SCHEMA", "PK_INDEX_ID", "ZONE", "COLOCATION_KEY_INDEX");

    private static final List<String> INDEX_VIEW_COLUMNS = List.of("INDEX_ID", "INDEX_NAME", "COLUMNS", "TYPE");

    private static final List<String> TABLE_COLUMNS_VIEW_COLUMNS = List.of("COLUMN_NAME", "TYPE", "LENGTH", "PREC", "SCALE",
            "NULLABLE");

    private final String tableName;

    private final IgniteSql sql;

    public TableDefinitionCollector(String tableName, IgniteSql sql) {
        this.tableName = tableName;
        this.sql = sql;
    }

    /**
     * Collect all table info from corresponding system views.
     *
     * @return Future with table definition build or {@code null} if table doesn't exist.
     */
    public CompletableFuture<TableDefinition.Builder> collectDefinition() {
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
        return new SelectFromView<>(sql, TABLE_VIEW_COLUMNS, "TABLES", name(tableName), row -> {
            String schema = row.stringValue("SCHEMA");
            int indexId = row.intValue("PK_INDEX_ID");
            String zone = row.stringValue("ZONE");
            String colocationColumns = row.stringValue("COLOCATION_KEY_INDEX");
            Builder builder = TableDefinition.builder(tableName).schema(schema)
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

    private CompletableFuture<Builder> collectIndexes(TableDefinitionBuilderWithIndexId tableDefinitionWithPkIndexId) {
        return new SelectFromView<>(sql, INDEX_VIEW_COLUMNS, "INDEXES", tableName(tableName), row -> {
            int pkIndexId = tableDefinitionWithPkIndexId.indexId;

            int indexId = row.intValue("INDEX_ID");
            String indexName = row.stringValue("INDEX_NAME");
            String columns = row.stringValue("COLUMNS");
            String type = row.stringValue("TYPE");

            return Index.create(indexName, type, columns, indexId == pkIndexId);
        }).executeAsync().thenApply(indexes -> {
            Builder builder = tableDefinitionWithPkIndexId.builder;
            for (Index index : indexes) {
                if (index.isPkIndex) {
                    builder.primaryKey(index.type, index.columns);
                } else {
                    builder.index(index.name, index.type, index.columns);
                }
            }
            return builder;
        });
    }

    private CompletableFuture<TableDefinition.Builder> collectColumns(TableDefinition.Builder builder) {
        return new SelectFromView<>(sql, TABLE_COLUMNS_VIEW_COLUMNS, "TABLE_COLUMNS", tableName(tableName),
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

    private static class Index {
        private final String name;

        private final IndexType type;

        private final List<ColumnSorted> columns;

        private final boolean isPkIndex;

        private Index(String name, IndexType type, List<ColumnSorted> columns, boolean isPkIndex) {
            this.name = name;
            this.type = type;
            this.columns = columns;
            this.isPkIndex = isPkIndex;
        }

        private static Index create(String name, String type, String columns, boolean isPkIndex) {
            return new Index(name, IndexType.valueOf(type), fromRaw(columns), isPkIndex);
        }
    }

    private static List<ColumnSorted> fromRaw(String columns) {
        return Arrays.stream(columns.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(ColumnSorted::column)
                .collect(Collectors.toList());
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
