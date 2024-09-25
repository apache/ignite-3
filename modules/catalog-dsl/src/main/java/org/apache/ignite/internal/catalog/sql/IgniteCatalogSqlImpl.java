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

import static org.apache.ignite.internal.catalog.sql.Option.indexId;
import static org.apache.ignite.internal.catalog.sql.Option.name;
import static org.apache.ignite.internal.catalog.sql.Option.tableName;
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.IndexDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition.Builder;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Table;

/**
 * Implementation of the catalog.
 */
public class IgniteCatalogSqlImpl implements IgniteCatalog {
    private final IgniteSql sql;

    private final IgniteTables tables;

    public IgniteCatalogSqlImpl(IgniteSql sql, IgniteTables tables) {
        this.sql = sql;
        this.tables = tables;
    }

    @Override
    public CompletableFuture<Table> createTableAsync(Class<?> keyClass, Class<?> valueClass) {
        return new CreateFromAnnotationsImpl(sql)
                .processKeyValueClasses(keyClass, valueClass)
                .executeAsync()
                .thenCompose(tableZoneId -> tables.tableAsync(tableZoneId.tableName()));
    }

    @Override
    public CompletableFuture<Table> createTableAsync(Class<?> recordClass) {
        return new CreateFromAnnotationsImpl(sql)
                .processRecordClass(recordClass)
                .executeAsync()
                .thenCompose(tableZoneId -> tables.tableAsync(tableZoneId.tableName()));
    }

    @Override
    public CompletableFuture<Table> createTableAsync(TableDefinition definition) {
        return new CreateFromDefinitionImpl(sql)
                .from(definition)
                .executeAsync()
                .thenCompose(tableZoneId -> tables.tableAsync(tableZoneId.tableName()));
    }

    @Override
    public Table createTable(Class<?> recordClass) {
        return join(createTableAsync(recordClass));
    }

    @Override
    public Table createTable(Class<?> keyClass, Class<?> valueClass) {
        return join(createTableAsync(keyClass, valueClass));
    }

    @Override
    public Table createTable(TableDefinition definition) {
        return join(createTableAsync(definition));
    }

    @Override
    public CompletableFuture<TableDefinition> tableDefinitionAsync(String tableName) {
        return new SelectFromView<>(sql, "TABLES", name(tableName), row -> {
            String schema = row.stringValue("SCHEMA");
            int indexId = row.intValue("PK_INDEX_ID");
            String zone = row.stringValue("ZONE");
            Builder builder = TableDefinition.builder(tableName).schema(schema).zone(zone).primaryKey();
            return new TableDefinitionBuilderWithIndexId(builder, indexId);
        }).executeAsync()
                .thenApply(definitions -> {
                    if (definitions.isEmpty()) {
                        throw new TableNotFoundException(tableName);
                    }

                    assert definitions.size() == 1;

                    return definitions.get(0);
                })
                .thenCompose(tableDefinitionWithPkIndexId ->
                        new SelectFromView<>(sql, "INDEXES", tableName(tableName), row -> {
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
                        }))
                .thenCompose(tableDefinition -> new SelectFromView<>(sql, "TABLES_COLUMNS", tableName(tableName), row -> {
                    String columnName = row.stringValue("COLUMN_NAME");
                    String type = row.stringValue("TYPE");
                    int length = row.intValue("LENGTH");
                    int precision = row.intValue("PRECISION");
                    int scale = row.intValue("SCALE");
                    boolean nullable = row.booleanValue("NULLABLE");

                    Class<?> typeClass = org.apache.ignite.sql.ColumnType.valueOf(type).javaClass();
                    return ColumnDefinition.column(columnName, ColumnType.of(typeClass, length, precision, scale, nullable));
                }).executeAsync().thenApply(tableDefinition::columns))
                .thenCompose(
                        tableDefinition -> new SelectFromView<>(sql, "TABLES_COLOCATION_COLUMNS", tableName(tableName),
                                row -> row.stringValue("COLOCATION_COLUMN"))
                                .executeAsync()
                                .thenApply(colocatedColumn -> tableDefinition.colocateBy(colocatedColumn).build())
                );
    }

    @Override
    public TableDefinition tableDefinition(String tableName) {
        return join(tableDefinitionAsync(tableName));
    }

    @Override
    public CompletableFuture<Void> createZoneAsync(ZoneDefinition definition) {
        return new CreateFromDefinitionImpl(sql)
                .from(definition)
                .executeAsync()
                .thenRun(() -> {});
    }

    @Override
    public void createZone(ZoneDefinition definition) {
        join(createZoneAsync(definition));
    }

    @Override
    public CompletableFuture<ZoneDefinition> zoneDefinitionAsync(String zoneName) {
        return new SelectFromView<>(sql, "ZONES", name(zoneName), row -> toZoneDefinition(zoneName, row))
                .executeAsync()
                .thenApply(zoneDefinitions -> {
                    if (zoneDefinitions.isEmpty()) {
                        throw new DistributionZoneNotFoundException(zoneName);
                    }
                    assert zoneDefinitions.size() == 1;

                    return zoneDefinitions.get(0);
                })
                .thenCompose(
                        zoneDefinition -> new SelectFromView<>(sql, "ZONE_STORAGE_PROFILES", Option.zoneName(zoneName),
                                row -> row.stringValue("STORAGE_PROFILE"))
                                .executeAsync()
                                .thenApply(profiles -> zoneDefinition.toBuilder().storageProfiles(String.join(",", profiles)).build()));
    }

    @Override
    public ZoneDefinition zoneDefinition(String zoneName) {
        return join(zoneDefinitionAsync(zoneName));
    }

    @Override
    public CompletableFuture<Void> dropTableAsync(TableDefinition definition) {
        return new DropTableImpl(sql)
                .name(definition.schemaName(), definition.tableName())
                .ifExists()
                .executeAsync()
                .thenRun(() -> {});
    }

    @Override
    public CompletableFuture<Void> dropTableAsync(String name) {
        return new DropTableImpl(sql)
                .name(name)
                .ifExists()
                .executeAsync()
                .thenRun(() -> {});
    }

    @Override
    public void dropTable(TableDefinition definition) {
        join(dropTableAsync(definition));
    }

    @Override
    public void dropTable(String name) {
        join(dropTableAsync(name));
    }

    @Override
    public CompletableFuture<Void> dropZoneAsync(ZoneDefinition definition) {
        return new DropZoneImpl(sql)
                .name(definition.zoneName())
                .ifExists()
                .executeAsync()
                .thenRun(() -> {});
    }

    @Override
    public CompletableFuture<Void> dropZoneAsync(String name) {
        return new DropZoneImpl(sql)
                .name(name)
                .ifExists()
                .executeAsync()
                .thenRun(() -> {});
    }

    @Override
    public void dropZone(ZoneDefinition definition) {
        join(dropZoneAsync(definition));
    }

    @Override
    public void dropZone(String name) {
        join(dropZoneAsync(name));
    }

    private static ZoneDefinition toZoneDefinition(String zoneName, SqlRow row) {
        int partitions = row.intValue("PARTITIONS");
        int replicas = row.intValue("REPLICAS");
        int dataNodesAutoAdjustScaleUp = row.intValue("DATA_NODES_AUTO_ADJUST_SCALE_UP");
        int dataNodesAutoAdjustScaleDown = row.intValue("DATA_NODES_AUTO_ADJUST_SCALE_DOWN");
        String filter = row.stringValue("DATA_NODES_FILTER");

        return ZoneDefinition.builder(zoneName)
                .partitions(partitions)
                .replicas(replicas)
                .dataNodesAutoAdjustScaleUp(dataNodesAutoAdjustScaleUp)
                .dataNodesAutoAdjustScaleDown(dataNodesAutoAdjustScaleDown)
                .filter(filter)
                .build();
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

        public static Index create(String name, String type, String columns, boolean isPkIndex) {
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

    private static <R> R join(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(mapToPublicException(unwrapCause(e)));
        }
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
