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
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.QualifiedName;
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
    public CompletableFuture<TableDefinition> tableDefinitionAsync(QualifiedName tableName) {
        TableDefinitionCollector collector = new TableDefinitionCollector(tableName, sql);

        return collector.collectDefinition();
    }

    @Override
    public TableDefinition tableDefinition(QualifiedName tableName) {
        return join(tableDefinitionAsync(tableName));
    }

    @Override
    public CompletableFuture<Void> createZoneAsync(ZoneDefinition definition) {
        return new CreateFromDefinitionImpl(sql)
                .from(definition)
                .executeAsync()
                .thenApply(unused -> null);
    }

    @Override
    public void createZone(ZoneDefinition definition) {
        join(createZoneAsync(definition));
    }

    @Override
    public CompletableFuture<ZoneDefinition> zoneDefinitionAsync(String zoneName) {
        List<String> zoneViewColumns = List.of(
                "ZONE_PARTITIONS",
                "ZONE_REPLICAS",
                "ZONE_QUORUM_SIZE",
                "DATA_NODES_AUTO_ADJUST_SCALE_UP",
                "DATA_NODES_AUTO_ADJUST_SCALE_DOWN",
                "DATA_NODES_FILTER",
                "ZONE_CONSISTENCY_MODE"
        );
        return new SelectFromView<>(sql, zoneViewColumns, "ZONES", name(zoneName), row -> toZoneDefinitionBuilder(zoneName, row))
                .executeAsync()
                .thenApply(zoneDefinitions -> {
                    if (zoneDefinitions.isEmpty()) {
                        return null;
                    }
                    assert zoneDefinitions.size() == 1;

                    return zoneDefinitions.get(0);
                })
                .thenCompose(
                        zoneDefinition -> {
                            if (zoneDefinition == null) {
                                return CompletableFutures.nullCompletedFuture();
                            }
                            return new SelectFromView<>(sql,
                                    List.of("STORAGE_PROFILE"),
                                    "ZONE_STORAGE_PROFILES",
                                    Option.zoneName(zoneName),
                                    row -> row.stringValue("STORAGE_PROFILE")
                            ).executeAsync()
                                    .thenApply(profiles -> zoneDefinition.storageProfiles(String.join(", ", profiles)).build());
                        });
    }

    @Override
    public ZoneDefinition zoneDefinition(String zoneName) {
        return join(zoneDefinitionAsync(zoneName));
    }

    @Override
    public CompletableFuture<Void> dropTableAsync(TableDefinition definition) {
        return new DropTableImpl(sql)
                .name(definition.qualifiedName())
                .ifExists()
                .executeAsync()
                .thenApply(unused -> null);
    }

    @Override
    public CompletableFuture<Void> dropTableAsync(QualifiedName name) {
        return new DropTableImpl(sql)
                .name(name)
                .ifExists()
                .executeAsync()
                .thenApply(unused -> null);
    }

    @Override
    public void dropTable(TableDefinition definition) {
        join(dropTableAsync(definition));
    }

    @Override
    public void dropTable(QualifiedName name) {
        join(dropTableAsync(name));
    }

    @Override
    public CompletableFuture<Void> dropZoneAsync(ZoneDefinition definition) {
        return new DropZoneImpl(sql)
                .name(definition.zoneName())
                .ifExists()
                .executeAsync()
                .thenApply(unused -> null);
    }

    @Override
    public CompletableFuture<Void> dropZoneAsync(String name) {
        return new DropZoneImpl(sql)
                .name(name)
                .ifExists()
                .executeAsync()
                .thenApply(unused -> null);
    }

    @Override
    public void dropZone(ZoneDefinition definition) {
        join(dropZoneAsync(definition));
    }

    @Override
    public void dropZone(String name) {
        join(dropZoneAsync(name));
    }

    private static ZoneDefinition.Builder toZoneDefinitionBuilder(String zoneName, SqlRow row) {
        int partitions = row.intValue("ZONE_PARTITIONS");
        int replicas = row.intValue("ZONE_REPLICAS");
        int quorumsSize = row.intValue("ZONE_QUORUM_SIZE");
        int dataNodesAutoAdjustScaleUp = row.intValue("DATA_NODES_AUTO_ADJUST_SCALE_UP");
        int dataNodesAutoAdjustScaleDown = row.intValue("DATA_NODES_AUTO_ADJUST_SCALE_DOWN");
        String filter = row.stringValue("DATA_NODES_FILTER");
        String consistencyMode = row.stringValue("ZONE_CONSISTENCY_MODE");

        return ZoneDefinition.builder(zoneName)
                .partitions(partitions)
                .replicas(replicas)
                .quorumSize(quorumsSize)
                .dataNodesAutoAdjustScaleUp(dataNodesAutoAdjustScaleUp)
                .dataNodesAutoAdjustScaleDown(dataNodesAutoAdjustScaleDown)
                .filter(filter)
                .consistencyMode(consistencyMode);
    }

    private static <R> R join(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(mapToPublicException(unwrapCause(e)));
        }
    }
}
