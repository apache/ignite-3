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

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.IgniteSql;
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

    private static <R> R join(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(mapToPublicException(unwrapCause(e)));
        }
    }
}
