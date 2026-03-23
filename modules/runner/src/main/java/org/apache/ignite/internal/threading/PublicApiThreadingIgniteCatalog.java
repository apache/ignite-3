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

package org.apache.ignite.internal.threading;

import static org.apache.ignite.internal.thread.PublicApiThreading.execUserAsyncOperation;
import static org.apache.ignite.internal.thread.PublicApiThreading.execUserSyncOperation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.table.distributed.PublicApiThreadingTable;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Table;

/**
 * Wrapper around {@link IgniteCatalog} that maintains public API invariants relating to threading.
 * That is, it adds protection against thread hijacking by users and also marks threads as 'executing a sync user operation' or
 * 'executing an async user operation'.
 */
public class PublicApiThreadingIgniteCatalog implements IgniteCatalog, Wrapper {
    private final IgniteCatalog catalog;
    private final Executor asyncContinuationExecutor;

    public PublicApiThreadingIgniteCatalog(IgniteCatalog catalog, Executor asyncContinuationExecutor) {
        this.catalog = catalog;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public CompletableFuture<Table> createTableAsync(Class<?> recordClass) {
        return doAsyncOperationForTable(() -> catalog.createTableAsync(recordClass));
    }

    @Override
    public CompletableFuture<Table> createTableAsync(Class<?> keyClass, Class<?> valueClass) {
        return doAsyncOperationForTable(() -> catalog.createTableAsync(keyClass, valueClass));
    }

    @Override
    public CompletableFuture<Table> createTableAsync(TableDefinition definition) {
        return doAsyncOperationForTable(() -> catalog.createTableAsync(definition));
    }

    @Override
    public Table createTable(Class<?> recordClass) {
        Table table = execUserSyncOperation(() -> catalog.createTable(recordClass));
        return wrapTableForPublicUse(table);
    }

    @Override
    public Table createTable(Class<?> keyClass, Class<?> valueClass) {
        Table table = execUserSyncOperation(() -> catalog.createTable(keyClass, valueClass));
        return wrapTableForPublicUse(table);
    }

    @Override
    public Table createTable(TableDefinition definition) {
        Table table = execUserSyncOperation(() -> catalog.createTable(definition));
        return wrapTableForPublicUse(table);
    }

    @Override
    public CompletableFuture<TableDefinition> tableDefinitionAsync(QualifiedName tableName) {
        return doAsyncOperation(() -> catalog.tableDefinitionAsync(tableName));
    }

    @Override
    public TableDefinition tableDefinition(QualifiedName tableName) {
        return execUserSyncOperation(() -> catalog.tableDefinition(tableName));
    }

    @Override
    public CompletableFuture<Void> createZoneAsync(ZoneDefinition definition) {
        return doAsyncOperation(() -> catalog.createZoneAsync(definition));
    }

    @Override
    public void createZone(ZoneDefinition definition) {
        execUserSyncOperation(() -> catalog.createZone(definition));
    }

    @Override
    public CompletableFuture<ZoneDefinition> zoneDefinitionAsync(String zoneName) {
        return execUserSyncOperation(() -> catalog.zoneDefinitionAsync(zoneName));
    }

    @Override
    public ZoneDefinition zoneDefinition(String zoneName) {
        return execUserSyncOperation(() -> catalog.zoneDefinition(zoneName));
    }

    @Override
    public CompletableFuture<Void> dropTableAsync(TableDefinition definition) {
        return doAsyncOperation(() -> catalog.dropTableAsync(definition));
    }

    @Override
    public CompletableFuture<Void> dropTableAsync(QualifiedName name) {
        return doAsyncOperation(() -> catalog.dropTableAsync(name));
    }

    @Override
    public void dropTable(TableDefinition definition) {
        execUserSyncOperation(() -> catalog.dropTable(definition));
    }

    @Override
    public void dropTable(QualifiedName name) {
        execUserSyncOperation(() -> catalog.dropTable(name));
    }

    @Override
    public CompletableFuture<Void> dropZoneAsync(ZoneDefinition definition) {
        return doAsyncOperation(() -> catalog.dropZoneAsync(definition));
    }

    @Override
    public CompletableFuture<Void> dropZoneAsync(String name) {
        return doAsyncOperation(() -> catalog.dropZoneAsync(name));
    }

    @Override
    public void dropZone(ZoneDefinition definition) {
        execUserSyncOperation(() -> catalog.dropZone(definition));
    }

    @Override
    public void dropZone(String name) {
        execUserSyncOperation(() -> catalog.dropZone(name));
    }

    private CompletableFuture<Table> doAsyncOperationForTable(Supplier<CompletableFuture<Table>> operation) {
        return doAsyncOperation(operation).thenApply(this::wrapTableForPublicUse);
    }

    private <T> CompletableFuture<T> doAsyncOperation(Supplier<CompletableFuture<T>> operation) {
        CompletableFuture<T> future = execUserAsyncOperation(operation);
        return PublicApiThreading.preventThreadHijack(future, asyncContinuationExecutor);
    }

    private PublicApiThreadingTable wrapTableForPublicUse(Table table) {
        return new PublicApiThreadingTable(table, asyncContinuationExecutor);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(catalog);
    }
}
