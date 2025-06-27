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

package org.apache.ignite.internal.restart;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Table;

/**
 * Reference to {@link IgniteCatalog} under a swappable {@link Ignite} instance. When a restart happens, this switches to the new Ignite
 * instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
class RestartProofIgniteCatalog implements IgniteCatalog, Wrapper {
    private final IgniteAttachmentLock attachmentLock;

    RestartProofIgniteCatalog(IgniteAttachmentLock attachmentLock) {
        this.attachmentLock = attachmentLock;
    }

    @Override
    public CompletableFuture<Table> createTableAsync(Class<?> recordClass) {
        return attachmentLock.attachedAsync(ignite ->
                ignite.catalog().createTableAsync(recordClass)
                        .thenApply(table -> wrapTable(table, ignite))
        );
    }

    @Override
    public CompletableFuture<Table> createTableAsync(Class<?> keyClass, Class<?> valueClass) {
        return attachmentLock.attachedAsync(ignite ->
                ignite.catalog().createTableAsync(keyClass, valueClass)
                        .thenApply(table -> wrapTable(table, ignite))
        );
    }

    @Override
    public CompletableFuture<Table> createTableAsync(TableDefinition definition) {
        return attachmentLock.attachedAsync(ignite ->
                ignite.catalog().createTableAsync(definition)
                        .thenApply(table -> wrapTable(table, ignite))
        );
    }

    @Override
    public Table createTable(Class<?> recordClass) {
        return attachmentLock.attached(ignite -> {
            Table table = ignite.catalog().createTable(recordClass);
            return wrapTable(table, ignite);
        });
    }

    @Override
    public Table createTable(Class<?> keyClass, Class<?> valueClass) {
        return attachmentLock.attached(ignite -> {
            Table table = ignite.catalog().createTable(keyClass, valueClass);
            return wrapTable(table, ignite);
        });
    }

    @Override
    public Table createTable(TableDefinition definition) {
        return attachmentLock.attached(ignite -> {
            Table table = ignite.catalog().createTable(definition);
            return wrapTable(table, ignite);
        });
    }

    @Override
    public CompletableFuture<TableDefinition> tableDefinitionAsync(QualifiedName tableName) {
        return attachmentLock.attachedAsync(ignite -> ignite.catalog().tableDefinitionAsync(tableName));
    }

    @Override
    public TableDefinition tableDefinition(QualifiedName tableName) {
        return attachmentLock.attached(ignite -> ignite.catalog().tableDefinition(tableName));
    }

    private Table wrapTable(Table table, Ignite ignite) {
        return new RestartProofTable(attachmentLock, ignite, RestartProofTable.tableId(table));
    }

    @Override
    public CompletableFuture<Void> createZoneAsync(ZoneDefinition definition) {
        return attachmentLock.attachedAsync(ignite -> ignite.catalog().createZoneAsync(definition));
    }

    @Override
    public void createZone(ZoneDefinition definition) {
        attachmentLock.consumeAttached(ignite -> ignite.catalog().createZone(definition));
    }

    @Override
    public CompletableFuture<ZoneDefinition> zoneDefinitionAsync(String zoneName) {
        return attachmentLock.attachedAsync(ignite -> ignite.catalog().zoneDefinitionAsync(zoneName));
    }

    @Override
    public ZoneDefinition zoneDefinition(String zoneName) {
        return attachmentLock.attached(ignite -> ignite.catalog().zoneDefinition(zoneName));
    }

    @Override
    public CompletableFuture<Void> dropTableAsync(TableDefinition definition) {
        return attachmentLock.attachedAsync(ignite -> ignite.catalog().dropTableAsync(definition));
    }

    @Override
    public CompletableFuture<Void> dropTableAsync(QualifiedName name) {
        return attachmentLock.attachedAsync(ignite -> ignite.catalog().dropTableAsync(name));
    }

    @Override
    public void dropTable(TableDefinition definition) {
        attachmentLock.consumeAttached(ignite -> ignite.catalog().dropTable(definition));
    }

    @Override
    public void dropTable(QualifiedName name) {
        attachmentLock.consumeAttached(ignite -> ignite.catalog().dropTable(name));
    }

    @Override
    public CompletableFuture<Void> dropZoneAsync(ZoneDefinition definition) {
        return attachmentLock.attachedAsync(ignite -> ignite.catalog().dropZoneAsync(definition));
    }

    @Override
    public CompletableFuture<Void> dropZoneAsync(String name) {
        return attachmentLock.attachedAsync(ignite -> ignite.catalog().dropZoneAsync(name));
    }

    @Override
    public void dropZone(ZoneDefinition definition) {
        attachmentLock.consumeAttached(ignite -> ignite.catalog().dropZone(definition));
    }

    @Override
    public void dropZone(String name) {
        attachmentLock.consumeAttached(ignite -> ignite.catalog().dropZone(name));
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(ignite -> Wrappers.unwrap(ignite.catalog(), classToUnwrap));
    }
}
