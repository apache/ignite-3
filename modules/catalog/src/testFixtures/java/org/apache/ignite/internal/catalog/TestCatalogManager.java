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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.ReverseIterator;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of the {@link CatalogManager} which under the hood uses {@link CatalogManagerImpl}.
 *
 * <p>Available to use in two modes:</p>
 * <ul>
 *     <li>With the use of {@link #TestCatalogManager(String, HybridClock, VaultManager, MetaStorageManager) external components}, then
 *     only {@link ClockWaiter} and {@link CatalogManagerImpl} will be created inside, and {@link #start()}, {@link #beforeNodeStop()} and
 *     {@link #stop()} will need to be used for internal components;</li>
 *     <li>{@link #TestCatalogManager(String, HybridClock) Creating all the necessary components inside} then it will be necessary to use
 *     {@link #startAllAndWithDeployWatches()}, {@link #beforeNodeStopAll()} and {@link #stopAll()}.</li>
 * </ul>
 */
public class TestCatalogManager implements CatalogManager {
    private final VaultManager vault;

    private final MetaStorageManager metastore;

    private final ClockWaiter clockWaiter;

    private final CatalogManager catalogManager;

    /**
     * Constructor within which all the necessary components are created, it will be necessary to use
     * {@link #startAllAndWithDeployWatches()}, {@link #beforeNodeStopAll()} and {@link #stopAll()}.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     */
    public TestCatalogManager(String nodeName, HybridClock clock) {
        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage(nodeName));

        clockWaiter = new ClockWaiter(nodeName, clock);

        catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter);
    }

    /**
     * Constructor with external components, it will be necessary to use {@link #start()}, {@link #beforeNodeStop()} and {@link #stop()}.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     * @param vault External vault manager.
     * @param metastore External meta storage manager.
     */
    public TestCatalogManager(String nodeName, HybridClock clock, VaultManager vault, MetaStorageManager metastore) {
        this.vault = vault;
        this.metastore = metastore;

        clockWaiter = new ClockWaiter(nodeName, clock);

        catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter);
    }

    @Override
    public CompletableFuture<Void> createTable(CreateTableParams params) {
        return catalogManager.createTable(params);
    }

    @Override
    public CompletableFuture<Void> dropTable(DropTableParams params) {
        return catalogManager.dropTable(params);
    }

    @Override
    public CompletableFuture<Void> addColumn(AlterTableAddColumnParams params) {
        return catalogManager.addColumn(params);
    }

    @Override
    public CompletableFuture<Void> dropColumn(AlterTableDropColumnParams params) {
        return catalogManager.dropColumn(params);
    }

    @Override
    public CompletableFuture<Void> alterColumn(AlterColumnParams params) {
        return catalogManager.alterColumn(params);
    }

    @Override
    public CompletableFuture<Void> createIndex(CreateSortedIndexParams params) {
        return catalogManager.createIndex(params);
    }

    @Override
    public CompletableFuture<Void> createIndex(CreateHashIndexParams params) {
        return catalogManager.createIndex(params);
    }

    @Override
    public CompletableFuture<Void> dropIndex(DropIndexParams params) {
        return catalogManager.dropIndex(params);
    }

    @Override
    public CompletableFuture<Void> createZone(CreateZoneParams params) {
        return catalogManager.createZone(params);
    }

    @Override
    public CompletableFuture<Void> dropZone(DropZoneParams params) {
        return catalogManager.dropZone(params);
    }

    @Override
    public CompletableFuture<Void> alterZone(AlterZoneParams params) {
        return catalogManager.alterZone(params);
    }

    @Override
    public CompletableFuture<Void> renameZone(RenameZoneParams params) {
        return catalogManager.renameZone(params);
    }

    @Override
    public @Nullable CatalogTableDescriptor table(String tableName, long timestamp) {
        return catalogManager.table(tableName, timestamp);
    }

    @Override
    public @Nullable CatalogTableDescriptor table(int tableId, long timestamp) {
        return catalogManager.table(tableId, timestamp);
    }

    @Override
    public @Nullable CatalogTableDescriptor table(int tableId, int catalogVersion) {
        return catalogManager.table(tableId, catalogVersion);
    }

    @Override
    public Collection<CatalogTableDescriptor> tables(int catalogVersion) {
        return catalogManager.tables(catalogVersion);
    }

    @Override
    public @Nullable CatalogIndexDescriptor index(String indexName, long timestamp) {
        return catalogManager.index(indexName, timestamp);
    }

    @Override
    public @Nullable CatalogIndexDescriptor index(int indexId, long timestamp) {
        return catalogManager.index(indexId, timestamp);
    }

    @Override
    public @Nullable CatalogIndexDescriptor index(int indexId, int catalogVersion) {
        return catalogManager.index(indexId, catalogVersion);
    }

    @Override
    public Collection<CatalogIndexDescriptor> indexes(int catalogVersion) {
        return catalogManager.indexes(catalogVersion);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor schema(int version) {
        return catalogManager.schema(version);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor schema(@Nullable String schemaName, int version) {
        return catalogManager.schema(schemaName, version);
    }

    @Override
    public CatalogZoneDescriptor zone(String zoneName, long timestamp) {
        return catalogManager.zone(zoneName, timestamp);
    }

    @Override
    public CatalogZoneDescriptor zone(int zoneId, long timestamp) {
        return catalogManager.zone(zoneId, timestamp);
    }

    @Override
    public @Nullable CatalogZoneDescriptor zone(int zoneId, int catalogVersion) {
        return catalogManager.zone(zoneId, catalogVersion);
    }

    @Override
    public Collection<CatalogZoneDescriptor> zones(int catalogVersion) {
        return catalogManager.zones(catalogVersion);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor activeSchema(long timestamp) {
        return catalogManager.activeSchema(timestamp);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor activeSchema(@Nullable String schemaName, long timestamp) {
        return catalogManager.activeSchema(schemaName, timestamp);
    }

    @Override
    public int activeCatalogVersion(long timestamp) {
        return catalogManager.activeCatalogVersion(timestamp);
    }

    @Override
    public int latestCatalogVersion() {
        return catalogManager.latestCatalogVersion();
    }

    @Override
    public CompletableFuture<Void> catalogReadyFuture(int version) {
        return catalogManager.catalogReadyFuture(version);
    }

    @Override
    public void listen(CatalogEvent evt, EventListener<? extends CatalogEventParameters> closure) {
        catalogManager.listen(evt, closure);
    }

    @Override
    public void start() {
        List.of(clockWaiter, catalogManager).forEach(IgniteComponent::start);
    }

    /** Starts all components and {@link MetaStorageManager#deployWatches()}. */
    public void startAllAndWithDeployWatches() {
        allComponents().forEach(IgniteComponent::start);

        assertThat(metastore.deployWatches(), willCompleteSuccessfully());
    }

    @Override
    public void beforeNodeStop() {
        List.of(catalogManager, clockWaiter).forEach(IgniteComponent::beforeNodeStop);
    }

    /** Calls {@link IgniteComponent#beforeNodeStop()} on all components. */
    public void beforeNodeStopAll() {
        new ReverseIterator<>(allComponents()).forEachRemaining(IgniteComponent::beforeNodeStop);
    }

    @Override
    public void stop() throws Exception {
        IgniteUtils.stopAll(clockWaiter, catalogManager);
    }

    /** Stops all components. */
    public void stopAll() throws Exception {
        Iterator<IgniteComponent> iterator = new ReverseIterator<>(allComponents());

        Spliterator<IgniteComponent> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE);

        IgniteUtils.stopAll(StreamSupport.stream(spliterator, false));
    }

    private List<IgniteComponent> allComponents() {
        return List.of(vault, metastore, clockWaiter, catalogManager);
    }
}
