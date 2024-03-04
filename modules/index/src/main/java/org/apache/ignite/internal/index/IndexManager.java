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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_DESTROY;
import static org.apache.ignite.internal.table.distributed.index.IndexUtils.registerIndexToTable;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DestroyIndexEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * An Ignite component that is responsible for handling index-related commands like CREATE or DROP
 * as well as managing indexes' lifecycle.
 */
// TODO: IGNITE-19082 Delete this class
public class IndexManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexManager.class);

    /** Schema manager. */
    private final SchemaManager schemaManager;

    /** Table manager. */
    private final TableManager tableManager;

    /** Catalog service. */
    private final CatalogService catalogService;

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Separate executor for IO operations like storage initialization. */
    private final ExecutorService ioExecutor;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Versioned value used only at the start of the manager. */
    private final IncrementalVersionedValue<Void> startVv;

    /** Version value of multi-version table storages by ID for which indexes were created. */
    private final IncrementalVersionedValue<Int2ObjectMap<MvTableStorage>> mvTableStoragesByIdVv;

    /**
     * Constructor.
     *
     * @param schemaManager Schema manager.
     * @param tableManager Table manager.
     * @param catalogService Catalog service.
     * @param ioExecutor Separate executor for IO operations like storage initialization.
     */
    public IndexManager(
            SchemaManager schemaManager,
            TableManager tableManager,
            CatalogService catalogService,
            MetaStorageManager metaStorageManager,
            ExecutorService ioExecutor,
            Consumer<LongFunction<CompletableFuture<?>>> registry
    ) {
        this.schemaManager = schemaManager;
        this.tableManager = tableManager;
        this.catalogService = catalogService;
        this.metaStorageManager = metaStorageManager;
        this.ioExecutor = ioExecutor;

        startVv = new IncrementalVersionedValue<>(registry);
        mvTableStoragesByIdVv = new IncrementalVersionedValue<>(registry, Int2ObjectMaps::emptyMap);
    }

    @Override
    public CompletableFuture<Void> start() {
        LOG.debug("Index manager is about to start");

        // TODO: IGNITE-21635 По идее нам уже не нужно, тейбл менеджер будет сам делать на старте и на ребалансе надо тоже
        // startIndexes();

        catalogService.listen(INDEX_CREATE, (CreateIndexEventParameters parameters) -> onIndexCreate(parameters));
        catalogService.listen(INDEX_DESTROY, (DestroyIndexEventParameters parameters) -> onIndexDestroy(parameters));

        LOG.info("Index manager started");

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        LOG.debug("Index manager is about to stop");

        if (!stopGuard.compareAndSet(false, true)) {
            LOG.debug("Index manager already stopped");

            return;
        }

        busyLock.block();

        LOG.info("Index manager stopped");
    }

    /**
     * Returns a multi-version table storage with created index storages by passed parameters.
     *
     * <p>Example: when we start building an index, we will need {@link IndexStorage} (as well as storage {@link MvPartitionStorage}) to
     * build it and we can get them in {@link CatalogEvent#INDEX_CREATE} using this method.</p>
     *
     * <p>During recovery, it is important to wait until the local node becomes a primary replica so that all index building commands are
     * applied from the replication log.</p>
     *
     * @param causalityToken Causality token.
     * @param tableId Table ID.
     * @return Future with multi-version table storage, completes with {@code null} if the table does not exist according to the passed
     *      parameters.
     */
    CompletableFuture<MvTableStorage> getMvTableStorage(long causalityToken, int tableId) {
        return mvTableStoragesByIdVv.get(causalityToken).thenApply(mvTableStoragesById -> mvTableStoragesById.get(tableId));
    }

    private CompletableFuture<Boolean> onIndexDestroy(DestroyIndexEventParameters parameters) {
        int indexId = parameters.indexId();
        int tableId = parameters.tableId();

        long causalityToken = parameters.causalityToken();

        CompletableFuture<TableViewInternal> tableFuture = tableManager.tableAsync(causalityToken, tableId);

        return inBusyLockAsync(busyLock, () -> mvTableStoragesByIdVv.update(
                causalityToken,
                updater(mvTableStorageById -> tableFuture.thenApply(table -> inBusyLock(busyLock, () -> {
                    if (table != null) {
                        // In case of DROP TABLE the table will be removed first.
                        table.unregisterIndex(indexId);

                        return mvTableStorageById;
                    } else {
                        return removeMvTableStorageIfPresent(mvTableStorageById, tableId);
                    }
                })))
        )).thenApply(unused -> false);
    }

    private CompletableFuture<Boolean> onIndexCreate(CreateIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            CatalogIndexDescriptor index = parameters.indexDescriptor();

            int indexId = index.id();
            int tableId = index.tableId();

            long causalityToken = parameters.causalityToken();
            int catalogVersion = parameters.catalogVersion();

            CatalogTableDescriptor table = catalogService.table(tableId, catalogVersion);

            assert table != null : "tableId=" + tableId + ", indexId=" + indexId;

            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Creating local index: name={}, id={}, tableId={}, token={}",
                        index.name(), indexId, tableId, causalityToken
                );
            }

            return startIndexAsync(table, index, causalityToken).thenApply(unused -> false);
        });
    }

    @SuppressWarnings("unused")
    private void startIndexes() {
        CompletableFuture<Long> recoveryFinishedFuture = metaStorageManager.recoveryFinishedFuture();

        assert recoveryFinishedFuture.isDone();

        long causalityToken = recoveryFinishedFuture.join();

        List<CompletableFuture<?>> startIndexFutures = new ArrayList<>();

        acceptAliveIndexes(catalogService, (table, index) -> startIndexFutures.add(startIndexAsync(table, index, causalityToken)));

        // Forces to wait until recovery is complete before the metastore watches are deployed to avoid races with other components.
        startVv.update(causalityToken, (unused, throwable) -> allOf(startIndexFutures.toArray(CompletableFuture[]::new)))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error starting indexes", throwable);
                    } else {
                        LOG.info("Indexes started successfully");
                    }
                });
    }

    private CompletableFuture<?> startIndexAsync(
            CatalogTableDescriptor table,
            CatalogIndexDescriptor index,
            long causalityToken
    ) {
        int tableId = index.tableId();

        // TODO: IGNITE-19712 Listen to assignment changes and start new index storages.
        CompletableFuture<PartitionSet> tablePartitionFuture = tableManager.localPartitionSetAsync(causalityToken, tableId);

        CompletableFuture<SchemaRegistry> schemaRegistryFuture = schemaManager.schemaRegistry(causalityToken, tableId);

        return mvTableStoragesByIdVv.update(
                causalityToken,
                updater(mvTableStorageById -> tablePartitionFuture.thenCombineAsync(schemaRegistryFuture,
                        (partitionSet, schemaRegistry) -> inBusyLock(busyLock, () -> {
                            registerIndex(table, index, partitionSet, schemaRegistry);

                            return addMvTableStorageIfAbsent(mvTableStorageById, getTableViewStrict(tableId).internalTable().storage());
                        }), ioExecutor))
        );
    }

    private void registerIndex(
            CatalogTableDescriptor table,
            CatalogIndexDescriptor index,
            PartitionSet partitionSet,
            SchemaRegistry schemaRegistry
    ) {
        TableViewInternal tableView = getTableViewStrict(table.id());

        registerIndexToTable(tableView, table, index, partitionSet, schemaRegistry);
    }

    private static Int2ObjectMap<MvTableStorage> addMvTableStorageIfAbsent(
            Int2ObjectMap<MvTableStorage> mvTableStorageById,
            MvTableStorage mvTableStorage
    ) {
        int tableId = mvTableStorage.getTableDescriptor().getId();

        if (mvTableStorageById.containsKey(tableId)) {
            return mvTableStorageById;
        }

        Int2ObjectMap<MvTableStorage> newMap = new Int2ObjectOpenHashMap<>(mvTableStorageById);

        newMap.put(tableId, mvTableStorage);

        return newMap;
    }

    private static Int2ObjectMap<MvTableStorage> removeMvTableStorageIfPresent(
            Int2ObjectMap<MvTableStorage> mvTableStorageById,
            int tableId
    ) {
        if (!mvTableStorageById.containsKey(tableId)) {
            return mvTableStorageById;
        }

        Int2ObjectMap<MvTableStorage> newMap = new Int2ObjectOpenHashMap<>(mvTableStorageById);

        newMap.remove(tableId);

        return newMap;
    }

    private TableViewInternal getTableViewStrict(int tableId) {
        TableViewInternal table = tableManager.cachedTable(tableId);

        assert table != null : tableId;

        return table;
    }

    private static <T> BiFunction<T, Throwable, CompletableFuture<T>> updater(Function<T, CompletableFuture<T>> updateFunction) {
        return (t, throwable) -> {
            if (throwable != null) {
                return failedFuture(throwable);
            }

            return updateFunction.apply(t);
        };
    }

    /**
     * Collects indexes (including deleted ones) for tables (tables from the latest version of the catalog) from the earliest to the latest
     * version of the catalog that need to be started on node recovery.
     *
     * @param catalogService Catalog service.
     */
    static void acceptAliveIndexes(CatalogService catalogService, BiConsumer<CatalogTableDescriptor, CatalogIndexDescriptor> consumer) {
        int earliestCatalogVersion = catalogService.earliestCatalogVersion();
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        IntSet processedObjects = new IntOpenHashSet();
        catalogService.indexes(latestCatalogVersion).stream()
                .filter(idx -> processedObjects.add(idx.id()))
                .forEach(idx -> consumer.accept(catalogService.table(idx.tableId(), latestCatalogVersion), idx));

        for (int ver = earliestCatalogVersion; ver < latestCatalogVersion; ver++) {
            int ver0 = ver;
            catalogService.indexes(ver).stream()
                    .filter(idx -> idx.status() == CatalogIndexStatus.AVAILABLE)
                    .filter(idx -> processedObjects.add(idx.id()))
                    .forEach(idx -> consumer.accept(catalogService.table(idx.tableId(), ver0), idx));
        }
    }
}
