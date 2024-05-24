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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_REMOVED;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.table.distributed.index.IndexUtils.registerIndexToTable;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.LongPriorityQueue;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * An Ignite component that is responsible for handling index-related commands like CREATE or DROP
 * as well as managing indexes' lifecycle.
 *
 * <p>To avoid errors when using indexes while applying replication log during node recovery, the registration of indexes was moved to the
 * start of the tables.</p>
 */
public class IndexManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexManager.class);

    /** Schema manager. */
    private final SchemaManager schemaManager;

    /** Table manager. */
    private final TableManager tableManager;

    /** Catalog service. */
    private final CatalogService catalogService;

    /** Separate executor for IO operations like storage initialization. */
    private final ExecutorService ioExecutor;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Versioned value to prevent races when registering/unregistering indexes when processing metastore or catalog events. */
    private final IncrementalVersionedValue<Void> handleMetastoreEventVv;

    /** Low watermark. */
    private final LowWatermark lowWatermark;

    /** A queue for deferred index destruction events. */
    private final LongPriorityQueue<DestroyIndexEvent> destructionEventsQueue =
            new LongPriorityQueue<>(DestroyIndexEvent::catalogVersion);

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
            ExecutorService ioExecutor,
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            LowWatermark lowWatermark
    ) {
        this.schemaManager = schemaManager;
        this.tableManager = tableManager;
        this.catalogService = catalogService;
        this.ioExecutor = ioExecutor;
        this.lowWatermark = lowWatermark;

        handleMetastoreEventVv = new IncrementalVersionedValue<>(registry);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        LOG.debug("Index manager is about to start");

        recoverDestructionQueue();

        catalogService.listen(INDEX_CREATE, (CreateIndexEventParameters parameters) -> onIndexCreate(parameters));
        catalogService.listen(INDEX_REMOVED, fromConsumer(this::onIndexRemoved));
        lowWatermark.listen(LOW_WATERMARK_CHANGED, parameters -> onLwmChanged((ChangeLowWatermarkEventParameters) parameters));

        LOG.info("Index manager started");

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        LOG.debug("Index manager is about to stop");

        if (!stopGuard.compareAndSet(false, true)) {
            LOG.debug("Index manager already stopped");

            return nullCompletedFuture();
        }

        busyLock.block();

        LOG.info("Index manager stopped");

        return nullCompletedFuture();
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
    CompletableFuture<@Nullable MvTableStorage> getMvTableStorage(long causalityToken, int tableId) {
        return tableManager.tableAsync(causalityToken, tableId).thenApply(table -> table == null ? null : table.internalTable().storage());
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

    private void onIndexRemoved(RemoveIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            int indexId = parameters.indexId();
            int catalogVersion = parameters.catalogVersion();
            int previousCatalogVersion = catalogVersion - 1;

            // Retrieve descriptor during synchronous call, before the previous catalog version could be concurrently compacted.
            CatalogIndexDescriptor indexDescriptor = catalogService.index(indexId, previousCatalogVersion);
            assert indexDescriptor != null : "indexId=" + indexId + ", catalogVersion=" + previousCatalogVersion;

            int tableId = indexDescriptor.tableId();

            if (catalogService.table(tableId, catalogVersion) == null) {
                // Nothing to do. Index will be destroyed along with the table.
                return;
            }

            destructionEventsQueue.enqueue(new DestroyIndexEvent(catalogVersion, indexId, tableId));
        });
    }

    private CompletableFuture<Boolean> onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int newEarliestCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

            List<DestroyIndexEvent> events = destructionEventsQueue.drainUpTo(newEarliestCatalogVersion);

            return runAsync(
                    () -> events.forEach(event -> destroyIndex(event.indexId(), event.tableId())),
                    ioExecutor
            ).thenApply(unused -> false);
        });
    }

    private void destroyIndex(int indexId, int tableId) {
        TableViewInternal table = tableManager.cachedTable(tableId);

        if (table != null) {
            // In case of DROP TABLE the table will be removed with all its indexes.
            table.unregisterIndex(indexId);
        }
    }

    private CompletableFuture<?> startIndexAsync(
            CatalogTableDescriptor table,
            CatalogIndexDescriptor index,
            long causalityToken
    ) {
        int tableId = index.tableId();

        CompletableFuture<PartitionSet> tablePartitionFuture = tableManager.localPartitionSetAsync(causalityToken, tableId);

        CompletableFuture<SchemaRegistry> schemaRegistryFuture = schemaManager.schemaRegistry(causalityToken, tableId);

        return handleMetastoreEventVv.update(
                causalityToken,
                updater(mvTableStorageById -> tablePartitionFuture.thenCombineAsync(schemaRegistryFuture,
                        (partitionSet, schemaRegistry) -> inBusyLock(busyLock, () -> {
                            registerIndex(table, index, partitionSet, schemaRegistry);

                            return null;
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

    /** Recover deferred destroy events. */
    private void recoverDestructionQueue() {
        // LWM starts updating only after the node is restored.
        HybridTimestamp lwm = lowWatermark.getLowWatermark();

        int earliestCatalogVersion = catalogService.activeCatalogVersion(hybridTimestampToLong(lwm));
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        for (int catalogVersion = latestCatalogVersion - 1; catalogVersion >= earliestCatalogVersion; catalogVersion--) {
            int nextVersion = catalogVersion + 1;
            catalogService.indexes(catalogVersion).stream()
                    .filter(idx -> catalogService.index(idx.id(), nextVersion) == null)
                    .forEach(idx -> destructionEventsQueue.enqueue(new DestroyIndexEvent(nextVersion, idx.id(), idx.tableId())));
        }
    }

    /** Internal event. */
    private static class DestroyIndexEvent {
        final int catalogVersion;
        final int indexId;
        final int tableId;

        DestroyIndexEvent(int catalogVersion, int indexId, int tableId) {
            this.catalogVersion = catalogVersion;
            this.indexId = indexId;
            this.tableId = tableId;
        }

        public int catalogVersion() {
            return catalogVersion;
        }

        public int indexId() {
            return indexId;
        }

        public int tableId() {
            return tableId;
        }
    }
}
