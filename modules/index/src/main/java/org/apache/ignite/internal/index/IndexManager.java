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
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_DROP;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.CatalogSchemaManager;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor.StorageColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * An Ignite component that is responsible for handling index-related commands like CREATE or DROP
 * as well as managing indexes' lifecycle.
 */
// TODO: IGNITE-19082 Delete this class
public class IndexManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexManager.class);

    /** Schema manager. */
    private final CatalogSchemaManager schemaManager;

    /** Table manager. */
    private final TableManager tableManager;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Versioned value used only at the start of the manager. */
    private final IncrementalVersionedValue<Void> startVv;

    /**
     * Constructor.
     *
     * @param schemaManager Schema manager.
     * @param tableManager Table manager.
     * @param catalogManager Catalog manager.
     */
    public IndexManager(
            CatalogSchemaManager schemaManager,
            TableManager tableManager,
            CatalogManager catalogManager,
            MetaStorageManager metaStorageManager,
            Consumer<LongFunction<CompletableFuture<?>>> registry
    ) {
        this.schemaManager = schemaManager;
        this.tableManager = tableManager;
        this.catalogManager = catalogManager;
        this.metaStorageManager = metaStorageManager;

        startVv = new IncrementalVersionedValue<>(registry);
    }

    @Override
    public void start() {
        LOG.debug("Index manager is about to start");

        startIndexes();

        catalogManager.listen(INDEX_CREATE, (parameters, exception) -> {
            assert exception == null : parameters;

            return onIndexCreate((CreateIndexEventParameters) parameters);
        });

        catalogManager.listen(INDEX_DROP, (parameters, exception) -> {
            assert exception == null;

            return onIndexDrop((DropIndexEventParameters) parameters);
        });

        LOG.info("Index manager started");
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

    private CompletableFuture<Boolean> onIndexDrop(DropIndexEventParameters parameters) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        int indexId = parameters.indexId();

        try {
            return tableManager.tableAsync(parameters.causalityToken(), parameters.tableId())
                    .thenApply(table -> {
                        if (table != null) {
                            // In case of DROP TABLE the table will be removed first.
                            table.unregisterIndex(indexId);
                        }

                        return false;
                    });
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Boolean> onIndexCreate(CreateIndexEventParameters parameters) {
        CatalogIndexDescriptor index = parameters.indexDescriptor();

        int indexId = index.id();
        int tableId = index.tableId();

        long causalityToken = parameters.causalityToken();
        int catalogVersion = parameters.catalogVersion();

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            CatalogTableDescriptor table = catalogManager.table(tableId, catalogVersion);

            assert table != null : "tableId=" + tableId + ", indexId=" + indexId;

            return createIndexLocally(causalityToken, table, index).thenApply(unused -> false);
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> createIndexLocally(
            long causalityToken,
            CatalogTableDescriptor table,
            CatalogIndexDescriptor index
    ) {
        int tableId = table.id();
        int indexId = index.id();

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Creating local index: name={}, id={}, tableId={}, token={}",
                    index.name(), indexId, tableId, causalityToken
            );
        }

        return registerIndex(table, index, causalityToken);
    }

    /**
     * This class encapsulates the logic of conversion from table row to a particular index key.
     */
    private static class TableRowToIndexKeyConverter implements ColumnsExtractor {
        private final SchemaRegistry registry;
        private final String[] indexedColumns;
        private final Object mutex = new Object();

        private volatile VersionedConverter converter = new VersionedConverter(-1, null);

        TableRowToIndexKeyConverter(SchemaRegistry registry, String[] indexedColumns) {
            this.registry = registry;
            this.indexedColumns = indexedColumns;
        }

        @Override
        public BinaryTuple extractColumns(BinaryRow row) {
            return converter(row).extractColumns(row);
        }

        private ColumnsExtractor converter(BinaryRow row) {
            int schemaVersion = row.schemaVersion();

            VersionedConverter converter = this.converter;

            if (converter.version != schemaVersion) {
                synchronized (mutex) {
                    converter = this.converter;

                    if (converter.version != schemaVersion) {
                        converter = createConverter(schemaVersion);

                        this.converter = converter;
                    }
                }
            }

            return converter;
        }

        /** Creates converter for given version of the schema. */
        private VersionedConverter createConverter(int schemaVersion) {
            SchemaDescriptor descriptor = registry.schema();

            if (descriptor.version() < schemaVersion) {
                registry.waitLatestSchema();
            }

            if (descriptor.version() != schemaVersion) {
                descriptor = registry.schema(schemaVersion);
            }

            int[] indexedColumns = resolveColumnIndexes(descriptor);

            var rowConverter = BinaryRowConverter.columnsExtractor(descriptor, indexedColumns);

            return new VersionedConverter(descriptor.version(), rowConverter);
        }

        private int[] resolveColumnIndexes(SchemaDescriptor descriptor) {
            int[] result = new int[indexedColumns.length];

            for (int i = 0; i < indexedColumns.length; i++) {
                Column column = descriptor.column(indexedColumns[i]);

                assert column != null : indexedColumns[i];

                result[i] = column.schemaIndex();
            }

            return result;
        }

        /**
         * Convenient wrapper which glues together a function which actually converts one row to another,
         * and a version of the schema the function was build upon.
         */
        private static class VersionedConverter implements ColumnsExtractor {
            private final int version;
            private final ColumnsExtractor delegate;

            private VersionedConverter(int version, ColumnsExtractor delegate) {
                this.version = version;
                this.delegate = delegate;
            }

            @Override
            public BinaryTuple extractColumns(BinaryRow row) {
                return delegate.extractColumns(row);
            }
        }
    }

    private void startIndexes() {
        CompletableFuture<Long> recoveryFinishedFuture = metaStorageManager.recoveryFinishedFuture();

        assert recoveryFinishedFuture.isDone();

        int catalogVersion = catalogManager.latestCatalogVersion();
        long causalityToken = recoveryFinishedFuture.join();

        List<CompletableFuture<?>> startIndexFutures = new ArrayList<>();

        for (CatalogIndexDescriptor index : catalogManager.indexes(catalogVersion)) {
            int tableId = index.tableId();

            CatalogTableDescriptor table = catalogManager.table(tableId, catalogVersion);

            assert table != null : "tableId=" + tableId + ", indexId=" + index.id();

            CompletableFuture<Void> registerIndexFuture = registerIndex(table, index, causalityToken);

            startIndexFutures.add(registerIndexFuture);
        }

        startVv.update(causalityToken, (unused, throwable) -> allOf(startIndexFutures.toArray(CompletableFuture[]::new)))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error starting indexes", throwable);
                    } else {
                        LOG.debug("Indexes started successfully");
                    }
                });
    }

    private CompletableFuture<Void> registerIndex(
            CatalogTableDescriptor table,
            CatalogIndexDescriptor index,
            long causalityToken
    ) {
        int tableId = index.tableId();

        // TODO: IGNITE-19712 Listen to assignment changes and start new index storages.
        CompletableFuture<PartitionSet> tablePartitionFuture = tableManager.localPartitionSetAsync(causalityToken, tableId);

        CompletableFuture<SchemaRegistry> schemaRegistryFuture = schemaManager.schemaRegistry(causalityToken, tableId);

        return tablePartitionFuture.thenAcceptBoth(schemaRegistryFuture, (partitionSet, schemaRegistry) -> {
            TableImpl tableImpl = tableManager.getTable(tableId);

            assert tableImpl != null : "tableId=" + tableId + ", indexId=" + index.id();

            var storageIndexDescriptor = StorageIndexDescriptor.create(table, index);

            TableRowToIndexKeyConverter tableRowConverter = new TableRowToIndexKeyConverter(
                    schemaRegistry,
                    storageIndexDescriptor.columns().stream().map(StorageColumnDescriptor::name).toArray(String[]::new)
            );

            if (storageIndexDescriptor instanceof StorageSortedIndexDescriptor) {
                tableImpl.registerSortedIndex(
                        (StorageSortedIndexDescriptor) storageIndexDescriptor,
                        tableRowConverter,
                        partitionSet
                );
            } else {
                boolean unique = index.unique();

                tableImpl.registerHashIndex(
                        (StorageHashIndexDescriptor) storageIndexDescriptor,
                        unique,
                        tableRowConverter,
                        partitionSet
                );

                if (unique) {
                    tableImpl.pkId(index.id());
                }
            }
        });
    }
}
