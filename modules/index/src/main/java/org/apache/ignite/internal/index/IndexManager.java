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
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
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
public class IndexManager extends Producer<IndexEvent, IndexEventParameters> implements IgniteComponent {
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
        int indexId = parameters.indexId();
        int tableId = parameters.tableId();

        long causalityToken = parameters.causalityToken();

        if (!busyLock.enterBusy()) {
            fireEvent(IndexEvent.DROP,
                    new IndexEventParameters(causalityToken, tableId, indexId),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            return tableManager.tableAsync(causalityToken, parameters.tableId())
                    .thenCompose(table -> {
                        if (table != null) {
                            // In case of DROP TABLE the table will be removed first.
                            table.unregisterIndex(indexId);
                        }

                        return fireEvent(
                                IndexEvent.DROP,
                                new IndexEventParameters(causalityToken, tableId, indexId)
                        );
                    })
                    .thenApply(unused -> false);
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Boolean> onIndexCreate(CreateIndexEventParameters parameters) {
        CatalogIndexDescriptor index = parameters.indexDescriptor();

        long causalityToken = parameters.causalityToken();

        if (!busyLock.enterBusy()) {
            fireEvent(IndexEvent.CREATE,
                    new IndexEventParameters(causalityToken, index.tableId(), index.id()),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            CatalogTableDescriptor table = catalogManager.table(index.tableId(), parameters.catalogVersion());

            assert table != null : "tableId=" + index.tableId() + ", indexId=" + index.id();

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

        CompletableFuture<?> fireCreateIndexEventFuture = fireCreateIndexEvent(index, causalityToken, tableId);

        CompletableFuture<Void> registerIndexFuture = registerIndex(table, index, causalityToken, tableId);

        return allOf(fireCreateIndexEventFuture, registerIndexFuture);
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

    /**
     * Converts a catalog index descriptor to an event index descriptor.
     *
     * @param descriptor Catalog index descriptor.
     */
    private static IndexDescriptor toEventIndexDescriptor(CatalogIndexDescriptor descriptor) {
        if (descriptor instanceof CatalogHashIndexDescriptor) {
            return toEventHashIndexDescriptor(((CatalogHashIndexDescriptor) descriptor));
        }

        if (descriptor instanceof CatalogSortedIndexDescriptor) {
            return toEventSortedIndexDescriptor(((CatalogSortedIndexDescriptor) descriptor));
        }

        throw new IllegalArgumentException("Unknown index type: " + descriptor);
    }

    /**
     * Converts a catalog hash index descriptor to an event hash index descriptor.
     *
     * @param descriptor Catalog hash index descriptor.
     */
    private static IndexDescriptor toEventHashIndexDescriptor(CatalogHashIndexDescriptor descriptor) {
        return new IndexDescriptor(descriptor.name(), descriptor.columns());
    }

    /**
     * Converts a catalog sorted index descriptor to an event sorted index descriptor.
     *
     * @param descriptor Catalog sorted index descriptor.
     */
    private static SortedIndexDescriptor toEventSortedIndexDescriptor(CatalogSortedIndexDescriptor descriptor) {
        List<String> columns = new ArrayList<>(descriptor.columns().size());
        List<ColumnCollation> collations = new ArrayList<>(descriptor.columns().size());

        for (CatalogIndexColumnDescriptor column : descriptor.columns()) {
            columns.add(column.name());

            collations.add(toEventCollation(column.collation()));
        }

        return new SortedIndexDescriptor(descriptor.name(), columns, collations);
    }

    private static ColumnCollation toEventCollation(CatalogColumnCollation collation) {
        return ColumnCollation.get(collation.asc(), collation.nullsFirst());
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

            CompletableFuture<?> fireCreateIndexEventFuture = fireCreateIndexEvent(index, causalityToken, tableId);

            CompletableFuture<Void> registerIndexFuture = registerIndex(table, index, causalityToken, tableId);

            startIndexFutures.add(allOf(fireCreateIndexEventFuture, registerIndexFuture));
        }

        startVv.update(causalityToken, (unused, throwable) -> allOf(startIndexFutures.toArray(CompletableFuture[]::new)))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error starting indexes", throwable);
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Indexes started successfully");
                        }
                    }
                });
    }

    private CompletableFuture<Void> registerIndex(
            CatalogTableDescriptor table,
            CatalogIndexDescriptor index,
            long causalityToken,
            int configTableId
    ) {
        // TODO: IGNITE-19712 Listen to assignment changes and start new index storages.
        CompletableFuture<PartitionSet> tablePartitionFuture = tableManager.localPartitionSetAsync(causalityToken, configTableId);

        CompletableFuture<SchemaRegistry> schemaRegistryFuture = schemaManager.schemaRegistry(causalityToken, configTableId);

        return tablePartitionFuture.thenAcceptBoth(schemaRegistryFuture, (partitionSet, schemaRegistry) -> {
            TableImpl tableImpl = tableManager.getTable(configTableId);

            assert tableImpl != null : "tableId=" + configTableId + ", indexId=" + index.id();

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

    private CompletableFuture<?> fireCreateIndexEvent(
            CatalogIndexDescriptor index,
            long causalityToken,
            int configTableId
    ) {
        return fireEvent(
                IndexEvent.CREATE,
                new IndexEventParameters(causalityToken, configTableId, index.id(), toEventIndexDescriptor(index))
        );
    }
}
