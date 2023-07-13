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
import static org.apache.ignite.internal.util.ArrayUtils.STRING_EMPTY_ARRAY;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.AbstractIndexCommandParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * An Ignite component that is responsible for handling index-related commands like CREATE or DROP
 * as well as managing indexes' lifecycle.
 */
// TODO: IGNITE-19082 Delete this class
public class IndexManager extends Producer<IndexEvent, IndexEventParameters> implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexManager.class);

    /** Schema manager. */
    private final SchemaManager schemaManager;

    /** Table manager. */
    private final TableManager tableManager;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param catalogManager Catalog manager.
     * @param schemaManager Schema manager.
     * @param tableManager Table manager.
     */
    public IndexManager(
            CatalogManager catalogManager,
            SchemaManager schemaManager,
            TableManager tableManager
    ) {
        this.schemaManager = Objects.requireNonNull(schemaManager, "schemaManager");
        this.tableManager = tableManager;
        this.catalogManager = catalogManager;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        LOG.debug("Index manager is about to start");

        catalogManager.listen(CatalogEvent.INDEX_CREATE,
                (params, ex) -> onIndexCreate((CreateIndexEventParameters) params).thenApply(ignore -> false));
        catalogManager.listen(CatalogEvent.INDEX_DROP,
                (params, ex) -> onIndexDrop((DropIndexEventParameters) params).thenApply(ignore -> false));

        LOG.info("Index manager started");
    }

    /** {@inheritDoc} */
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
     * Creates index from provided parameters.
     */
    public CompletableFuture<Boolean> createIndexAsync(AbstractIndexCommandParams params) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            if (StringUtils.nullOrEmpty(params.indexName())) {
                return failedFuture(new IgniteInternalException(
                        ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                        "Index name should be at least 1 character long"
                ));
            }

            CompletableFuture<Void> indexCreateFuture = (params instanceof CreateSortedIndexParams)
                    ? catalogManager.createIndex((CreateSortedIndexParams) params)
                    : catalogManager.createIndex((CreateHashIndexParams) params);

            return indexCreateFuture.thenApply(ignore -> catalogManager.index(params.indexName(), Long.MAX_VALUE))
                    .thenApply(ignore -> false);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops the index with a given parameters asynchronously.
     */
    public CompletableFuture<Boolean> dropIndexAsync(DropIndexParams params) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return catalogManager.dropIndex(params)
                    .thenApply(ignore -> false);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Callback method is called when index configuration changed and an index was dropped.
     *
     * @param evt Index configuration event.
     * @return A future.
     */
    private CompletableFuture<?> onIndexDrop(DropIndexEventParameters evt) {
        int idxId = evt.indexId();
        int tableId = evt.tableId();
        long causalityToken = evt.causalityToken();

        if (!busyLock.enterBusy()) {
            fireEvent(IndexEvent.DROP,
                    new IndexEventParameters(causalityToken, tableId, idxId),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            CompletableFuture<?> fireEventFuture = fireEvent(IndexEvent.DROP, new IndexEventParameters(causalityToken, tableId, idxId));

            catalogManager.table(tableId, evt.catalogVersion());
            CompletableFuture<?> dropIndexFuture = tableManager.tableAsync(tableId)
                    .thenAccept(table -> {
                        if (table != null) { // in case of DROP TABLE the table will be removed first
                            table.unregisterIndex(idxId);
                        }
                    });

            // TODO: investigate why DropIndexFuture hangs.
            return allOf(fireEventFuture, dropIndexFuture);
        } catch (Throwable th) {
            LOG.warn("Failed to process drop index event.", th);

            return failedFuture(th);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Callback method triggers when index configuration changed and a new index was added.
     *
     * @param evt Index configuration changed event.
     * @return A future.
     */
    private CompletableFuture<?> onIndexCreate(CreateIndexEventParameters evt) {
        CatalogIndexDescriptor indexDescriptor = evt.indexDescriptor();

        if (!busyLock.enterBusy()) {
            fireEvent(IndexEvent.CREATE,
                    new IndexEventParameters(evt.causalityToken(), indexDescriptor.tableId(), indexDescriptor.id()),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            CatalogTableDescriptor tableDescriptor = catalogManager.table(indexDescriptor.tableId(), evt.catalogVersion());

            assert tableDescriptor != null : "Table not found: tableId=" + indexDescriptor.tableId();

            return createIndexLocally(evt.causalityToken(), tableDescriptor, indexDescriptor);
        } catch (Throwable th) {
            LOG.warn("Failed to process create index event.", th);

            return failedFuture(th);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<?> createIndexLocally(
            long causalityToken,
            CatalogTableDescriptor tableDescriptor,
            CatalogIndexDescriptor indexDescriptor
    ) {
        int tableId = tableDescriptor.id();
        int indexId = indexDescriptor.id();

        LOG.trace("Creating local index: name={}, id={}, tableId={}, token={}",
                indexDescriptor.name(), indexId, tableId, causalityToken);

        IndexDescriptor eventIndexDescriptor = toEventIndexDescriptor(indexDescriptor);

        var storageIndexDescriptor = StorageIndexDescriptor.create(tableDescriptor, indexDescriptor);

        CompletableFuture<?> fireEventFuture =
                fireEvent(IndexEvent.CREATE, new IndexEventParameters(causalityToken, tableId, indexId, eventIndexDescriptor));

        TableImpl table = tableManager.getTable(tableId);

        assert table != null : tableId;

        CompletableFuture<SchemaRegistry> schemaRegistryFuture = schemaManager.schemaRegistry(causalityToken, tableId);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19712 Listen to assignment changes and start new index storages.
        CompletableFuture<PartitionSet> tablePartitionFuture = tableManager.localPartitionSetAsync(causalityToken, tableId);

        CompletableFuture<?> createIndexFuture = tablePartitionFuture.thenAcceptBoth(schemaRegistryFuture, (partitions, schemaRegistry) -> {
            TableRowToIndexKeyConverter tableRowConverter = new TableRowToIndexKeyConverter(
                    schemaRegistry,
                    eventIndexDescriptor.columns().toArray(STRING_EMPTY_ARRAY)
            );

            if (eventIndexDescriptor instanceof SortedIndexDescriptor) {
                table.registerSortedIndex(
                        (StorageSortedIndexDescriptor) storageIndexDescriptor,
                        tableRowConverter::convert,
                        partitions
                );
            } else {
                boolean unique = indexDescriptor.unique();

                table.registerHashIndex(
                        (StorageHashIndexDescriptor) storageIndexDescriptor,
                        unique,
                        tableRowConverter::convert,
                        partitions
                );

                if (unique) {
                    table.pkId(indexId);
                }
            }
        });

         return createIndexFuture;
    }

    /**
     * This class encapsulates the logic of conversion from table row to a particular index key.
     */
    private static class TableRowToIndexKeyConverter {
        private final SchemaRegistry registry;
        private final String[] indexedColumns;
        private final Object mutex = new Object();

        private volatile VersionedConverter converter = new VersionedConverter(-1, null);

        TableRowToIndexKeyConverter(SchemaRegistry registry, String[] indexedColumns) {
            this.registry = registry;
            this.indexedColumns = indexedColumns;
        }

        public BinaryTuple convert(BinaryRow binaryRow) {
            VersionedConverter converter = this.converter;

            if (converter.version != binaryRow.schemaVersion()) {
                synchronized (mutex) {
                    if (converter.version != binaryRow.schemaVersion()) {
                        converter = createConverter(binaryRow.schemaVersion());

                        this.converter = converter;
                    }
                }
            }

            return converter.convert(binaryRow);
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
        private static class VersionedConverter {
            private final int version;
            private final Function<BinaryRow, BinaryTuple> delegate;

            private VersionedConverter(int version, Function<BinaryRow, BinaryTuple> delegate) {
                this.version = version;
                this.delegate = delegate;
            }

            /** Converts the given row to tuple. */
            public BinaryTuple convert(BinaryRow binaryRow) {
                return delegate.apply(binaryRow);
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
}
