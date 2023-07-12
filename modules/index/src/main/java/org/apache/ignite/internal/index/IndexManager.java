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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toIndexDescriptor;
import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toTableDescriptor;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationUtils.findTableView;
import static org.apache.ignite.internal.util.ArrayUtils.STRING_EMPTY_ARRAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
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
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesView;
import org.apache.ignite.internal.schema.configuration.index.HashIndexChange;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexChange;
import org.apache.ignite.internal.schema.configuration.index.TableIndexChange;
import org.apache.ignite.internal.schema.configuration.index.TableIndexConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableNotFoundException;

/**
 * An Ignite component that is responsible for handling index-related commands like CREATE or DROP
 * as well as managing indexes' lifecycle.
 */
// TODO: IGNITE-19082 Delete this class
public class IndexManager extends Producer<IndexEvent, IndexEventParameters> implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexManager.class);

    /** Common tables and indexes configuration. */
    private final TablesConfiguration tablesCfg;

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
     * @param tablesCfg Tables and indexes configuration.
     * @param catalogManager Catalog manager.
     * @param schemaManager Schema manager.
     * @param tableManager Table manager.
     */
    public IndexManager(
            TablesConfiguration tablesCfg,
            CatalogManager catalogManager,
            SchemaManager schemaManager,
            TableManager tableManager
    ) {
        this.tablesCfg = Objects.requireNonNull(tablesCfg, "tablesCfg");
        this.schemaManager = Objects.requireNonNull(schemaManager, "schemaManager");
        this.tableManager = tableManager;
        this.catalogManager = catalogManager;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        LOG.debug("Index manager is about to start");

        tablesCfg.indexes().listenElements(new ConfigurationListener());

        tableManager.listen(TableEvent.CREATE, (param, ex) -> {
            if (ex != null) {
                return completedFuture(false);
            }

            // We can't return this future as the listener's result, because a deadlock can happen in the configuration component:
            // this listener is called inside a configuration notification thread and all notifications are required to finish before
            // new configuration modifications can occur (i.e. we are creating an index below). Therefore we create the index fully
            // asynchronously and rely on the underlying components to handle PK index synchronisation.
            tableManager.tableAsync(param.causalityToken(), param.tableId())
                    .thenCompose(table -> {
                        String[] pkColumns = Arrays.stream(table.schemaView().schema().keyColumns().columns())
                                .map(Column::name)
                                .toArray(String[]::new);

                        String pkName = table.name() + "_PK";
                        int pkId = table.tableId() + 1;

                        // Update config bypassing the Catalog, because PK has just been created in Catalog with the table.
                        return createIndexInternal(pkId, "PUBLIC", pkName, table.name(), false,
                                change -> change.changeUniq(true).convert(HashIndexChange.class)
                                        .changeColumnNames(pkColumns)
                        );
                    })
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Error when creating index: " + e);
                        }
                    });

            return completedFuture(false);
        });

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
            CompletableFuture<Void> indexCreateFuture = (params instanceof CreateSortedIndexParams)
                    ? catalogManager.createIndex((CreateSortedIndexParams) params)
                    : catalogManager.createIndex((CreateHashIndexParams) params);

            return indexCreateFuture.thenApply(ignore -> catalogManager.index(params.indexName(), Long.MAX_VALUE))
                    .thenCompose(index -> createIndexInternal(
                            index.id(),
                            params.schemaName(),
                            params.indexName(),
                            params.tableName(),
                            true,
                            createIndexChanger(params)
                    ));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Deprecated(forRemoval = true)
    private Consumer<TableIndexChange> createIndexChanger(AbstractIndexCommandParams indexParams) {
        if (indexParams instanceof CreateSortedIndexParams) {
            CreateSortedIndexParams params = (CreateSortedIndexParams) indexParams;
            return tableIndexChange -> tableIndexChange.convert(SortedIndexChange.class).changeColumns(colsInit -> {
                for (int i = 0; i < params.columns().size(); i++) {
                    String columnName = params.columns().get(i);
                    CatalogColumnCollation collation = params.collations().get(i);
                    //TODO: https://issues.apache.org/jira/browse/IGNITE-17563 Pass null ordering for columns.
                    colsInit.create(columnName, colInit -> colInit.changeAsc(collation.asc()));
                }
            });
        } else {
            CreateHashIndexParams params = (CreateHashIndexParams) indexParams;
            return tableIndexChange -> tableIndexChange.convert(HashIndexChange.class)
                    .changeColumnNames(params.columns().toArray(STRING_EMPTY_ARRAY));
        }
    }

    @Deprecated(forRemoval = true)
    private CompletableFuture<Boolean> createIndexInternal(
            int indexId,
            String schemaName,
            String indexName,
            String tableName,
            boolean failIfExists,
            Consumer<TableIndexChange> indexChange
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        LOG.debug("Going to create index [schema={}, table={}, index={}]", schemaName, tableName, indexName);

        try {
            validateName(indexName);

            CompletableFuture<Boolean> future = new CompletableFuture<>();

            // Check index existence flag, avoid usage of hasCause + IndexAlreadyExistsException.
            AtomicBoolean idxExist = new AtomicBoolean(false);

            tablesCfg.change(tablesChange -> tablesChange.changeIndexes(indexListChange -> {
                idxExist.set(false);

                if (indexListChange.get(indexName) != null) {
                    idxExist.set(true);

                    throw new IndexAlreadyExistsException(schemaName, indexName);
                }

                TableView tableCfg = tablesChange.tables().get(tableName);

                if (tableCfg == null) {
                    throw new TableNotFoundException(schemaName, tableName);
                }

                int tableId = tableCfg.id();

                tablesChange.changeGlobalIdCounter(indexId);

                Consumer<TableIndexChange> chg = indexChange.andThen(c -> c.changeTableId(tableId).changeId(indexId));

                indexListChange.create(indexName, chg);
            })).whenComplete((index, th) -> {
                if (th != null) {
                    LOG.debug("Unable to create index [schema={}, table={}, index={}]",
                            th, schemaName, tableName, indexName);

                    if (!failIfExists && idxExist.get()) {
                        future.complete(false);
                    } else {
                        future.completeExceptionally(th);
                    }
                } else {
                    TableIndexConfiguration idxCfg = tablesCfg.indexes().get(indexName);

                    if (idxCfg != null && idxCfg.value() != null) {
                        LOG.info("Index created [schema={}, table={}, index={}, indexId={}]",
                                schemaName, tableName, indexName, idxCfg.id().value());

                        future.complete(true);
                    } else {
                        var exception = new IgniteInternalException(
                                Common.INTERNAL_ERR, "Looks like the index was concurrently deleted");

                        LOG.info("Unable to create index [schema={}, table={}, index={}]",
                                exception, schemaName, tableName, indexName);

                        future.completeExceptionally(exception);
                    }
                }
            });

            return future;
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
                    .thenCompose(ignore -> dropIndexAsyncInternal(params.schemaName(), params.indexName(), true));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Deprecated(forRemoval = true)
    private CompletableFuture<Boolean> dropIndexAsyncInternal(
            String schemaName,
            String indexName,
            boolean failIfNotExists
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        LOG.debug("Going to drop index [schema={}, index={}]", schemaName, indexName);

        try {
            validateName(indexName);

            final CompletableFuture<Boolean> future = new CompletableFuture<>();

            // Check index existence flag, avoid usage of hasCause + IndexAlreadyExistsException.
            AtomicBoolean idxOrTblNotExist = new AtomicBoolean(false);

            tablesCfg.indexes().change(indexListChange -> {
                idxOrTblNotExist.set(false);

                TableIndexView idxView = indexListChange.get(indexName);

                if (idxView == null) {
                    idxOrTblNotExist.set(true);

                    throw new IndexNotFoundException(schemaName, indexName);
                }

                indexListChange.delete(indexName);
            }).whenComplete((ignored, th) -> {
                if (th != null) {
                    LOG.info("Unable to drop index [schema={}, index={}]", th, schemaName, indexName);

                    if (!failIfNotExists && idxOrTblNotExist.get()) {
                        future.complete(false);
                    } else {
                        future.completeExceptionally(th);
                    }
                } else {
                    LOG.info("Index dropped [schema={}, index={}]", schemaName, indexName);

                    future.complete(true);
                }
            });

            return future;
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void validateName(String indexName) {
        if (StringUtils.nullOrEmpty(indexName)) {
            throw new IgniteInternalException(
                    ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                    "Index name should be at least 1 character long"
            );
        }
    }

    /**
     * Callback method is called when index configuration changed and an index was dropped.
     *
     * @param evt Index configuration event.
     * @return A future.
     */
    private CompletableFuture<?> onIndexDrop(ConfigurationNotificationEvent<TableIndexView> evt) {
        TableIndexView tableIndexView = evt.oldValue();

        int idxId = tableIndexView.id();

        int tableId = tableIndexView.tableId();

        long causalityToken = evt.storageRevision();

        if (!busyLock.enterBusy()) {
            fireEvent(IndexEvent.DROP,
                    new IndexEventParameters(causalityToken, tableId, idxId),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            CompletableFuture<?> fireEventFuture = fireEvent(IndexEvent.DROP, new IndexEventParameters(causalityToken, tableId, idxId));

            CompletableFuture<?> dropIndexFuture = tableManager.tableAsync(causalityToken, tableId)
                    .thenAccept(table -> {
                        if (table != null) { // in case of DROP TABLE the table will be removed first
                            table.unregisterIndex(idxId);
                        }
                    });

            return allOf(fireEventFuture, dropIndexFuture);
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
    private CompletableFuture<?> onIndexCreate(ConfigurationNotificationEvent<TableIndexView> evt) {
        TableIndexView indexConfig = evt.newValue();

        int tableId = indexConfig.tableId();

        if (!busyLock.enterBusy()) {
            int idxId = indexConfig.id();

            fireEvent(IndexEvent.CREATE,
                    new IndexEventParameters(evt.storageRevision(), tableId, idxId),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            TablesView tablesView = evt.newValue(TablesView.class);

            TableView tableView = findTableView(tablesView.tables(), tableId);

            assert tableView != null : "tableId=" + tableId + ", indexId=" + indexConfig.id();

            CatalogTableDescriptor tableDescriptor = toTableDescriptor(tableView);
            CatalogIndexDescriptor indexDescriptor = toIndexDescriptor(indexConfig);

            return createIndexLocally(evt.storageRevision(), tableDescriptor, indexDescriptor);
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

    private class ConfigurationListener implements ConfigurationNamedListListener<TableIndexView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<TableIndexView> ctx) {
            return onIndexCreate(ctx);
        }

        @Override
        public CompletableFuture<?> onRename(ConfigurationNotificationEvent<TableIndexView> ctx) {
            return failedFuture(new UnsupportedOperationException("https://issues.apache.org/jira/browse/IGNITE-16196"));
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<TableIndexView> ctx) {
            return onIndexDrop(ctx);
        }

        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<TableIndexView> ctx) {
            return failedFuture(new IllegalStateException("Should not be called"));
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
