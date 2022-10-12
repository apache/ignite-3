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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.HashIndexView;
import org.apache.ignite.internal.schema.configuration.index.IndexColumnView;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexChange;
import org.apache.ignite.internal.schema.configuration.index.TableIndexConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableNotFoundException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An Ignite component that is responsible for handling index-related commands like CREATE or DROP
 * as well as managing indexes lifecycle.
 */
public class IndexManager extends Producer<IndexEvent, IndexEventParameters> implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexManager.class);

    /** Common tables and indexes configuration. */
    private final TablesConfiguration tablesCfg;

    private final TableManager tableManager;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param tablesCfg Tables and indexes configuration.
     */
    public IndexManager(TablesConfiguration tablesCfg, TableManager tableManager) {
        this.tablesCfg = Objects.requireNonNull(tablesCfg, "tablesCfg");
        this.tableManager = tableManager;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        LOG.debug("Index manager is about to start");

        tablesCfg.indexes().listenElements(new ConfigurationListener());

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
     * Creates index from provided configuration changer.
     *
     * @param schemaName A name of the schema to create index in.
     * @param indexName A name of the index to create.
     * @param tableName A name of the table to create index for.
     * @param failIfExists Flag indicates whether exception be thrown if index exists or not.
     * @param indexChange A consumer that suppose to change the configuration in order to provide description of an index.
     * @return A future represented the result of creation.
     */
    public CompletableFuture<Boolean> createIndexAsync(
            String schemaName,
            String indexName,
            String tableName,
            boolean failIfExists,
            Consumer<TableIndexChange> indexChange
    ) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        LOG.debug("Going to create index [schema={}, table={}, index={}]", schemaName, tableName, indexName);

        try {
            validateName(indexName);

            CompletableFuture<Boolean> future = new CompletableFuture<>();

            // Check index existence flag, avoid usage of hasCause + IndexAlreadyExistsException.
            AtomicBoolean idxExist = new AtomicBoolean(false);

            tablesCfg.indexes().change(indexListChange -> {
                idxExist.set(false);

                if (indexListChange.get(indexName) != null) {
                    idxExist.set(true);

                    throw new IndexAlreadyExistsException(schemaName, indexName);
                }

                TableConfiguration tableCfg = tablesCfg.tables().get(tableName);

                if (tableCfg == null) {
                    throw new TableNotFoundException(schemaName, tableName);
                }

                ExtendedTableConfiguration exTableCfg = ((ExtendedTableConfiguration) tableCfg);

                final UUID tableId = exTableCfg.id().value();

                Consumer<TableIndexChange> chg = indexChange.andThen(c -> c.changeTableId(tableId));

                indexListChange.create(indexName, chg);
            }).whenComplete((index, th) -> {
                if (th != null) {
                    LOG.info("Unable to create index [schema={}, table={}, index={}]",
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
                                Common.UNEXPECTED_ERR, "Looks like the index was concurrently deleted");

                        LOG.info("Unable to create index [schema={}, table={}, index={}]",
                                exception, schemaName, tableName, indexName);

                        future.completeExceptionally(exception);
                    }
                }
            });

            return future;
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops the index with a given name asynchronously.
     *
     * @param schemaName A name of the schema the index belong to.
     * @param indexName A name of the index to drop.
     * @param failIfNotExists Flag, which force failure, when {@code trues} if index doen't not exists.
     * @return A future representing the result of the operation.
     */
    public CompletableFuture<Boolean> dropIndexAsync(
            String schemaName,
            String indexName,
            boolean failIfNotExists
    ) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
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
        UUID idxId = evt.oldValue().id();

        if (!busyLock.enterBusy()) {
            fireEvent(IndexEvent.DROP,
                    new IndexEventParameters(evt.storageRevision(), idxId),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            fireEvent(IndexEvent.DROP, new IndexEventParameters(evt.storageRevision(), idxId), null);
        } finally {
            busyLock.leaveBusy();
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Callback method triggers when index configuration changed and a new index was added.
     *
     * @param evt Index configuration changed event.
     * @return A future.
     */
    private CompletableFuture<?> onIndexCreate(ConfigurationNotificationEvent<TableIndexView> evt) {
        if (!busyLock.enterBusy()) {
            UUID idxId = evt.newValue().id();

            fireEvent(IndexEvent.CREATE,
                    new IndexEventParameters(evt.storageRevision(), idxId),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            UUID tableId = evt.newValue().tableId();

            return createIndexLocally(
                    evt.storageRevision(),
                    tableId,
                    evt.newValue());
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<?> createIndexLocally(long causalityToken, UUID tableId, TableIndexView tableIndexView) {
        assert tableIndexView != null;

        LOG.trace("Creating local index: name={}, id={}, tableId={}, token={}",
                tableIndexView.name(), tableIndexView.id(), tableId, causalityToken);

        Index<?> index = newIndex(tableId, tableIndexView);

        tableManager.registerSortedIndex(tableId, tableIndexView.id(), binRow -> toIndexKey(tableIndexView, binRow));

        fireEvent(IndexEvent.CREATE, new IndexEventParameters(causalityToken, index), null);

        return CompletableFuture.completedFuture(null);
    }

    private Index<?> newIndex(UUID tableId, TableIndexView indexView) {
        if (indexView instanceof SortedIndexView) {
            return new SortedIndexImpl(
                    indexView.id(),
                    tableId,
                    convert((SortedIndexView) indexView)
            );
        } else if (indexView instanceof HashIndexView) {
            return new HashIndex(
                    indexView.id(),
                    tableId,
                    convert((HashIndexView) indexView)
            );
        }

        throw new AssertionError("Unknown index type [type=" + (indexView != null ? indexView.getClass() : null) + ']');
    }

    private IndexDescriptor convert(HashIndexView indexView) {
        return new IndexDescriptor(
                indexView.name(),
                Arrays.asList(indexView.columnNames())
        );
    }

    private SortedIndexDescriptor convert(SortedIndexView indexView) {
        int colsCount = indexView.columns().size();
        var indexedColumns = new ArrayList<String>(colsCount);
        var collations = new ArrayList<ColumnCollation>(colsCount);

        for (var columnName : indexView.columns().namedListKeys()) {
            IndexColumnView columnView = indexView.columns().get(columnName);

            indexedColumns.add(columnName);
            collations.add(ColumnCollation.get(columnView.asc(), false));
        }

        return new SortedIndexDescriptor(
                indexView.name(),
                indexedColumns,
                collations
        );
    }

    private CompletableFuture<BinaryTuple> toIndexKey(TableIndexView indexView, BinaryRow tableRow) {
        return tableManager.tableAsyncInternal(indexView.tableId(), false).thenApply(table -> {
            SchemaDescriptor descriptor = table.schemaView().schema(tableRow.schemaVersion());

            int[] indexedColumns = resolveIndexColumns(descriptor, indexView);

            if (indexedColumns == null) {
                return null;
            }

            BinaryTupleSchema tupleSchema = BinaryTupleSchema.createSchema(descriptor, indexedColumns);

            var converter = new BinaryConverter(descriptor, tupleSchema);

            return new BinaryTuple(tupleSchema, converter.toTuple(tableRow));
        });
    }

    private int @Nullable [] resolveIndexColumns(SchemaDescriptor descriptor, TableIndexView indexView) {
        List<String> names;
        if (indexView instanceof HashIndexView) {
            names = List.of(((HashIndexView) indexView).columnNames());
        } else if (indexView instanceof SortedIndexView) {
            names = ((SortedIndexView) indexView).columns().namedListKeys();
        } else {
            // TODO replace with valid exception
            throw new IllegalStateException();
        }

        int[] result = new int[names.size()];
        int idx = 0;

        for (String name : names) {
            Column column = descriptor.column(name);

            if (column == null) {
                return null;
            }

            result[idx++] = column.schemaIndex();
        }

        return result;
    }

    private class ConfigurationListener implements ConfigurationNamedListListener<TableIndexView> {
        /** {@inheritDoc} */
        @Override
        public @NotNull CompletableFuture<?> onCreate(@NotNull ConfigurationNotificationEvent<TableIndexView> ctx) {
            return onIndexCreate(ctx);
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<TableIndexView> ctx
        ) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("https://issues.apache.org/jira/browse/IGNITE-16196"));
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull CompletableFuture<?> onDelete(@NotNull ConfigurationNotificationEvent<TableIndexView> ctx) {
            return onIndexDrop(ctx);
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<TableIndexView> ctx) {
            return CompletableFuture.failedFuture(new IllegalStateException("Should not be called"));
        }
    }
}
