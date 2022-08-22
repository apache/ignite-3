/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.schema.definition.TableDefinitionImpl.canonicalName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.HashIndexView;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableNotFoundException;
import org.jetbrains.annotations.NotNull;

/**
 * An Ignite component that is responsible for handling index-related commands like CREATE or DROP
 * as well as managing indexes lifecycle.
 */
public class IndexManager extends Producer<IndexEvent, IndexEventParameters> implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexManager.class);

    private final TableManager tableManager;

    private final Consumer<ConfigurationNamedListListener<TableIndexView>> indicesConfigurationChangeSubscription;

    private final Map<UUID, Index<?>> indexById = new ConcurrentHashMap<>();
    private final Map<String, Index<?>> indexByName = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param tableManager Table manager.
     */
    public IndexManager(
            TableManager tableManager,
            Consumer<ConfigurationNamedListListener<TableIndexView>> indicesConfigurationChangeSubscription
    ) {
        this.tableManager = Objects.requireNonNull(tableManager, "tableManager");
        this.indicesConfigurationChangeSubscription = Objects.requireNonNull(indicesConfigurationChangeSubscription, "tablesConfiguration");
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        LOG.debug("Index manager is about to start");

        indicesConfigurationChangeSubscription.accept(new ConfigurationListener());

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

        indexById.clear();
        indexByName.clear();

        LOG.info("Index manager stopped");
    }

    /**
     * Creates index from provided configuration changer.
     *
     * @param schemaName A name of the schema to create index in.
     * @param indexName A name of the index to create.
     * @param tableName A name of the table to create index for.
     * @param indexChange A consumer that suppose to change the configuration in order to provide description of an index.
     * @param failIfExists Flag indicates whether exception be thrown if index exists or not.
     * @return {@code True} if index was created successfully, {@code false} otherwise.
     * @throws IndexAlreadyExistsException If index already exists and
     */
    public boolean createIndex(
            String schemaName,
            String indexName,
            String tableName,
            boolean failIfExists,
            Consumer<TableIndexChange> indexChange
    ) {
        return join(createIndexAsync(schemaName, indexName, tableName, failIfExists, indexChange)) != null;
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
    // TODO: https://issues.apache.org/jira/browse/IGNITE-17474
    // Validation of the index name uniqueness is not implemented, because with given
    // configuration hierarchy this is a bit tricky exercise. Given that this hierarchy
    // is subject to change in the future, seems to be more rational just to omit this
    // part for now
    public CompletableFuture<Index<?>> createIndexAsync(
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

            CompletableFuture<Index<?>> future = new CompletableFuture<>();

            var canonicalName = canonicalName(schemaName, tableName);

            tableManager.tableAsyncInternal(canonicalName).thenAccept((table) -> {
                if (table == null) {
                    var exception = new TableNotFoundException(canonicalName);

                    LOG.info("Unable to create index [schema={}, table={}, index={}]",
                            exception, schemaName, tableName, indexName);

                    future.completeExceptionally(exception);

                    return;
                }

                tableManager.alterTableAsync(table.name(), tableChange -> tableChange.changeIndices(indexListChange -> {
                    if (indexListChange.get(indexName) != null) {
                        if (!failIfExists) {
                            future.complete(null);

                            return;
                        }
                        var exception = new IndexAlreadyExistsException(indexName);

                        LOG.info("Unable to create index [schema={}, table={}, index={}]",
                                exception, schemaName, tableName, indexName);

                        future.completeExceptionally(exception);

                        return;
                    }

                    indexListChange.create(indexName, indexChange);

                    TableIndexView indexView = indexListChange.get(indexName);

                    Set<String> columnNames = Set.copyOf(tableChange.columns().namedListKeys());

                    validateColumns(indexView, columnNames);
                })).whenComplete((index, th) -> {
                    if (th != null) {
                        LOG.info("Unable to create index [schema={}, table={}, index={}]",
                                th, schemaName, tableName, indexName);

                        future.completeExceptionally(th);
                    } else if (!future.isDone()) {
                        String canonicalIndexName = canonicalName(schemaName, indexName);

                        Index<?> createdIndex = indexByName.get(canonicalIndexName);

                        if (createdIndex != null) {
                            LOG.info("Index created [schema={}, table={}, index={}, indexId={}]",
                                    schemaName, tableName, indexName, createdIndex.id());

                            future.complete(createdIndex);
                        } else {
                            var exception = new IgniteInternalException(
                                    Common.UNEXPECTED_ERR, "Looks like the index was concurrently deleted");

                            LOG.info("Unable to create index [schema={}, table={}, index={}]",
                                    exception, schemaName, tableName, indexName);

                            future.completeExceptionally(exception);
                        }
                    }
                });
            });

            return future;
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops the index with a given name.
     *
     * @param schemaName A name of the schema the index belong to.
     * @param indexName A name of the index to drop.
     * @param failIfNotExist Flag, which force failure, when {@code trues} if index doen't not exists.
     * @return {@code True} if index was removed, {@code false} otherwise.
     * @throws IndexNotFoundException If index doesn't exist and {@code failIfNotExist} param was {@code true}.
     */
    public boolean dropIndex(
            String schemaName,
            String indexName,
            boolean failIfNotExist
    ) {
        return join(dropIndexAsync(schemaName, indexName, failIfNotExist));
    }

    /**
     * Drops the index with a given name asynchronously.
     *
     * @param schemaName A name of the schema the index belong to.
     * @param indexName A name of the index to drop.
     * @param failIfNotExist Flag, which force failure, when {@code trues} if index doen't not exists.
     * @return A future representing the result of the operation.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-17474
    // For now it is impossible to locate the index neither by id nor name.
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

            String canonicalName = canonicalName(schemaName, indexName);

            Index<?> index = indexByName.get(canonicalName);

            if (index == null) {
                return  failIfNotExists
                        ? CompletableFuture.failedFuture(new IndexNotFoundException(canonicalName))
                        : CompletableFuture.completedFuture(false);
            }

            CompletableFuture<Boolean> future = new CompletableFuture<>();

            tableManager.tableAsyncInternal(index.tableId(), false).thenAccept((table) -> {
                if (table == null) {
                    var exception = new IndexNotFoundException(canonicalName);

                    LOG.info("Unable to drop index [schema={}, index={}]",
                            exception, schemaName, indexName);

                    future.completeExceptionally(exception);

                    return;
                }

                tableManager.alterTableAsync(table.name(), tableChange -> tableChange.changeIndices(indexListChange -> {
                    if (indexListChange.get(indexName) == null) {
                        var exception = new IndexNotFoundException(canonicalName);

                        LOG.info("Unable to drop index [schema={}, index={}]",
                                exception, schemaName, indexName);

                        future.completeExceptionally(exception);

                        return;
                    }

                    indexListChange.delete(indexName);
                })).whenComplete((ignored, th) -> {
                    if (th != null) {
                        LOG.info("Unable to drop index [schema={}, index={}]",
                                th, schemaName, indexName);

                        future.completeExceptionally(th);
                    } else if (!future.isDone()) {
                        LOG.info("Index dropped [schema={}, index={}]", schemaName, indexName);

                        future.complete(true);
                    }
                });
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

    private void validateColumns(TableIndexView indexView, Set<String> tableColumns) {
        if (indexView instanceof SortedIndexView) {
            var sortedIndexView = (SortedIndexView) indexView;

            validateColumns(sortedIndexView.columns().namedListKeys(), tableColumns);
        } else if (indexView instanceof HashIndexView) {
            validateColumns(Arrays.asList(((HashIndexView) indexView).columnNames()), tableColumns);
        } else {
            throw new AssertionError("Unknown index type [type=" + (indexView != null ? indexView.getClass() : null) + ']');
        }
    }

    private void validateColumns(Iterable<String> indexedColumns, Set<String> tableColumns) {
        if (CollectionUtils.nullOrEmpty(indexedColumns)) {
            throw new IgniteInternalException(
                    ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                    "At least one column should be specified by index definition"
            );
        }

        for (var columnName : indexedColumns) {
            if (!tableColumns.contains(columnName)) {
                throw new IgniteInternalException(
                        Table.COLUMN_NOT_FOUND_ERR,
                        "Column not found [name=" + columnName + ']'
                );
            }
        }
    }

    /**
     * Callback method is called when index configuration changed and an index was dropped.
     *
     * @param evt Index configuration event.
     * @return A future.
     */
    private CompletableFuture<?> onIndexDrop(ConfigurationNotificationEvent<TableIndexView> evt) {
        if (!busyLock.enterBusy()) {
            String idxName = evt.newValue().name();
            UUID idxId = ((TableIndexView) evt.newValue()).id();

            fireEvent(IndexEvent.DROP,
                    new IndexEventParameters(evt.storageRevision(), idxId, idxName),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            Index<?> index = indexById.remove(evt.oldValue().id());
            indexByName.remove(index.name(), index);

            fireEvent(IndexEvent.DROP, new IndexEventParameters(evt.storageRevision(), index.id(), index.name()), null);
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
            String idxName = evt.newValue().name();
            UUID idxId = ((TableIndexView) evt.newValue()).id();

            fireEvent(IndexEvent.CREATE,
                    new IndexEventParameters(evt.storageRevision(), idxId, idxName),
                    new NodeStoppingException()
            );

            return failedFuture(new NodeStoppingException());
        }

        try {
            return createIndexLocally(
                    evt.storageRevision(),
                    //TODO: https://issues.apache.org/jira/browse/IGNITE-17474 Add tableID to index config instead of lookup to parent.
                    evt.config(ExtendedTableConfiguration.class).id().value(), // Parent element is table.
                    evt.newValue());
        } finally {
            busyLock.leaveBusy();
        }
    }

    @NotNull
    private CompletableFuture<?> createIndexLocally(long causalityToken, UUID tableId, TableIndexView tableIndexView) {
        assert tableIndexView != null;

        LOG.trace("Creating local index: name={}, id={}, tableId={}, token={}",
                tableIndexView.name(), tableIndexView.id(), tableId, causalityToken);

        Index<?> index = createIndex(tableId, tableIndexView);

        Index<?> prev = indexById.putIfAbsent(index.id(), index);

        assert prev == null;

        prev = indexByName.putIfAbsent(index.name(), index);

        assert prev == null;

        fireEvent(IndexEvent.CREATE, new IndexEventParameters(causalityToken, index), null);

        return CompletableFuture.completedFuture(null);
    }

    private Index<?> createIndex(UUID tableId, TableIndexView indexView) {
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
                canonicalName("PUBLIC", indexView.name()),
                Arrays.asList(indexView.columnNames())
        );
    }

    private SortedIndexDescriptor convert(SortedIndexView indexView) {
        var indexedColumns = new ArrayList<String>();
        var collations = new ArrayList<ColumnCollation>();

        for (var columnName : indexView.columns().namedListKeys()) {
            IndexColumnView columnView = indexView.columns().get(columnName);

            indexedColumns.add(columnName);
            collations.add(ColumnCollation.get(columnView.asc(), false));
        }

        return new SortedIndexDescriptor(
                canonicalName("PUBLIC", indexView.name()),
                indexedColumns,
                collations
        );
    }

    /**
     * Waits for future result and return, or unwraps {@link CompletionException} to {@link IgniteException} if failed.
     *
     * @param future Completable future.
     * @return Future result.
     */
    private <T> T join(CompletableFuture<T> future) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return future.join();
        } catch (CompletionException ex) {
            throw convertThrowable(ex.getCause());
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Convert to public throwable.
     *
     * @param th Throwable.
     * @return Public throwable.
     */
    private RuntimeException convertThrowable(Throwable th) {
        if (th instanceof RuntimeException) {
            return (RuntimeException) th;
        }

        return new IgniteException(th);
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
