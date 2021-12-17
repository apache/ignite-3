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

package org.apache.ignite.internal.idx;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableView;
import org.apache.ignite.internal.idx.event.IndexEvent;
import org.apache.ignite.internal.idx.event.IndexEventParameters;
import org.apache.ignite.internal.manager.AbstractProducer;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.storage.index.SortedIndexColumnCollation;
import org.apache.ignite.internal.storage.index.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableNotFoundException;
import org.jetbrains.annotations.NotNull;

/**
 * Internal index manager facade provides low-level methods for indexes operations.
 */
public class IndexManagerImpl extends AbstractProducer<IndexEvent, IndexEventParameters> implements IndexManager, IgniteComponent {
    private static final IgniteUuidGenerator INDEX_ID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    private final TablesConfiguration tablesCfg;

    private final DataStorageConfiguration dataStorageCfg;

    private final Path idxsStoreDir;

    private final TxManager txManager;

    private final TableManager tblMgr;

    /** Indexes by canonical name. */
    private final Map<String, InternalSortedIndex> idxs = new ConcurrentHashMap<>();

    /** Indexes by ID. */
    private final Map<IgniteUuid, InternalSortedIndex> idxsById = new ConcurrentHashMap<>();

    /** Indexes by ID. */
    private final Map<IgniteUuid, InternalSortedIndex> idxsByTableId = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param tablesCfg      Tables configuration.
     * @param dataStorageCfg Data storage configuration.
     * @param idxsStoreDir   Indexes store directory.
     * @param txManager      TX manager.
     */
    public IndexManagerImpl(
            TableManager tblMgr,
            TablesConfiguration tablesCfg,
            DataStorageConfiguration dataStorageCfg,
            Path idxsStoreDir,
            TxManager txManager
    ) {
        this.tblMgr = tblMgr;
        this.tablesCfg = tablesCfg;
        this.dataStorageCfg = dataStorageCfg;
        this.idxsStoreDir = idxsStoreDir;
        this.txManager = txManager;
    }

    @Override
    public void start() {
        tablesCfg.tables().any().indices().listenElements(
                new ConfigurationNamedListListener<TableIndexView>() {
                    @Override
                    public @NotNull CompletableFuture<?> onCreate(@NotNull ConfigurationNotificationEvent<TableIndexView> ctx) {
                        TableConfiguration tbl = ctx.config(TableConfiguration.class);
                        String tblName = tbl.name().value();
                        String idxName = ctx.newValue().name();

                        if (!busyLock.enterBusy()) {
                            fireEvent(IndexEvent.CREATE,
                                    new IndexEventParameters(idxName, tblName, null),
                                    new NodeStoppingException());

                            return CompletableFuture.completedFuture(new NodeStoppingException());
                        }
                        try {
                            onIndexCreate(ctx);
                        } catch (Exception e) {
                            fireEvent(IndexEvent.CREATE, new IndexEventParameters(idxName, tblName, null), e);

                            return CompletableFuture.completedFuture(e);
                        } finally {
                            busyLock.leaveBusy();
                        }

                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public @NotNull CompletableFuture<?> onRename(@NotNull String oldName, @NotNull String newName,
                            @NotNull ConfigurationNotificationEvent<TableIndexView> ctx) {
                        // TODO: rename index support
                        return ConfigurationNamedListListener.super.onRename(oldName, newName, ctx);
                    }

                    @Override
                    public @NotNull CompletableFuture<?> onDelete(@NotNull ConfigurationNotificationEvent<TableIndexView> ctx) {
                        TableConfiguration tbl = ctx.config(TableConfiguration.class);
                        String tblName = tbl.name().value();
                        String idxName = ctx.newValue().name();

                        if (!busyLock.enterBusy()) {
                            fireEvent(IndexEvent.DROP,
                                    new IndexEventParameters(idxName, tblName, null),
                                    new NodeStoppingException());

                            return CompletableFuture.completedFuture(new NodeStoppingException());
                        }
                        try {
                            onIndexDrop(ctx);
                        } finally {
                            busyLock.leaveBusy();
                        }

                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<TableIndexView> ctx) {
                        // TODO: is it true? What happens when columns are dropped?
                        assert false : "Index cannot be updated [ctx=" + ctx + ']';

                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private void onIndexDrop(ConfigurationNotificationEvent<TableIndexView> ctx) {
        TableConfiguration tbl = ctx.config(TableConfiguration.class);
        String tblName = tbl.name().value();
        String idxName = ctx.newValue().name();
    }

    private void onIndexCreate(ConfigurationNotificationEvent<TableIndexView> ctx) throws NodeStoppingException {
        assert ctx.newValue() instanceof SortedIndexView : "Unsupported index type: " + ctx.newValue();

        ExtendedTableConfiguration tbl = ctx.config(ExtendedTableConfiguration.class);
        tblMgr.table(IgniteUuid.fromString(tbl.id().value()));

        createIndexLocally(
                tblMgr.table(IgniteUuid.fromString(tbl.id().value())),
                (SortedIndexView) ctx.newValue()
        );
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public List<InternalSortedIndex> indexes(UUID tblId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void createIndex(String name, Consumer<TableChange> tableChange) {
        join(createIndexAsync(name, tableChange));
    }

    /** {@inheritDoc} */
    @Override
    public void createIndexAsync(String name, Consumer<TableChange> tableChange) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return alterTableAsyncInternal(name, tableChange);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for creating table asynchronously.
     *
     * @param name Table name.
     * @param idxCh Indexes changer.
     * @return Future representing pending completion of the operation.
     */
    @NotNull
    private CompletableFuture<Void> alterTableAsyncInternal(
            String name,
            Consumer<NamedListChange<TableIndexView, TableIndexChange>> idxCh
    ) {
        CompletableFuture<Void> tblFut = new CompletableFuture<>();

        tableAsync(name, true).thenAccept(tbl -> {
            if (tbl == null) {
                tblFut.completeExceptionally(new IgniteException(
                        LoggerMessageHelper.format("Table [name={}] does not exist and cannot be altered", name)));
            } else {
                IgniteUuid tblId = ((TableImpl) tbl).tableId();

                tablesCfg.tables().change(ch -> ch.createOrUpdate(name, tblCh -> {
                            tableChange.accept(tblCh);

                            ((ExtendedTableChange) tblCh).changeSchemas(schemasCh ->
                                    schemasCh.createOrUpdate(String.valueOf(schemasCh.size() + 1), schemaCh -> {
                                        ExtendedTableView currTableView = (ExtendedTableView) tablesCfg.tables().get(name).value();

                                        SchemaDescriptor descriptor;

                                        //TODO IGNITE-15747 Remove try-catch and force configuration validation
                                        // here to ensure a valid configuration passed to prepareSchemaDescriptor() method.
                                        try {
                                            descriptor = SchemaUtils.prepareSchemaDescriptor(
                                                    ((ExtendedTableView) tblCh).schemas().size(),
                                                    tblCh);

                                            descriptor.columnMapping(SchemaUtils.columnMapper(
                                                    tablesById.get(tblId).schemaView().schema(currTableView.schemas().size()),
                                                    currTableView,
                                                    descriptor,
                                                    tblCh));
                                        } catch (IllegalArgumentException ex) {
                                            // Convert unexpected exceptions here,
                                            // because validation actually happens later,
                                            // when bulk configuration update is applied.
                                            ConfigurationValidationException e =
                                                    new ConfigurationValidationException(ex.getMessage());

                                            e.addSuppressed(ex);

                                            throw e;
                                        }

                                        schemaCh.changeSchema(SchemaSerializerImpl.INSTANCE.serialize(descriptor));
                                    }));
                        }
                )).whenComplete((res, t) -> {
                    if (t != null) {
                        Throwable ex = getRootCause(t);

                        LOG.error(LoggerMessageHelper.format("Table wasn't altered [name={}]", name), ex);

                        tblFut.completeExceptionally(ex);
                    } else {
                        tblFut.complete(res);
                    }
                });
            }
        });

        return tblFut;
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

    /**
     * Creates a new table with the specified name or returns an existing table with the same name.
     *
     * @param idxName Index canonical name ([schema_name].[index_name]).
     * @param idxInitChange Table configuration.
     * @return A table instance.
     */
    private CompletableFuture<InternalSortedIndex> createIndexAsync(
            String tableCanonicalName,
            String idxName,
            Consumer<TableIndexChange> idxInitChange
    ) {
        CompletableFuture<InternalSortedIndex> idxFut = new CompletableFuture<>();

        IgniteUuid idxId = INDEX_ID_GENERATOR.randomUuid();

        TableView tbl = (TableView) tablesCfg.tables().get(tableCanonicalName);

        if (tbl == null) {
            throw new TableNotFoundException(tableCanonicalName);
        }

        tablesCfg.tables().change(chg -> chg.update(tableCanonicalName, tblChg -> {
            tblChg.changeIndices(idxes -> {
                idxes.create(idxName, idxChg -> {
                    idxInitChange.accept(idxChg);
                });
            });
        }));

        return idxFut;
    }

    private void createIndexLocally(TableImpl tbl, SortedIndexView idxView) {
        SchemaDescriptor tblSchema = tbl.schemaView().schema();

        Map<String, Column> cols = Stream.concat(
                Arrays.stream(tblSchema.keyColumns().columns()),
                Arrays.stream(tblSchema.valueColumns().columns())
        ).collect(Collectors.toMap(Column::name, Function.identity()));

        List<SortedIndexColumnDescriptor> idxCols = Stream.concat(
                        idxView.columns().namedListKeys().stream(),
                        Arrays.stream(tbl.schemaView().schema().keyColumns().columns())
                                .sorted(Comparator.comparing(Column::columnOrder))
                                .map(Column::name)
                )
                .distinct()
                .map(colName -> {
                    IndexColumnView idxCol = idxView.columns().get(colName);
                    return new SortedIndexColumnDescriptor(
                            cols.get(colName),
                            new SortedIndexColumnCollation(idxCol != null ? idxCol.asc() : true));
                })
                .collect(Collectors.toList());

        SortedIndexDescriptor idxDesc = new SortedIndexDescriptor(idxView.name(), idxCols);

        InternalSortedIndexImpl idx = new InternalSortedIndexImpl(
                IgniteUuid.fromString(idxView.id()),
                idxView.name(),
                tbl.internalTable().storage().createSortedIndex(idxDesc),
                tbl
        );

        idxs.put(idx.name(), idx);
        idxsById.put(idx.id(), idx);
        idxsByTableId.put(tbl.tableId(), idx);

        fireEvent(IndexEvent.CREATE, new IndexEventParameters(idx), null);
    }
}
