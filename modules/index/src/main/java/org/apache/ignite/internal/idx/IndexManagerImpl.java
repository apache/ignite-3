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
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.idx.event.IndexEvent;
import org.apache.ignite.internal.idx.event.IndexEventParameters;
import org.apache.ignite.internal.manager.AbstractProducer;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexColumnCollation;
import org.apache.ignite.internal.storage.index.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;

/**
 * Internal index manager facade provides low-level methods for indexes operations.
 */
public class IndexManagerImpl extends AbstractProducer<IndexEvent, IndexEventParameters> implements IndexManager, IgniteComponent {
    private static final IgniteLogger LOG = IgniteLogger.forClass(IndexManagerImpl.class);

    private static final IgniteUuidGenerator INDEX_ID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    private final TablesConfiguration tablesCfg;

    private final DataStorageConfiguration dataStorageCfg;

    private final Path idxsStoreDir;

    private final TxManager txManager;

    private final TableManager tblMgr;

    /** Indexes by canonical name. */
    private final Map<String, InternalSortedIndex> idxsByName = new ConcurrentHashMap<>();

    /** Indexes by ID. */
    private final Map<IgniteUuid, InternalSortedIndex> idxsById = new ConcurrentHashMap<>();

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
                                    new IndexEventParameters(idxName, tblName),
                                    new NodeStoppingException());

                            return CompletableFuture.completedFuture(new NodeStoppingException());
                        }
                        try {
                            onIndexCreate(ctx);
                        } catch (Exception e) {
                            fireEvent(IndexEvent.CREATE, new IndexEventParameters(idxName, tblName), e);

                            LOG.error(LoggerMessageHelper.format(
                                    "Internal error, index creation failed [name={}, table={}]", idxName, tblName
                                    ), e);

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
                        String idxName = ctx.oldValue().name();

                        if (!busyLock.enterBusy()) {
                            fireEvent(IndexEvent.DROP,
                                    new IndexEventParameters(idxName, tblName),
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
        IgniteUuid idxId = IgniteUuid.fromString(ctx.oldValue().id());

        InternalSortedIndex idx = idxsById.remove(idxId);
        idxsByName.remove(idx.name());

        idx.drop();

        fireEvent(IndexEvent.DROP, new IndexEventParameters(idx.name(), tblName), null);
    }

    private void onIndexCreate(ConfigurationNotificationEvent<TableIndexView> ctx) throws NodeStoppingException {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-15916
        assert ((InnerNode) ctx.newValue()).specificNode() instanceof SortedIndexView : "Unsupported index type: " + ctx.newValue();

        ExtendedTableConfiguration tblCfg = ctx.config(TableConfiguration.class);

        TableImpl tbl = tblMgr.table(IgniteUuid.fromString(tblCfg.id().value()));

        createIndexLocally(
                tbl,
                ((InnerNode) ctx.newValue()).specificNode()
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
    public InternalSortedIndex createIndex(
            String idxCanonicalName,
            String tblCanonicalName,
            Consumer<TableIndexChange> idxChange
    ) {
        return join(createIndexAsync(idxCanonicalName, tblCanonicalName, idxChange));
    }

    /** {@inheritDoc} */
    @Override
    public void dropIndex(
            String idxCanonicalName
    ) {
        join(dropIndexAsync(idxCanonicalName));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropIndexAsync(String idxCanonicalName) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            CompletableFuture<Void> idxFut = new CompletableFuture<>();

            InternalSortedIndex idx = idxsByName.get(idxCanonicalName);

            if (idx == null) {
                idxFut.completeExceptionally(new IndexNotFoundException(idxCanonicalName));

                return idxFut;
            }

            tablesCfg.tables().change(ch -> ch.createOrUpdate(
                            idx.tableName(),
                            tblCh -> tblCh.changeIndices(idxes -> idxes.delete(idxCanonicalName))
                    ))
                    .whenComplete((res, t) -> {
                        if (t != null) {
                            Throwable ex = getRootCause(t);

                            if (!(ex instanceof IndexNotFoundException)) {
                                LOG.error(LoggerMessageHelper.format("Index wasn't dropped [name={}]", idxCanonicalName), ex);
                            }

                            idxFut.completeExceptionally(ex);
                        } else {
                            idxFut.complete(null);
                        }
                    });

            return idxFut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<InternalSortedIndex> createIndexAsync(
            String idxCanonicalName,
            String tblCanonicalName,
            Consumer<TableIndexChange> idx
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            CompletableFuture<InternalSortedIndex> idxFut = new CompletableFuture<>();

            final IgniteUuid idxId = INDEX_ID_GENERATOR.randomUuid();

            tblMgr.alterTableAsync(tblCanonicalName, chng -> chng.changeIndices(idxes -> {
                if (idxsByName.get(idxCanonicalName) != null) {
                    throw new IndexAlreadyExistsException(idxCanonicalName);
                }

                idxes.create(idxCanonicalName, idxCh -> {
                    idx.accept(idxCh);

                    idxCh.changeId(idxId.toString());
                });
            })).whenComplete((res, t) -> {
                if (t != null) {
                    Throwable ex = getRootCause(t);

                    if (!(ex instanceof IndexAlreadyExistsException)) {
                        LOG.error(LoggerMessageHelper.format("Index wasn't created [name={}]", idxCanonicalName), ex);
                    }

                    idxFut.completeExceptionally(ex);
                } else {
                    idxFut.complete(idxsById.get(idxId));
                }
            });

            return idxFut;
        } finally {
            busyLock.leaveBusy();
        }
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
     * Gets a cause exception for a client.
     *
     * @param t Exception wrapper.
     * @return A root exception which will be acceptable to throw for public API.
     */
    //TODO: IGNITE-16051 Implement exception converter for public API.
    private @NotNull IgniteException getRootCause(Throwable t) {
        Throwable ex;

        if (t instanceof CompletionException) {
            if (t.getCause() instanceof ConfigurationChangeException) {
                ex = t.getCause().getCause();
            } else {
                ex = t.getCause();
            }

        } else {
            ex = t;
        }

        return ex instanceof IgniteException ? (IgniteException) ex : new IgniteException(ex);
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
                                .map(Column::name)
                )
                .distinct()
                .map(colName -> {
                    IndexColumnView idxCol = idxView.columns().get(colName);
                    return new SortedIndexColumnDescriptor(
                            cols.get(colName),
                            new SortedIndexColumnCollation(idxCol == null || idxCol.asc()));
                })
                .collect(Collectors.toList());

        SortedIndexDescriptor idxDesc = new SortedIndexDescriptor(idxView.name(), idxCols);

        InternalSortedIndexImpl idx = new InternalSortedIndexImpl(
                IgniteUuid.fromString(idxView.id()),
                idxView.name(),
                tbl.internalTable().storage().createSortedIndex(idxDesc),
                tbl
        );

        tbl.addRowListener(idx);

        idxsByName.put(idx.name(), idx);
        idxsById.put(idx.id(), idx);

        fireEvent(IndexEvent.CREATE, new IndexEventParameters(idx), null);
    }
}
