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

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.directProxy;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.idx.event.IndexEvent;
import org.apache.ignite.internal.idx.event.IndexEventParameters;
import org.apache.ignite.internal.manager.AbstractProducer;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteObjectName;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Internal index manager facade provides low-level methods for indexes operations.
 */
public class IndexManagerImpl extends AbstractProducer<IndexEvent, IndexEventParameters> implements IndexManager, IgniteComponent {
    private static final IgniteLogger LOG = IgniteLogger.forClass(IndexManagerImpl.class);

    private final TablesConfiguration tablesCfg;

    private final TableManager tblMgr;

    /** Indexes by canonical name. */
    private final Map<String, InternalSortedIndex> idxsByName = new ConcurrentHashMap<>();

    /** Indexes by id. */
    private final Map<UUID, InternalSortedIndex> idxsById = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param tablesCfg Tables configuration.
     */
    public IndexManagerImpl(
            TableManager tblMgr,
            TablesConfiguration tablesCfg
    ) {
        this.tblMgr = tblMgr;
        this.tablesCfg = tablesCfg;
    }

    @Override
    public void start() {
        tablesCfg.tables().any().indices().listenElements(
                new ConfigurationNamedListListener<>() {
                    @Override
                    public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<TableIndexView> ctx) {
                        TableConfiguration tbl = ctx.config(TableConfiguration.class);
                        String tblName = tbl.name().value();
                        UUID idxId = ctx.newValue().id();
                        String idxName = ctx.newValue().name();

                        if (!busyLock.enterBusy()) {
                            fireEvent(IndexEvent.CREATE,
                                    new IndexEventParameters(idxId, idxName, tblName),
                                    new NodeStoppingException());

                            return CompletableFuture.completedFuture(new NodeStoppingException());
                        }
                        try {
                            onIndexCreate(ctx);
                        } catch (Exception e) {
                            fireEvent(IndexEvent.CREATE, new IndexEventParameters(idxId, idxName, tblName), e);

                            LOG.error("Internal error, index creation failed [name={}, table={}]", e, idxName, tblName);

                            return CompletableFuture.completedFuture(e);
                        } finally {
                            busyLock.leaveBusy();
                        }

                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<?> onRename(String oldName, String newName,
                            ConfigurationNotificationEvent<TableIndexView> ctx) {
                        // TODO: IGNITE-16196 Supports index rename
                        return ConfigurationNamedListListener.super.onRename(oldName, newName, ctx);
                    }

                    @Override
                    public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<TableIndexView> ctx) {
                        TableConfiguration tbl = ctx.config(TableConfiguration.class);
                        String tblName = tbl.name().value();
                        String idxName = ctx.oldValue().name();
                        UUID idxId = ctx.oldValue().id();

                        if (!busyLock.enterBusy()) {
                            fireEvent(IndexEvent.DROP,
                                    new IndexEventParameters(idxId, idxName, tblName),
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
                    public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<TableIndexView> ctx) {
                        assert false : "Index cannot be updated [ctx=" + ctx + ']';

                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private void onIndexDrop(ConfigurationNotificationEvent<TableIndexView> ctx) {
        TableConfiguration tbl = ctx.config(TableConfiguration.class);
        String tblName = tbl.name().value();

        String idxName = ctx.oldValue().name();
        UUID idxId = ctx.oldValue().id();

        InternalSortedIndex idx = idxsByName.remove(idxName);

        assert idx != null : "Index wasn't found: indexName=" + idxName;

        idxsById.remove(idxId);

        idx.drop();

        fireEvent(IndexEvent.DROP, new IndexEventParameters(
                idxId,
                idx.name(),
                tblName
        ), null);
    }

    private void onIndexCreate(ConfigurationNotificationEvent<TableIndexView> ctx) throws NodeStoppingException {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-15916
        assert ctx.newValue() instanceof SortedIndexView : "Unsupported index type: " + ctx.newValue();

        ExtendedTableConfiguration tblCfg = ctx.config(TableConfiguration.class);

        TableImpl tbl = tblMgr.table(tblCfg.id().value());

        createIndexLocally(tbl, (SortedIndexView) ctx.newValue());
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
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

            indexAsync(idxCanonicalName).thenAccept((idx) -> {
                if (idx == null) {
                    idxFut.completeExceptionally(new IndexNotFoundException(idxCanonicalName));
                    return;
                }

                try {
                    CompletableFuture<TableImpl> tblFut = tblMgr.tableAsync(idx.tableId());

                    tblFut.thenAccept(tbl -> {
                        tablesCfg.tables().change(ch -> ch.createOrUpdate(
                                        tbl.name(),
                                        tblCh -> tblCh.changeIndices(idxes -> idxes.delete(idxCanonicalName))
                                ))
                                .whenComplete((res, t) -> {
                                    if (t != null) {
                                        Throwable ex = getRootCause(t);

                                        if (!(ex instanceof IndexNotFoundException)) {
                                            LOG.error("Index wasn't dropped [name={}]", ex, idxCanonicalName);
                                        }

                                        idxFut.completeExceptionally(ex);
                                    } else {
                                        idxFut.complete(null);
                                    }
                                });
                    });
                } catch (NodeStoppingException ex) {
                    idxFut.completeExceptionally(ex);
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
            Consumer<TableIndexChange> idxChange
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        String idxName = IgniteObjectName.parseCanonicalName(idxCanonicalName);

        try {
            CompletableFuture<InternalSortedIndex> idxFut = new CompletableFuture<>();

            tblMgr.tableAsync(tblCanonicalName).thenAccept((tbl) -> {
                if (tbl == null) {
                    idxFut.completeExceptionally(new TableNotFoundException(tblCanonicalName));
                }

                indexAsync(idxName).thenAccept((idx) -> {
                    if (idx != null) {
                        idxFut.completeExceptionally(new IndexAlreadyExistsException(idxCanonicalName));
                    }

                    tblMgr.alterTableAsync(tblCanonicalName, chng -> chng.changeIndices(idxes -> {
                                if (idxes.get(idxName) != null) {
                                    idxFut.completeExceptionally(new IndexAlreadyExistsException(idxCanonicalName));
                                }

                                idxes.create(idxName, idxChange::accept);
                            }
                    )).whenComplete((res, t) -> {
                        if (t != null) {
                            Throwable ex = getRootCause(t);

                            if (!(ex instanceof IndexAlreadyExistsException)) {
                                LOG.error("Index wasn't created [name={}]", ex, idxCanonicalName);
                            }

                            idxFut.completeExceptionally(ex);
                        } else {
                            idxFut.complete(idxsByName.get(idxName));
                        }
                    });
                });
            });

            return idxFut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public InternalSortedIndex index(String idxCanonicalName) {
        return join(indexAsync(idxCanonicalName));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<InternalSortedIndex> indexAsync(String idxCanonicalName) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            String idxName = IgniteObjectName.parseCanonicalName(idxCanonicalName);

            UUID idxId = directIndexId(idxName);

            if (idxId == null) { // If not configured.
                return CompletableFuture.completedFuture(null);
            }

            return indexAsyncInternal(idxId);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<InternalSortedIndex> indexAsyncInternal(UUID idxId) {
        InternalSortedIndex idx = idxsById.get(idxId);

        if (idx != null) {
            return CompletableFuture.completedFuture(idx);
        }

        CompletableFuture<InternalSortedIndex> getIdxFut = new CompletableFuture<>();

        EventListener<IndexEventParameters> clo = new EventListener<>() {
            @Override
            public boolean notify(IndexEventParameters parameters, @Nullable Throwable e) {
                if (!idxId.equals(parameters.indexId())) {
                    return false;
                }

                if (e == null) {
                    getIdxFut.complete(parameters.index());
                } else {
                    getIdxFut.completeExceptionally(e);
                }

                return true;
            }

            @Override
            public void remove(Throwable e) {
                getIdxFut.completeExceptionally(e);
            }
        };

        listen(IndexEvent.CREATE, clo);

        idx = idxsById.get(idxId);

        if (idx != null && getIdxFut.complete(idx) || !isIndexConfigured(idxId) && getIdxFut.complete(null)) {
            removeListener(IndexEvent.CREATE, clo, null);
        } else {
            getIdxFut.thenRun(() -> removeListener(IndexEvent.CREATE, clo, null));
        }

        return getIdxFut;
    }

    /**
     * Checks that the index is configured with specific name.
     *
     * @param idxId Index id.
     * @return True when the table is configured into cluster, false otherwise.
     */
    private boolean isIndexConfigured(UUID idxId) {
        try {
            NamedListView<TableView> directTablesCfg = directProxy(tablesCfg.tables()).value();

            // TODO: IGNITE-15721 Need to review this approach after the ticket would be fixed.
            // Probably, it won't be required getting configuration of all tables from Metastore.
            for (String name : directTablesCfg.namedListKeys()) {
                TableView tableView = directTablesCfg.get(name);

                getByInternalId(tableView.indices(), idxId);
            }

            return true;
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    /**
     * Gets direct id of index with {@code idxName}.
     *
     * @param idxName Index name.
     * @return TableIndexView.
     */
    private @Nullable UUID directIndexId(String idxName) {
        NamedListView<TableView> directTablesCfg = directProxy(tablesCfg.tables()).value();

        // TODO: IGNITE-15721 Need to review this approach after the ticket would be fixed.
        // Probably, it won't be required getting configuration of all tables from Metastore.
        for (String name : directTablesCfg.namedListKeys()) {
            TableView tableView = directTablesCfg.get(name);

            if (tableView != null) {
                TableIndexView tableIndexView = tableView.indices().get(idxName);

                if (tableIndexView != null) {
                    return tableIndexView.id();
                }
            }
        }
        return null;
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
    private IgniteException getRootCause(Throwable t) {
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

        //        Map<String, Column> cols = Stream.concat(
        //                Arrays.stream(tblSchema.keyColumns().columns()),
        //                Arrays.stream(tblSchema.valueColumns().columns())
        //        ).collect(Collectors.toMap(Column::name, Function.identity()));

        List<SortedIndexColumnDescriptor> idxCols = Stream.concat(
                idxView.columns().namedListKeys().stream(),
                Arrays.stream(tblSchema.keyColumns().columns())
                        .map(Column::name)
        )
                .distinct()
                .map(colName -> {
                    IndexColumnView idxCol = idxView.columns().get(colName);
                    return new SortedIndexColumnDescriptor(
                            tblSchema.column(colName),
                            new SortedIndexColumnCollation(idxCol == null || idxCol.asc()));
                })
                .collect(Collectors.toList());

        SortedIndexDescriptor idxDesc = new SortedIndexDescriptor(
                idxView.id().toString(),
                idxCols,
                tblSchema.keyColumns().columns()
        );

        InternalSortedIndexImpl idx = new InternalSortedIndexImpl(
                idxView.id(),
                idxView.name(),
                tbl.internalTable().storage().createSortedIndex(idxDesc),
                tbl
        );

        tbl.addRowListener(idx);

        idxsById.put(idxView.id(), idx);
        idxsByName.put(idxView.name(), idx);

        fireEvent(IndexEvent.CREATE, new IndexEventParameters(tbl.name(), idx), null);
    }

    /** {@inheritDoc} */
    @Override
    public InternalSortedIndex getIndexById(UUID id) {
        InternalSortedIndex res = join(indexAsyncInternal(id));

        return res;
    }
}
