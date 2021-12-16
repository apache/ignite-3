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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.idx.event.IndexEvent;
import org.apache.ignite.internal.idx.event.IndexEventParameters;
import org.apache.ignite.internal.manager.AbstractProducer;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
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
                        assert false : "Index cannot be updated [ctx=" + ctx +']';

                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private void onIndexDrop(ConfigurationNotificationEvent<TableIndexView> ctx) {
        TableConfiguration tbl = ctx.config(TableConfiguration.class);
        String tblName = tbl.name().value();
        String idxName = ctx.newValue().name();
    }

    private void onIndexCreate(ConfigurationNotificationEvent<TableIndexView> ctx) {

    }


    @Override
    public void stop() throws Exception {
    }

    @Override
    public List<InternalSortedIndex> indexes(UUID tblId) {
        return null;
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

    private void createIndexLocally(InternalTable tbl, SortedIndexView idx) {
        MySortedIndexDescriptor idxDesc = new MySortedIndexDescriptor(null, null);

        SortedIndexStorage idxStore = tbl.storage().createSortedIndex(idxDesc);

    }
}
