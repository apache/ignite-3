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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.directProxy;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableView;
import org.apache.ignite.internal.configuration.schema.SchemaConfiguration;
import org.apache.ignite.internal.configuration.schema.SchemaView;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteObjectName;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table manager.
 */
public class TableManager extends Producer<TableEvent, TableEventParameters> implements IgniteTables, IgniteTablesInternal,
        IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(TableManager.class);

    private static final int INITIAL_SCHEMA_VERSION = 1;

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Data storage configuration. */
    private final DataStorageConfiguration dataStorageCfg;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /** Storage engine instance. Only one type is available right now, which is the {@link RocksDbStorageEngine}. */
    private final StorageEngine engine;

    /** Transaction manager. */
    private final TxManager txManager;

    /** Partitions store directory. */
    private final Path partitionsStoreDir;

    /** Here a table future stores during creation (until the table can be provided to client). */
    private final Map<UUID, CompletableFuture<Table>> tableCreateFuts = new ConcurrentHashMap<>();

    /** Versioned store for tables by name. */
    private final VersionedValue<Map<String, TableImpl>> tablesVv;

    /** Versioned store for tables by id. */
    private final VersionedValue<Map<UUID, TableImpl>> tablesByIdVv;

    /** Resolver that resolves a network address to node id. */
    private final Function<NetworkAddress, String> netAddrResolver;

    /** Data region instances. */
    private final Map<String, DataRegion> dataRegions = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Creates a new table manager.
     *
     * @param registry           Registry for versioned values.
     * @param tablesCfg          Tables configuration.
     * @param dataStorageCfg     Data storage configuration.
     * @param raftMgr            Raft manager.
     * @param baselineMgr        Baseline manager.
     * @param partitionsStoreDir Partitions store directory.
     * @param txManager          TX manager.
     */
    public TableManager(
            Consumer<Consumer<Long>> registry,
            TablesConfiguration tablesCfg,
            DataStorageConfiguration dataStorageCfg,
            Loza raftMgr,
            BaselineManager baselineMgr,
            TopologyService topologyService,
            Path partitionsStoreDir,
            TxManager txManager
    ) {
        this.tablesCfg = tablesCfg;
        this.dataStorageCfg = dataStorageCfg;
        this.raftMgr = raftMgr;
        this.baselineMgr = baselineMgr;
        this.partitionsStoreDir = partitionsStoreDir;
        this.txManager = txManager;

        netAddrResolver = addr -> {
            ClusterNode node = topologyService.getByAddress(addr);

            if (node == null) {
                throw new IllegalStateException("Can't resolve ClusterNode by its networkAddress=" + addr);
            }

            return node.id();
        };

        engine = new RocksDbStorageEngine();

        tablesVv = new VersionedValue<>(registry, HashMap::new);
        tablesByIdVv = new VersionedValue<>(registry, HashMap::new);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        ((ExtendedTableConfiguration) tablesCfg.tables().any()).schemas().listenElements(new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<SchemaView> schemasCtx) {
                long causalityToken = schemasCtx.storageRevision();

                ExtendedTableConfiguration tblCfg = schemasCtx.config(TableConfiguration.class);

                UUID tblId = tblCfg.id().value();

                String tblName = tblCfg.name().value();

                SchemaDescriptor schemaDescriptor = SchemaSerializerImpl.INSTANCE.deserialize((schemasCtx.newValue().schema()));

                if (!busyLock.enterBusy()) {
                    if (schemaDescriptor.version() != INITIAL_SCHEMA_VERSION) {
                        fireEvent(
                                TableEvent.ALTER,
                                new TableEventParameters(causalityToken, tblId, tblName),
                                new NodeStoppingException()
                        );
                    }

                    return CompletableFuture.failedFuture(new NodeStoppingException());
                }

                try {
                    createSchemaInternal(schemasCtx);
                } finally {
                    busyLock.leaveBusy();
                }

                return CompletableFuture.completedFuture(null);
            }
        });

        ((ExtendedTableConfiguration) tablesCfg.tables().any()).assignments().listen(assignmentsCtx -> {
            if (!busyLock.enterBusy()) {
                return CompletableFuture.failedFuture(new NodeStoppingException());
            }

            try {
                updateAssignmentInternal(assignmentsCtx);
            } finally {
                busyLock.leaveBusy();
            }

            return CompletableFuture.completedFuture(null);
        });

        tablesCfg.tables().listenElements(new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<TableView> ctx) {
                if (!busyLock.enterBusy()) {
                    String tblName = ctx.newValue().name();
                    UUID tblId = ((ExtendedTableView) ctx.newValue()).id();

                    fireEvent(TableEvent.CREATE,
                            new TableEventParameters(ctx.storageRevision(), tblId, tblName),
                            new NodeStoppingException()
                    );

                    return CompletableFuture.failedFuture(new NodeStoppingException());
                }

                try {
                    createTableLocally(
                            ctx.storageRevision(),
                            ctx.newValue().name(),
                            ((ExtendedTableView) ctx.newValue()).id(),
                            ctx.newValue().partitions()
                    );
                } finally {
                    busyLock.leaveBusy();
                }

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<?> onRename(String oldName, String newName, ConfigurationNotificationEvent<TableView> ctx) {
                // TODO: IGNITE-15485 Support table rename operation.

                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<TableView> ctx) {
                if (!busyLock.enterBusy()) {
                    String tblName = ctx.oldValue().name();
                    UUID tblId = ((ExtendedTableView) ctx.oldValue()).id();

                    fireEvent(
                            TableEvent.DROP,
                            new TableEventParameters(ctx.storageRevision(), tblId, tblName),
                            new NodeStoppingException()
                    );

                    return CompletableFuture.failedFuture(new NodeStoppingException());
                }

                try {
                    dropTableLocally(
                            ctx.storageRevision(),
                            ctx.oldValue().name(),
                            ((ExtendedTableView) ctx.oldValue()).id(),
                            (List<List<ClusterNode>>) ByteUtils.fromBytes(((ExtendedTableView) ctx.oldValue()).assignments())
                    );
                } finally {
                    busyLock.leaveBusy();
                }

                return CompletableFuture.completedFuture(null);
            }
        });

        engine.start();

        DataRegion defaultDataRegion = engine.createDataRegion(dataStorageCfg.defaultRegion());

        dataRegions.put(DEFAULT_DATA_REGION_NAME, defaultDataRegion);

        defaultDataRegion.start();
    }

    /**
     * Internal method to create a schema.
     *
     * @param schemasCtx Create schema configuration event.
     */
    private void createSchemaInternal(ConfigurationNotificationEvent<SchemaView> schemasCtx) {
        ExtendedTableConfiguration tblCfg = (ExtendedTableConfiguration) schemasCtx.config(TableConfiguration.class);

        UUID tblId = tblCfg.id().value();

        long causalityToken = schemasCtx.storageRevision();

        SchemaDescriptor schemaDescriptor = SchemaSerializerImpl.INSTANCE.deserialize((schemasCtx.newValue().schema()));

        tablesByIdVv.update(causalityToken, tablesById -> {
            TableImpl table = tablesById.get(tblId);

            ((SchemaRegistryImpl) table.schemaView()).onSchemaRegistered(schemaDescriptor);

            if (schemaDescriptor.version() != INITIAL_SCHEMA_VERSION) {
                fireEvent(TableEvent.ALTER, new TableEventParameters(causalityToken, table), null);
            }

            return tablesById;
        }, th -> {
            throw new IgniteInternalException(IgniteStringFormatter.format("Cannot create a schema for table"
                    + " [tableId={}, schemaVer={}]", tblId, schemaDescriptor.version()), th);
        });
    }

    /**
     * Updates or creates partition raft groups.
     *
     * @param assignmentsCtx Change assignment event.
     */
    private void updateAssignmentInternal(ConfigurationNotificationEvent<byte[]> assignmentsCtx) {
        ExtendedTableConfiguration tblCfg = assignmentsCtx.config(TableConfiguration.class);

        UUID tblId = tblCfg.id().value();

        long causalityToken = assignmentsCtx.storageRevision();

        List<List<ClusterNode>> oldAssignments = assignmentsCtx.oldValue() == null ? null :
                (List<List<ClusterNode>>) ByteUtils.fromBytes(assignmentsCtx.oldValue());

        List<List<ClusterNode>> newAssignments = (List<List<ClusterNode>>) ByteUtils.fromBytes(assignmentsCtx.newValue());

        // Empty assignments might be a valid case if tables are created from within cluster init HOCON
        // configuration, which is not supported now.
        assert newAssignments != null : IgniteStringFormatter.format("Table [id={}] has empty assignments.", tblId);

        int partitions = newAssignments.size();

        CompletableFuture<?>[] futures = new CompletableFuture<?>[partitions];

        // TODO: IGNITE-15554 Add logic for assignment recalculation in case of partitions or replicas changes
        // TODO: Until IGNITE-15554 is implemented it's safe to iterate over partitions and replicas cause there will
        // TODO: be exact same amount of partitions and replicas for both old and new assignments
        for (int i = 0; i < partitions; i++) {
            int partId = i;

            List<ClusterNode> oldPartitionAssignment = oldAssignments == null ? Collections.emptyList() :
                    oldAssignments.get(partId);

            List<ClusterNode> newPartitionAssignment = newAssignments.get(partId);

            var toAdd = new HashSet<>(newPartitionAssignment);

            toAdd.removeAll(oldPartitionAssignment);

            // Create new raft nodes according to new assignments.
            tablesByIdVv.update(causalityToken, tablesById -> {
                InternalTable internalTable = tablesById.get(tblId).internalTable();

                try {
                    futures[partId] = raftMgr.updateRaftGroup(
                            raftGroupName(tblId, partId),
                            newPartitionAssignment,
                            toAdd,
                            () -> new PartitionListener(tblId,
                                    new VersionedRowStore(internalTable.storage().getOrCreatePartition(partId),
                                            txManager))
                    ).thenAccept(
                            updatedRaftGroupService -> ((InternalTableImpl) internalTable)
                                    .updateInternalTableRaftGroupService(partId, updatedRaftGroupService)
                    ).exceptionally(th -> {
                        LOG.error("Failed to update raft groups one the node", th);

                        return null;
                    });
                } catch (NodeStoppingException e) {
                    throw new AssertionError("Loza was stopped before Table manager", e);
                }

                return tablesById;
            }, th -> {
                throw new IgniteInternalException(IgniteStringFormatter.format("Cannot start RAFT group for table"
                        + " [tableId={}, part={}]", tblId, partId), th);
            });
        }

        CompletableFuture.allOf(futures).join();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        Map<String, TableImpl> tables = tablesVv.latest();

        for (TableImpl table : tables.values()) {
            try {
                table.internalTable().storage().stop();
                table.internalTable().close();

                for (int p = 0; p < table.internalTable().partitions(); p++) {
                    raftMgr.stopRaftGroup(raftGroupName(table.tableId(), p));
                }
            } catch (Exception e) {
                LOG.error("Failed to stop a table {}", e, table.name());
            }
        }

        // Stop all data regions when all table storages are stopped.
        for (Entry<String, DataRegion> entry : dataRegions.entrySet()) {
            try {
                entry.getValue().stop();
            } catch (Exception e) {
                LOG.error("Failed to stop data region " + entry.getKey(), e);
            }
        }

        engine.stop();
    }

    /**
     * Creates local structures for a table.
     *
     * @param causalityToken Causality token.
     * @param name  Table name.
     * @param tblId Table id.
     * @param partitions Count of partitions.
     */
    private void createTableLocally(long causalityToken, String name, UUID tblId, int partitions) {
        Path storageDir = partitionsStoreDir.resolve(name);

        try {
            Files.createDirectories(storageDir);
        } catch (IOException e) {
            throw new IgniteInternalException(
                    "Failed to create partitions store directory for " + name + ": " + e.getMessage(),
                    e
            );
        }

        TableConfiguration tableCfg = tablesCfg.tables().get(name);

        DataRegion dataRegion = dataRegions.computeIfAbsent(tableCfg.dataRegion().value(), dataRegionName -> {
            DataRegion newDataRegion = engine.createDataRegion(dataStorageCfg.regions().get(dataRegionName));

            try {
                newDataRegion.start();
            } catch (Exception e) {
                try {
                    newDataRegion.stop();
                } catch (Exception stopException) {
                    e.addSuppressed(stopException);
                }

                throw e;
            }

            return newDataRegion;
        });

        TableStorage tableStorage = engine.createTable(storageDir, tableCfg, dataRegion);

        tableStorage.start();

        InternalTableImpl internalTable = new InternalTableImpl(name, tblId, new Int2ObjectOpenHashMap<>(partitions),
                partitions, netAddrResolver, txManager, tableStorage);

        var schemaRegistry = new SchemaRegistryImpl(v -> {
            if (!busyLock.enterBusy()) {
                throw new IgniteException(new NodeStoppingException());
            }

            try {
                return tableSchema(tblId, v);
            } finally {
                busyLock.leaveBusy();
            }
        }, () -> {
            if (!busyLock.enterBusy()) {
                throw new IgniteException(new NodeStoppingException());
            }

            try {
                return latestSchemaVersion(tblId);
            } finally {
                busyLock.leaveBusy();
            }
        });

        var table = new TableImpl(internalTable, schemaRegistry);

        tablesVv.update(causalityToken, previous -> {
            var val = new HashMap<>(previous);

            val.put(name, table);

            return val;
        }, th -> {
            throw new IgniteInternalException(IgniteStringFormatter.format("Cannot create a table [name={}, id={}]", name, tblId), th);
        });

        tablesByIdVv.update(causalityToken, previous -> {
            var val = new HashMap<>(previous);

            val.put(tblId, table);

            return val;
        }, th -> {
            throw new IgniteInternalException(IgniteStringFormatter.format("Cannot create a table [name={}, id={}]", name, tblId), th);
        });

        CompletableFuture.allOf(tablesByIdVv.get(causalityToken), tablesVv.get(causalityToken)).thenRun(() -> {
            completeApiCreateFuture(table);

            fireEvent(TableEvent.CREATE, new TableEventParameters(causalityToken, table), null);
        });
    }

    /**
     * Completes appropriate future to return result from API {@link TableManager#createTable(String, Consumer)}.
     *
     * @param table Table.
     */
    private void completeApiCreateFuture(TableImpl table) {
        CompletableFuture<Table> tblFut = tableCreateFuts.get(table.tableId());

        if (tblFut != null) {
            tblFut.complete(table);

            tableCreateFuts.values().removeIf(fut -> fut == tblFut);
        }
    }

    /**
     * Return table schema of certain version from history.
     *
     * @param tblId     Table id.
     * @param schemaVer Schema version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor tableSchema(UUID tblId, int schemaVer) {
        TableImpl table = tablesByIdVv.latest().get(tblId);

        assert table != null : "Table is undefined [tblId=" + tblId + ']';

        ExtendedTableConfiguration tblCfg = ((ExtendedTableConfiguration) tablesCfg.tables().get(table.name()));

        if (schemaVer <= table.schemaView().lastSchemaVersion()) {
            return getSchemaDescriptorLocally(schemaVer, tblCfg);
        }

        CompletableFuture<SchemaDescriptor> fut = new CompletableFuture<>();

        var clo = new EventListener<TableEventParameters>() {
            @Override
            public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
                if (tblId.equals(parameters.tableId()) && schemaVer <= parameters.table().schemaView().lastSchemaVersion()) {
                    fut.complete(getSchemaDescriptorLocally(schemaVer, tblCfg));

                    return true;
                }

                return false;
            }

            @Override
            public void remove(@NotNull Throwable exception) {
                fut.completeExceptionally(exception);
            }
        };

        listen(TableEvent.ALTER, clo);

        if (schemaVer <= table.schemaView().lastSchemaVersion()) {
            fut.complete(getSchemaDescriptorLocally(schemaVer, tblCfg));
        }

        if (!isSchemaExists(tblId, schemaVer) && fut.complete(null)) {
            removeListener(TableEvent.ALTER, clo);
        }

        return fut.join();
    }

    /**
     * Gets a schema descriptor from the local node configuration storage.
     *
     * @param schemaVer Schema version.
     * @param tblCfg    Table configuration.
     * @return Schema descriptor.
     */
    @NotNull
    private SchemaDescriptor getSchemaDescriptorLocally(int schemaVer, ExtendedTableConfiguration tblCfg) {
        SchemaConfiguration schemaCfg = tblCfg.schemas().get(String.valueOf(schemaVer));

        assert schemaCfg != null;

        return SchemaSerializerImpl.INSTANCE.deserialize(schemaCfg.schema().value());
    }

    /**
     * Drops local structures for a table.
     *
     * @param causalityToken Causality token.
     * @param name           Table name.
     * @param tblId          Table id.
     * @param assignment     Affinity assignment.
     */
    private void dropTableLocally(long causalityToken, String name, UUID tblId, List<List<ClusterNode>> assignment) {
        try {
            int partitions = assignment.size();

            for (int p = 0; p < partitions; p++) {
                raftMgr.stopRaftGroup(raftGroupName(tblId, p));
            }

            tablesVv.update(causalityToken, previousVal -> {
                var map = new HashMap<>(previousVal);

                map.remove(name);

                return map;
            }, th -> {
                throw new IgniteInternalException(IgniteStringFormatter.format("Cannot drop a table [name={}, id={}]", name, tblId),
                        th);
            });

            AtomicReference<TableImpl> tableHolder = new AtomicReference<>();

            tablesByIdVv.update(causalityToken, previousVal -> {
                var map = new HashMap<>(previousVal);

                TableImpl table = map.remove(tblId);

                tableHolder.set(table);

                return map;
            }, th -> {
                throw new IgniteInternalException(IgniteStringFormatter.format("Cannot drop a table [name={}, id={}]", name, tblId),
                    th);
            });

            TableImpl table = tableHolder.get();

            assert table != null : "There is no table with the name specified [name=" + name + ']';

            table.internalTable().storage().destroy();

            CompletableFuture.allOf(tablesByIdVv.get(causalityToken), tablesVv.get(causalityToken)).thenRun(() ->
                    fireEvent(TableEvent.DROP, new TableEventParameters(causalityToken, table), null)
            );
        } catch (Exception e) {
            fireEvent(TableEvent.DROP, new TableEventParameters(causalityToken, tblId, name), e);
        }
    }

    /**
     * Compounds a RAFT group unique name.
     *
     * @param tblId Table identifier.
     * @param partition Number of table partitions.
     * @return A RAFT group name.
     */
    @NotNull
    private String raftGroupName(UUID tblId, int partition) {
        return tblId + "_part_" + partition;
    }

    /** {@inheritDoc} */
    @Override
    public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        return join(createTableAsync(name, tableInitChange));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return createTableAsyncInternal(IgniteObjectName.parseCanonicalName(name), tableInitChange);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method that creates a new table with the given {@code name} asynchronously. If a table with the same name already exists,
     * a future will be completed with {@link TableAlreadyExistsException}.
     *
     * @param name            Table name.
     * @param tableInitChange Table changer.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableAlreadyExistsException
     */
    private CompletableFuture<Table> createTableAsyncInternal(String name, Consumer<TableChange> tableInitChange) {
        CompletableFuture<Table> tblFut = new CompletableFuture<>();

        tableAsyncInternal(name).thenAccept(tbl -> {
            if (tbl != null) {
                tblFut.completeExceptionally(new TableAlreadyExistsException(name));
            } else {
                tablesCfg.tables().change(change -> {
                    if (change.get(name) != null) {
                        throw new TableAlreadyExistsException(name);
                    }

                    change.create(name, (ch) -> {
                        tableInitChange.accept(ch);

                        var extConfCh = ((ExtendedTableChange) ch);

                        tableCreateFuts.put(extConfCh.id(), tblFut);

                        // Affinity assignments calculation.
                        extConfCh.changeAssignments(ByteUtils.toBytes(AffinityUtils.calculateAssignments(
                                        baselineMgr.nodes(),
                                        ch.partitions(),
                                        ch.replicas())))
                                // Table schema preparation.
                                .changeSchemas(schemasCh -> schemasCh.create(
                                        String.valueOf(INITIAL_SCHEMA_VERSION),
                                        schemaCh -> {
                                            SchemaDescriptor schemaDesc;

                                            //TODO IGNITE-15747 Remove try-catch and force configuration
                                            // validation here to ensure a valid configuration passed to
                                            // prepareSchemaDescriptor() method.
                                            try {
                                                schemaDesc = SchemaUtils.prepareSchemaDescriptor(
                                                        ((ExtendedTableView) ch).schemas().size(),
                                                        ch);
                                            } catch (IllegalArgumentException ex) {
                                                throw new ConfigurationValidationException(ex.getMessage());
                                            }

                                            schemaCh.changeSchema(SchemaSerializerImpl.INSTANCE.serialize(schemaDesc));
                                        }
                                ));
                    });
                }).exceptionally(t -> {
                    Throwable ex = getRootCause(t);

                    if (ex instanceof TableAlreadyExistsException) {
                        tblFut.completeExceptionally(ex);
                    } else {
                        LOG.error(IgniteStringFormatter.format("Table wasn't created [name={}]", name), ex);

                        tblFut.completeExceptionally(ex);

                        tableCreateFuts.values().removeIf(fut -> fut == tblFut);
                    }

                    return null;
                });
            }
        });

        return tblFut;
    }

    /** {@inheritDoc} */
    @Override
    public void alterTable(String name, Consumer<TableChange> tableChange) {
        join(alterTableAsync(name, tableChange));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return alterTableAsyncInternal(IgniteObjectName.parseCanonicalName(name), tableChange);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method that alters a cluster table. If an appropriate table does not exist, a future will be
     * completed with {@link TableNotFoundException}.
     *
     * @param name        Table name.
     * @param tableChange Table changer.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableNotFoundException
     */
    @NotNull
    private CompletableFuture<Void> alterTableAsyncInternal(String name, Consumer<TableChange> tableChange) {
        CompletableFuture<Void> tblFut = new CompletableFuture<>();

        tableAsync(name).thenAccept(tbl -> {
            if (tbl == null) {
                tblFut.completeExceptionally(new TableNotFoundException(name));
            } else {
                TableImpl tblImpl = (TableImpl) tbl;

                tablesCfg.tables().change(ch -> {
                    if (ch.get(name) == null) {
                        throw new TableNotFoundException(name);
                    }

                    ch.update(name, tblCh -> {
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
                                                        tblImpl.schemaView().schema(currTableView.schemas().size()),
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
                    );
                }).whenComplete((res, t) -> {
                    if (t != null) {
                        Throwable ex = getRootCause(t);

                        if (ex instanceof TableNotFoundException) {
                            tblFut.completeExceptionally(ex);
                        } else {
                            LOG.error(IgniteStringFormatter.format("Table wasn't altered [name={}]", name), ex);

                            tblFut.completeExceptionally(ex);
                        }
                    } else {
                        tblFut.complete(res);
                    }
                });
            }
        });

        return tblFut;
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

    /** {@inheritDoc} */
    @Override
    public void dropTable(String name) {
        join(dropTableAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropTableAsync(String name) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return dropTableAsyncInternal(IgniteObjectName.parseCanonicalName(name));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method that drops a table with the name specified. If appropriate table does not be found, a future will be
     * completed with {@link TableNotFoundException}.
     *
     * @param name Table name.
     * @return Future representing pending completion of the operation.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     * @see TableNotFoundException
     */
    @NotNull
    private CompletableFuture<Void> dropTableAsyncInternal(String name) {
        CompletableFuture<Void> dropTblFut = new CompletableFuture<>();

        tableAsyncInternal(name).thenAccept(tbl -> {
            // In case of drop it's an optimization that allows not to fire drop-change-closure if there's no such
            // distributed table and the local config has lagged behind.
            if (tbl == null) {
                dropTblFut.completeExceptionally(new TableNotFoundException(name));
            } else {
                tablesCfg.tables()
                        .change(change -> {
                            if (change.get(name) == null) {
                                throw new TableNotFoundException(name);
                            }

                            change.delete(name);
                        })
                        .whenComplete((res, t) -> {
                            if (t != null) {
                                Throwable ex = getRootCause(t);

                                if (ex instanceof TableNotFoundException) {
                                    dropTblFut.completeExceptionally(ex);
                                } else {
                                    LOG.error(IgniteStringFormatter.format("Table wasn't dropped [name={}]", name), ex);

                                    dropTblFut.completeExceptionally(ex);
                                }
                            } else {
                                dropTblFut.complete(res);
                            }
                        });
            }
        });

        return dropTblFut;
    }

    /** {@inheritDoc} */
    @Override
    public List<Table> tables() {
        return join(tablesAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return tablesAsyncInternal();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for getting table.
     *
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<List<Table>> tablesAsyncInternal() {
        // TODO: IGNITE-16288 directTableIds should use async configuration API
        return CompletableFuture.supplyAsync(this::directTableIds)
                .thenCompose(tableIds -> {
                    var tableFuts = new CompletableFuture[tableIds.size()];

                    var i = 0;

                    for (UUID tblId : tableIds) {
                        tableFuts[i++] = tableAsyncInternal(tblId, false);
                    }

                    return CompletableFuture.allOf(tableFuts).thenApply(unused -> {
                        var tables = new ArrayList<Table>(tableIds.size());

                        try {
                            for (var fut : tableFuts) {
                                var table = fut.get();

                                if (table != null) {
                                    tables.add((Table) table);
                                }
                            }
                        } catch (Throwable t) {
                            throw new CompletionException(t);
                        }

                        return tables;
                    });
                });
    }

    /**
     * Collects a list of direct table ids.
     *
     * @return A list of direct table ids.
     * @see DirectConfigurationProperty
     */
    private List<UUID> directTableIds() {
        NamedListView<TableView> views = directProxy(tablesCfg.tables()).value();

        List<UUID> tableUuids = new ArrayList<>();

        for (int i = 0; i < views.size(); i++) {
            ExtendedTableView extView = (ExtendedTableView) views.get(i);

            tableUuids.add(extView.id());
        }

        return tableUuids;
    }

    /**
     * Gets direct id of table with {@code tblName}.
     *
     * @param tblName Name of the table.
     * @return Direct id of the table, or {@code null} if the table with the {@code tblName} has not been found.
     * @see DirectConfigurationProperty
     */
    @Nullable
    private UUID directTableId(String tblName) {
        try {
            return ((ExtendedTableConfiguration) directProxy(tablesCfg.tables()).get(tblName)).id().value();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Checks that the schema is configured in the Metasorage consensus.
     *
     * @param tblId Table id.
     * @param schemaVer Schema version.
     * @return True when the schema configured, false otherwise.
     */
    private boolean isSchemaExists(UUID tblId, int schemaVer) {
        return latestSchemaVersion(tblId) >= schemaVer;
    }

    /**
     * Gets the latest version of the table schema which available in Metastore.
     *
     * @param tblId Table id.
     * @return The latest schema version.
     */
    private int latestSchemaVersion(UUID tblId) {
        try {
            NamedListView<SchemaView> tblSchemas = ((ExtendedTableConfiguration) getByInternalId(directProxy(tablesCfg.tables()), tblId))
                    .schemas().value();

            int lastVer = INITIAL_SCHEMA_VERSION;

            for (String schemaVerAsStr : tblSchemas.namedListKeys()) {
                int ver = Integer.parseInt(schemaVerAsStr);

                if (ver > lastVer) {
                    lastVer = ver;
                }
            }

            return lastVer;
        } catch (NoSuchElementException e) {
            assert false : "Table must exist. [tableId=" + tblId + ']';

            return INITIAL_SCHEMA_VERSION;
        }
    }

    /** {@inheritDoc} */
    @Override
    public Table table(String name) {
        return join(tableAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public TableImpl table(UUID id) throws NodeStoppingException {
        return join(tableAsync(id));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return tableAsyncInternal(IgniteObjectName.parseCanonicalName(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableImpl> tableAsync(UUID id) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return tableAsyncInternal(id, true);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Gets a table by name, if it was created before. Doesn't parse canonical name.
     *
     * @param name Table name.
     * @return Future representing pending completion of the {@code TableManager#tableAsyncInternal} operation.
     * */
    @NotNull
    private CompletableFuture<Table> tableAsyncInternal(String name) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            UUID tableId = directTableId(name);

            if (tableId == null) {
                return CompletableFuture.completedFuture(null);
            }

            return (CompletableFuture) tableAsyncInternal(tableId, false);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for getting table by id.
     *
     * @param id Table id.
     * @param checkConfiguration {@code True} when the method checks a configuration before trying to get a table, {@code false} otherwise.
     * @return Future representing pending completion of the operation.
     */
    @NotNull
    private CompletableFuture<TableImpl> tableAsyncInternal(UUID id, boolean checkConfiguration) {
        if (checkConfiguration && !isTableConfigured(id)) {
            return CompletableFuture.completedFuture(null);
        }

        var tbl = tablesByIdVv.latest().get(id);

        if (tbl != null) {
            return CompletableFuture.completedFuture(tbl);
        }

        CompletableFuture<TableImpl> getTblFut = new CompletableFuture<>();

        EventListener<TableEventParameters> clo = new EventListener<>() {
            @Override
            public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable e) {
                if (!id.equals(parameters.tableId())) {
                    return false;
                }

                if (e == null) {
                    getTblFut.complete(parameters.table());
                } else {
                    getTblFut.completeExceptionally(e);
                }

                return true;
            }

            @Override
            public void remove(@NotNull Throwable e) {
                getTblFut.completeExceptionally(e);
            }
        };

        listen(TableEvent.CREATE, clo);

        tbl = tablesByIdVv.latest().get(id);

        if (tbl != null && getTblFut.complete(tbl) || !isTableConfigured(id) && getTblFut.complete(null)) {
            removeListener(TableEvent.CREATE, clo, null);
        }

        return getTblFut;
    }

    /**
     * Checks that the table is configured with specific id.
     *
     * @param id Table id.
     * @return True when the table is configured into cluster, false otherwise.
     */
    private boolean isTableConfigured(UUID id) {
        try {
            ((ExtendedTableConfiguration) getByInternalId(directProxy(tablesCfg.tables()), id)).id().value();

            return true;
        } catch (NoSuchElementException e) {
            return false;
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
     * Sets the nodes as baseline for all tables created by the manager.
     *
     * @param nodes New baseline nodes.
     * @throws NodeStoppingException If an implementation stopped before the method was invoked.
     */
    public void setBaseline(Set<String> nodes) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }
        try {
            setBaselineInternal(nodes);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for setting a baseline.
     *
     * @param nodes Names of baseline nodes.
     */
    private void setBaselineInternal(Set<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IgniteException("New baseline can't be null or empty");
        }

        var currClusterMembers = new HashSet<>(baselineMgr.nodes());

        var currClusterMemberNames =
                currClusterMembers.stream().map(ClusterNode::name).collect(Collectors.toSet());

        for (String nodeName : nodes) {
            if (!currClusterMemberNames.contains(nodeName)) {
                throw new IgniteException("Node '" + nodeName + "' not in current network cluster membership. "
                        + " Adding not alive nodes is not supported yet.");
            }
        }

        var newBaseline = currClusterMembers
                .stream().filter(n -> nodes.contains(n.name())).collect(Collectors.toSet());

        updateAssignments(currClusterMembers);

        if (!newBaseline.equals(currClusterMembers)) {
            updateAssignments(newBaseline);
        }
    }

    /**
     * Update assignments for all current tables according to input nodes list. These approach has known issues {@link
     * Ignite#setBaseline(Set)}.
     *
     * @param clusterNodes Set of nodes for assignment.
     */
    private void updateAssignments(Set<ClusterNode> clusterNodes) {
        var setBaselineFut = new CompletableFuture<>();

        var changePeersQueue = new ArrayList<Supplier<CompletableFuture<Void>>>();

        tablesCfg.tables()
                .change(tbls -> {
                    changePeersQueue.clear();

                    for (int i = 0; i < tbls.size(); i++) {
                        tbls.createOrUpdate(tbls.get(i).name(), changeX -> {
                            ExtendedTableChange change = (ExtendedTableChange) changeX;
                            byte[] currAssignments = change.assignments();

                            List<List<ClusterNode>> recalculatedAssignments = AffinityUtils.calculateAssignments(
                                    clusterNodes,
                                    change.partitions(),
                                    change.replicas());

                            if (!recalculatedAssignments.equals(ByteUtils.fromBytes(currAssignments))) {
                                change.changeAssignments(ByteUtils.toBytes(recalculatedAssignments));

                                changePeersQueue.add(() ->
                                        updateRaftTopology(
                                                (List<List<ClusterNode>>) ByteUtils.fromBytes(currAssignments),
                                                recalculatedAssignments,
                                                change.id()));
                            }
                        });
                    }
                })
                .thenCompose((v) -> {
                    CompletableFuture<?>[] changePeersFutures = new CompletableFuture<?>[changePeersQueue.size()];

                    int i = 0;

                    for (Supplier<CompletableFuture<Void>> task : changePeersQueue) {
                        changePeersFutures[i++] = task.get();
                    }

                    return CompletableFuture.allOf(changePeersFutures);
                })
                .whenComplete((res, th) -> {
                    if (th != null) {
                        setBaselineFut.completeExceptionally(th);
                    } else {
                        setBaselineFut.complete(null);
                    }
                });

        setBaselineFut.join();
    }

    /**
     * Update raft groups of table partitions to new peers list.
     *
     * @param oldAssignments Old assignment.
     * @param newAssignments New assignment.
     * @param tblId Table ID.
     * @return Future, which completes, when update finished.
     */
    private CompletableFuture<Void> updateRaftTopology(
            List<List<ClusterNode>> oldAssignments,
            List<List<ClusterNode>> newAssignments,
            UUID tblId) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[oldAssignments.size()];

        // TODO: IGNITE-15554 Add logic for assignment recalculation in case of partitions or replicas changes
        // TODO: Until IGNITE-15554 is implemented it's safe to iterate over partitions and replicas cause there will
        // TODO: be exact same amount of partitions and replicas for both old and new assignments
        for (int i = 0; i < oldAssignments.size(); i++) {
            final int p = i;

            List<ClusterNode> oldPartitionAssignment = oldAssignments.get(p);
            List<ClusterNode> newPartitionAssignment = newAssignments.get(p);

            try {
                futures[i] = raftMgr.changePeers(
                        raftGroupName(tblId, p),
                        oldPartitionAssignment,
                        newPartitionAssignment
                ).exceptionally(th -> {
                    LOG.error("Failed to update raft peers for group " + raftGroupName(tblId, p)
                            + "from " + oldPartitionAssignment + " to " + newPartitionAssignment, th);
                    return null;
                });
            } catch (NodeStoppingException e) {
                throw new AssertionError("Loza was stopped before Table manager", e);
            }
        }

        return CompletableFuture.allOf(futures);
    }
}
