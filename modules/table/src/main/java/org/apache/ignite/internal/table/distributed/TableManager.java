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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.manager.TableEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableSchemaView;
import org.apache.ignite.internal.table.distributed.raft.PartitionCommandListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operations;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;

/**
 * Table manager.
 */
public class TableManager extends Producer<TableEvent> implements IgniteTables {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(TableManager.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.";

    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager. */
    private final ConfigurationManager configurationMgr;

    /** Table creation subscription future. */
    private CompletableFuture<Long> tableCreationSubscriptionFut;

    /** Tables. */
    private Map<String, Table> tables = new HashMap<>();

    public TableManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        SchemaManager schemaManager,
        Loza raftMgr,
        VaultManager vaultManager
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;

        String localNodeName = configurationMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .name().value();

        configurationMgr.configurationRegistry().getConfiguration(ClusterConfiguration.KEY)
            .metastorageNodes().listen(ctx -> {
            if (ctx.newValue() != null) {
                if (hasMetastorageLocally(localNodeName, ctx.newValue()))
                    subscribeForTableCreation();
                else
                    unsubscribeForTableCreation();
            }
            return CompletableFuture.completedFuture(null);

        });

        String[] metastorageMembers = configurationMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes().value();

        if (hasMetastorageLocally(localNodeName, metastorageMembers))
            subscribeForTableCreation();

        String tableInternalPrefix = INTERNAL_PREFIX + "assignment.";

        tableCreationSubscriptionFut = metaStorageMgr.registerWatchByPrefix(new Key(tableInternalPrefix), new WatchListener() {
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    String placeholderValue = evt.newEntry().key().toString().substring(tableInternalPrefix.length() - 1);

                    String name = new String(vaultManager.get(ByteArray.fromString(INTERNAL_PREFIX + placeholderValue))
                        .join().value(), StandardCharsets.UTF_8);

                    if (evt.newEntry().value() == null) {
                        assert evt.oldEntry().value() != null : "Previous assignment is unknown";

                        List<List<ClusterNode>> assignment = (List<List<ClusterNode>>)ByteUtils.fromBytes(
                            evt.oldEntry().value());

                        int partitions = assignment.size();

                        for (int p = 0; p < partitions; p++)
                            raftMgr.stopRaftGroup(raftGroupName(name, p), assignment.get(p));

                        onEvent(TableEvent.DROP, List.of(name), null);
                    }
                    else if (evt.newEntry().value().length > 0) {
                        List<List<ClusterNode>> assignment = (List<List<ClusterNode>>)ByteUtils.fromBytes(
                            evt.newEntry().value());

                        int partitions = assignment.size();

                        HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

                        for (int p = 0; p < partitions; p++) {
                            partitionMap.put(p, raftMgr.startRaftGroup(
                                raftGroupName(name, p),
                                assignment.get(p),
                                new PartitionCommandListener()
                            ));
                        }

                        UUID tblId = UUID.fromString(placeholderValue);

                        Table table = new TableImpl(new InternalTableImpl(
                            tblId,
                            partitionMap,
                            partitions
                        ), new TableSchemaView() {
                            @Override public SchemaDescriptor schema() {
                                return schemaManager.schema(tblId);
                            }

                            @Override public SchemaDescriptor schema(int ver) {
                                return schemaManager.schema(tblId, ver);
                            }
                        });

                        onEvent(TableEvent.CREATE, List.of(name, table), null);
                    }
                }

                return true;
            }

            @Override public void onError(@NotNull Throwable e) {
                LOG.error("Metastorage listener issue", e);
            }
        });
    }

    /**
     * Compuuses a RAFT group unique name.
     *
     * @param tableName Name of the table.
     * @param partition Muber of table partition.
     * @return A RAFT group name.
     */
    @NotNull private String raftGroupName(String tableName, int partition) {
        return tableName + "_part_" + partition;
    }

    /**
     * Checks whether the local node hosts Metastorage.
     *
     * @param localNodeName Local node uniq name.
     * @param metastorageMembers Metastorage members names.
     * @return True if the node has Metastorage, false otherwise.
     */
    private boolean hasMetastorageLocally(String localNodeName, String[] metastorageMembers) {
        boolean isLocalNodeHasMetasorage = false;

        for (String name : metastorageMembers) {
            if (name.equals(localNodeName)) {
                isLocalNodeHasMetasorage = true;

                break;
            }
        }
        return isLocalNodeHasMetasorage;
    }

    /**
     * Subscribes on table create.
     */
    private void subscribeForTableCreation() {
        //TODO: IGNITE-14652 Change a metastorage update in listeners to multi-invoke
        configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
            .tables().listen(ctx -> {
            Set<String> tablesToStart = ctx.newValue().namedListKeys() == null ?
                Collections.EMPTY_SET : ctx.newValue().namedListKeys();

            tablesToStart.removeAll(ctx.oldValue().namedListKeys());

            long revision = ctx.storageRevision();

            List<CompletableFuture<Boolean>> futs = new ArrayList<>();

            for (String tblName : tablesToStart) {
                TableView tableView = ctx.newValue().get(tblName);
                long update = 0;

                UUID tblId = new UUID(revision, update);

                futs.add(metaStorageMgr.invoke(
                    new Key(INTERNAL_PREFIX + tblId.toString()),
                    Conditions.value().eq(null),
                    Operations.put(tableView.name().getBytes(StandardCharsets.UTF_8)),
                    Operations.noop()).thenCompose(res ->
                    res ? metaStorageMgr.put(new Key(INTERNAL_PREFIX + "assignment." + tblId.toString()), new byte[0])
                        .thenApply(v -> true)
                        : CompletableFuture.completedFuture(false)));
            }

            Set<String> tablesToStop = ctx.oldValue().namedListKeys() == null ?
                Collections.EMPTY_SET : ctx.oldValue().namedListKeys();

            tablesToStop.removeAll(ctx.newValue().namedListKeys());

            for (String tblName : tablesToStop) {
                TableImpl t = (TableImpl)tables.get(tblName);

                UUID tblId = t.tableId();

                futs.add(metaStorageMgr.invoke(
                    new Key(INTERNAL_PREFIX + "assignment." + tblId.toString()),
                    Conditions.value().ne(null),
                    Operations.remove(),
                    Operations.noop()).thenCompose(res ->
                    res ? metaStorageMgr.remove(new Key(INTERNAL_PREFIX + tblId.toString()))
                        .thenApply(v -> true)
                        : CompletableFuture.completedFuture(false)));
            }

            return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
        });
    }

    /**
     * Unsubscribe from table creation.
     */
    private void unsubscribeForTableCreation() {
        if (tableCreationSubscriptionFut == null)
            return;

        try {
            Long subscriptionId = tableCreationSubscriptionFut.get();

            metaStorageMgr.unregisterWatch(subscriptionId);

            tableCreationSubscriptionFut = null;
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Couldn't unsubscribe from table creation", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        CompletableFuture<Table> tblFut = new CompletableFuture<>();

        listen(TableEvent.CREATE, (params, e) -> {
            String tableName = (String)params.get(0);

            if (!name.equals(tableName))
                return false;

            if (e == null) {
                Table table = (Table)params.get(1);

                tables.put(tableName, table);

                tblFut.complete(table);
            }
            else
                tblFut.completeExceptionally(e);

            return true;
        });

        configurationMgr.configurationRegistry()
            .getConfiguration(TablesConfiguration.KEY).tables().change(change ->
            change.create(name, tableInitChange));

//        this.createTable("tbl1", change -> {
//            change.initReplicas(2);
//            change.initName("tbl1");
//            change.initPartitions(1_000);
//        });

        return tblFut.join();
    }

    /** {@inheritDoc} */
    @Override public void dropTable(String name) {
        CompletableFuture<Void> dropTblFut = new CompletableFuture<>();

        listen(TableEvent.DROP, new BiPredicate<List<Object>, Exception>() {
            @Override public boolean test(List<Object> params, Exception e) {
                String tableName = (String)params.get(0);

                if (!name.equals(tableName))
                    return false;

                if (e == null) {
                    Table drppedTable = tables.remove(tableName);

                    assert drppedTable != null;

                    dropTblFut.complete(null);
                }
                else
                    dropTblFut.completeExceptionally(e);

                return true;
            }
        });

        configurationMgr.configurationRegistry()
            .getConfiguration(TablesConfiguration.KEY).tables().change(change -> {
            change.delete(name);
        });

        dropTblFut.join();
    }

    /** {@inheritDoc} */
    @Override public List<Table> tables() {
        return new ArrayList<>(tables.values());
    }

    /** {@inheritDoc} */
    @Override public Table table(String name) {
        return tables.get(name);
    }
}
