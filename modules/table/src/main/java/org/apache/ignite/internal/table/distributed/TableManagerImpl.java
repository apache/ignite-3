package org.apache.ignite.internal.table.distributed;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.runner.LocalConfiguration;
import org.apache.ignite.configuration.schemas.table.TableInit;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.DistributedTableUtils;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.storage.TableStorageImpl;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operations;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.metastorage.internal.MetaStorageManager;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.TableManager;
import org.jetbrains.annotations.NotNull;

public class TableManagerImpl implements TableManager {
    /** Internal prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    /** Timeout. */
    private static final int TIMEOUT = 1000;

    /** Retry delay. */
    private static final int DELAY = 200;

    private static RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** Logger. */
    private final LogWrapper log = new LogWrapper(TableManagerImpl.class);

    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Network cluster. */
    private final NetworkCluster networkCluster;

    /** Configuration manager. */
    private final ConfigurationManager configurationMgr;

    /** Table creation subscription future. */
    private CompletableFuture<IgniteUuid> tableCreationSubscriptionFut = null;

    /** Tables. */
    private Map<String, Table> tables;

    public TableManagerImpl(
        ConfigurationManager configurationMgr,
        NetworkCluster networkCluster,
        MetaStorageManager metaStorageMgr
    ) {
        long startRevision = 0;

        tables = new HashMap<>();

        this.configurationMgr = configurationMgr;
        this.networkCluster = networkCluster;
        this.metaStorageMgr = metaStorageMgr;

        String localMemberName = configurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .name().value();

        configurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .metastorageMembers().listen(ctx -> {
            if (ctx.newValue() != null) {
                if (hasMetastorageLocally(localMemberName, ctx.newValue()))
                    subscribeForTableCreation();
                else
                    unsubscribeForTableCreation();
            }
            return CompletableFuture.completedFuture(null);

        });

        String[] metastorageMembers = configurationMgr.configurationRegistry().getConfiguration(LocalConfiguration.KEY)
            .metastorageMembers().value();

        if (hasMetastorageLocally(localMemberName, metastorageMembers))
            subscribeForTableCreation();

        String tableInternalPrefix = INTERNAL_PREFIX + "#.assignment";

        metaStorageMgr.watch(new Key(tableInternalPrefix), startRevision, new WatchListener() {
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    if (evt.newEntry().value() != null) {
                        long evtRevision = evt.newEntry().revision();

                        String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length());

                        String placeholderValue = keyTail.substring(0, keyTail.indexOf('.'));

                        UUID tblId = UUID.fromString(placeholderValue);

                        try {
                            String name = new String(metaStorageMgr.get(
                                new Key(INTERNAL_PREFIX + tblId.toString()), evtRevision).get()
                                .value(), StandardCharsets.UTF_8);
                            int partitions = configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
                                .tables().get(name).partitions().value();

                            List<List<NetworkMember>> assignment = (List<List<NetworkMember>>)DistributedTableUtils.fromBytes(
                                evt.newEntry().value());

                            HashMap<Integer, RaftGroupService> partitionMap = new HashMap<>(partitions);

                            for (int p = 0; p < partitions; p++) {
                                List<Peer> peers = new ArrayList<>();

                                for (NetworkMember member : assignment.get(p))
                                    peers.add(new Peer(member));

                                partitionMap.put(p, new RaftGroupServiceImpl("name" + "_part_" + p,
                                    networkCluster, FACTORY, TIMEOUT, peers, true, DELAY));
                            }

                            tables.put(name, new TableImpl(new TableStorageImpl(
                                tblId,
                                partitionMap,
                                partitions
                            )));
                        }
                        catch (InterruptedException | ExecutionException e) {
                            log.error("Failed to start table [key={}]",
                                evt.newEntry().key(), e);
                        }
                    }
                }

                return false;
            }

            @Override public void onError(@NotNull Throwable e) {
                log.error("Metastorage listener issue", e);
            }
        });
    }

    /**
     * Tests a member has a distributed Metastorage peer.
     *
     * @param localMemberName Local member uniq name.
     * @param metastorageMembers Metastorage members names.
     * @return True if the member has Metastorage, false otherwise.
     */
    private boolean hasMetastorageLocally(String localMemberName, String[] metastorageMembers) {
        boolean isLocalNodeHasMetasorage = false;

        for (String name : metastorageMembers) {
            if (name.equals(localMemberName)) {
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
        configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY)
            .tables().listen(ctx -> {
            HashSet<String> tblNamesToStart = new HashSet<>(ctx.newValue().namedListKeys());

            long revision = ctx.storageRevision();

            if (ctx.oldValue() != null)
                tblNamesToStart.removeAll(ctx.oldValue().namedListKeys());

            for (String tblName : tblNamesToStart) {
                TableView tableView = ctx.newValue().get(tblName);
                long update = 0;

                UUID tblId = new UUID(revision, update);

                String tableInternalPrefix = INTERNAL_PREFIX + tblId.toString();

                CompletableFuture<Boolean> fut = metaStorageMgr.invoke(
                    new Key(INTERNAL_PREFIX + tblId.toString()),
                    Conditions.value().eq(null),
                    Operations.put(tableView.name().getBytes(StandardCharsets.UTF_8)),
                    Operations.noop());

                try {
                    if (fut.get()) {
                        metaStorageMgr.put(new Key(tableInternalPrefix + ".assignment"), null);

                        log.info("Table manager created a table [name={}, revision={}]",
                            tableView.name(), revision);
                    }
                }
                catch (InterruptedException | ExecutionException e) {
                    log.error("Table was not fully initialized [name={}, revision={}]",
                        tableView.name(), revision, e);
                }
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    /**
     * Unsubscribe from table creation.
     */
    private void unsubscribeForTableCreation() {
        if (tableCreationSubscriptionFut == null)
            return;

        try {
            IgniteUuid subscriptionId = tableCreationSubscriptionFut.get();

            metaStorageMgr.stopWatch(subscriptionId);

            tableCreationSubscriptionFut = null;
        }
        catch (InterruptedException |ExecutionException e) {
            log.error("Couldn't unsubscribe from table creation", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Table createTable(String name, Consumer<TableInit> tableInitChange) {
        configurationMgr.configurationRegistry()
            .getConfiguration(TablesConfiguration.KEY).tables().change(change ->
            change.create(name, tableInitChange));

//        this.createTable("tbl1", change -> {
//            change.initReplicas(2);
//            change.initName("tbl1");
//            change.initPartitions(1_000);
//        });

        //TODO: Get it honestly using some future.
        Table tbl = null;

        while (tbl == null) {
            try {
                Thread.sleep(50);

                tbl = table(name);
            }
            catch (InterruptedException e) {
                log.error("Waiting of creation of table was interrupted.", e);
            }
        }

        return tbl;
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
