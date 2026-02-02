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

package org.apache.ignite.internal.partition;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.assignmentsChainKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.switchAppendKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.switchReduceKey;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.app.IgniteServerImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogNotFoundException;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.configuration.IgnitePaths;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.storage.impl.StorageDestructionIntent;
import org.apache.ignite.internal.raft.storage.impl.VaultGroupStoragesDestructionIntents;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.metrics.ResourceVacuumMetrics;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbPartitionStorage;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.DefaultLogEntryCodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

@ExtendWith(SystemPropertiesExtension.class)
class ItPartitionDestructionTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final int PARTITION_ID = 0;

    @Override
    protected int initialNodes() {
        return 0;
    }

    private static void aggressiveLowWatermarkIncrease(InitParametersBuilder builder) {
        builder.clusterConfiguration(aggressiveLwmIncreaseClusterConfig());
    }

    private static String aggressiveLwmIncreaseClusterConfig() {
        return "{\n"
                + "  ignite.gc.lowWatermark {\n"
                + "    dataAvailabilityTimeMillis: 1000,\n"
                + "    updateIntervalMillis: 100\n"
                + "  },\n"
                // Disable tx state storage cleanup.
                + "  ignite.system.properties." + TxManagerImpl.RESOURCE_TTL_PROP + " = " + Long.MAX_VALUE + "\n"
                + "}";
    }

    /**
     * This tests that, given that
     *
     * <ol>
     *     <li>A table was created and written to</li>
     *     <li>Then dropped</li>
     *     <li>LWM raised highly enough to make the table eligible for destruction</li>
     * </ol>
     *
     * <p>then the table MV storage will be destroyed.
     */
    @Test
    void testTableMvDataIsDestroyedOnTableDestruction() throws InterruptedException {
        cluster.startAndInit(1, ItPartitionDestructionTest::aggressiveLowWatermarkIncrease);

        createZoneAndTableWith1Partition(1);
        makePutInExplicitTxToTestTable();

        int tableId = testTableId();
        ZonePartitionId replicationGroupId = new ZonePartitionId(zoneId(tableId), PARTITION_ID);

        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));

        makeSurePartitionMvDataExistsOnDisk(ignite0, tableId);

        executeUpdate("DROP TABLE " + TABLE_NAME);

        verifyPartitionMvDataGetsRemovedFromDisk(ignite0, tableId, replicationGroupId);
    }

    @Test
    void partitionIsDestroyedOnZoneDestruction() throws Exception {
        cluster.startAndInit(1, ItPartitionDestructionTest::aggressiveLowWatermarkIncrease);

        createZoneAndTableWith1Partition(1);
        makePutInExplicitTxToTestTable();

        int tableId = testTableId();
        ZonePartitionId replicationGroupId = new ZonePartitionId(zoneId(tableId), PARTITION_ID);

        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));

        makeSurePartitionExistsOnDisk(ignite0, tableId, replicationGroupId);

        verifyZoneDistributionZoneManagerResourcesArePresent(ignite0, replicationGroupId.zoneId());

        executeUpdate("DROP TABLE " + TABLE_NAME);
        executeUpdate("DROP ZONE " + ZONE_NAME);

        verifyPartitionMvDataGetsRemovedFromDisk(ignite0, tableId, replicationGroupId);

        verifyPartitionNonMvDataExistsOnDisk(ignite0, replicationGroupId);

        // Trigger txStateStorage vacuumization that will remove all records from the storage and thus make it eligible for removal.
        assertThat(ignite0.txManager().vacuum(mock(ResourceVacuumMetrics.class)), willCompleteSuccessfully());

        verifyPartitionGetsFullyRemovedFromDisk(ignite0, tableId, replicationGroupId);

        verifyAssignmentKeysWereRemovedFromMetaStorage(ignite0, replicationGroupId);

        verifyZoneDistributionZoneManagerResourcesWereRemovedFromMetaStorage(ignite0, replicationGroupId.zoneId());
    }

    /**
     * This tests that, given that
     *
     * <ol>
     *     <li>A table was created and written to</li>
     *     <li>Then dropped</li>
     *     <li>LWM raised highly enough to make the table eligible for destruction</li>
     *     <li>But the LWM event not handled for some reason (so the table was not destroyed yet)</li>
     *     <li>The node gets restarted (but the Catalog still mentions the table in its history)</li>
     * </ol>
     *
     * <p>then the table MV storages will be destroyed on node start.
     */
    @Test
    void testTableMvDataIsDestroyedOnTableDestructionOnNodeRecovery()
            throws InterruptedException {
        cluster.startAndInit(1, ItPartitionDestructionTest::aggressiveLowWatermarkIncrease);
        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));
        Path workDir0 = ((IgniteServerImpl) cluster.server(0)).workDir();

        createZoneAndTableWith1Partition(1);
        makePutInExplicitTxToTestTable();

        // We need to do this flush because after restart we want to check that it was destroyed on start; but if we don't flush now,
        // it might not be flushed automatically relying on Raft log replay; but on node start the table will be already
        // stale, so its Raft groups will not be started, so we could see tx state storage empty anyway, even it was not destroyed.
        flushTxStateStorageToDisk(ignite0);

        int tableId = testTableId();
        ZonePartitionId replicationGroupId = new ZonePartitionId(zoneId(tableId), PARTITION_ID);

        makeSurePartitionMvDataExistsOnDisk(ignite0, tableId);

        // We don't want the dropped table to be destroyed before restart.
        disallowLwmRaiseUntilRestart(ignite0);

        executeUpdate("DROP TABLE " + TABLE_NAME);
        HybridTimestamp tsAfterDrop = latestCatalogVersionTs(ignite0);

        cluster.stopNode(0);

        // Simulate a situation when an LWM was raised (and persisted) and the node was stopped immediately, so we were not
        // able to destroy the dropped table yet, even though its drop moment is already under the LWM.
        raisePersistedLwm(workDir0, tsAfterDrop.tick());

        IgniteImpl restartedIgnite0 = unwrapIgniteImpl(cluster.startNode(0));

        verifyPartitionMvDataGetsRemovedFromDisk(restartedIgnite0, tableId, replicationGroupId);
    }

    @Test
    public void partitionIsDestroyedOnZoneDestructionOnNodeRecoveryIfLwmIsNotMovedWhileNodeIsAbsent() throws Exception {
        verifyPartitionIsDestroyedOnZoneDestructionOnNodeRecovery(false);
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27466")
    public void partitionIsDestroyedOnZoneDestructionOnNodeRecoveryIfLwmIsMovedWhileNodeIsAbsent() throws Exception {
        verifyPartitionIsDestroyedOnZoneDestructionOnNodeRecovery(true);
    }

    private void verifyPartitionIsDestroyedOnZoneDestructionOnNodeRecovery(boolean moveLwmWhileNodeIsAbsent) throws Exception {
        cluster.startAndInit(1, ItPartitionDestructionTest::aggressiveLowWatermarkIncrease);
        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));
        Path workDir0 = ((IgniteServerImpl) cluster.server(0)).workDir();

        createZoneAndTableWith1Partition(1);
        makePutInExplicitTxToTestTable();

        // We need to do this flush because after restart we want to check that it was destroyed on start; but if we don't flush now,
        // it might not be flushed automatically relying on Raft log replay; but on node start the table will be already
        // stale, so its Raft groups will not be started, so we could see tx state storage empty anyway, even it was not destroyed.
        flushTxStateStorageToDisk(ignite0);

        int tableId = testTableId();
        ZonePartitionId replicationGroupId = new ZonePartitionId(zoneId(tableId), PARTITION_ID);

        makeSurePartitionExistsOnDisk(ignite0, tableId, replicationGroupId);

        // We don't want the dropped zone to be destroyed before restart.
        disallowLwmRaiseUntilRestart(ignite0);

        executeUpdate("DROP TABLE " + TABLE_NAME);
        executeUpdate("DROP ZONE " + ZONE_NAME);
        HybridTimestamp tsAfterDrop = latestCatalogVersionTs(ignite0);

        verifyPartitionNonMvDataExistsOnDisk(ignite0, replicationGroupId);

        verifyZoneDistributionZoneManagerResourcesArePresent(ignite0, replicationGroupId.zoneId());

        // Trigger txStateStorage vacuumization that will remove all records from the storage and thus make it eligible for removal.
        assertThat(ignite0.txManager().vacuum(mock(ResourceVacuumMetrics.class)), willCompleteSuccessfully());

        cluster.stopNode(0);

        if (moveLwmWhileNodeIsAbsent) {
            // Simulate a situation when an LWM was raised (and persisted) and the node was stopped immediately, so we were not
            // able to destroy the dropped zone yet, even though its drop moment is already under the LWM.
            raisePersistedLwm(workDir0, tsAfterDrop.tick());
        }

        IgniteImpl restartedIgnite0 = unwrapIgniteImpl(cluster.startNode(0));

        verifyPartitionGetsFullyRemovedFromDisk(restartedIgnite0, tableId, replicationGroupId);

        verifyAssignmentKeysWereRemovedFromMetaStorage(restartedIgnite0, replicationGroupId);

        verifyZoneDistributionZoneManagerResourcesWereRemovedFromMetaStorage(restartedIgnite0, replicationGroupId.zoneId());
    }

    /**
     * This tests that, given that
     *
     * <ol>
     *     <li>A table was created and written to</li>
     *     <li>Then dropped</li>
     *     <li>LWM raised highly enough to make the table eligible for destruction</li>
     *     <li>But the LWM event not handled for some reason (so the table was not destroyed yet)</li>
     *     <li>Catalog got compacted so that its history does not contain any mentions of the table anymore</li>
     *     <li>The node gets restarted</li>
     * </ol>
     *
     * <p>then the table MV storages will be destroyed on node start.
     */
    @Test
    void testTableMvDataIsDestroyedOnTableDestructionOnNodeRecoveryAfterCatalogCompacted()
            throws InterruptedException {
        // Node 1 will host the Metastorage and will do Catalog compaction.
        // On node 0, we will verify that storages are destroyed on startup when it seems that the table is not mentioned in the Catalog.
        cluster.startAndInit(2, builder -> {
            builder.clusterConfiguration(aggressiveLwmIncreaseClusterConfig());
            builder.cmgNodeNames(cluster.nodeName(1));
            builder.metaStorageNodeNames(cluster.nodeName(1));
        });

        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));
        Path workDir0 = ((IgniteServerImpl) cluster.server(0)).workDir();

        createZoneAndTableWith1Partition(2);
        makePutInExplicitTxToTestTable();

        // We need to do this flush because after restart we want to check that it was destroyed on start; but if we don't flush now,
        // it might not be flushed automatically relying on Raft log replay; but on node start the table will be already
        // stale, so its Raft groups will not be started, so we could see tx state storage empty anyway, even it was not destroyed.
        flushTxStateStorageToDisk(ignite0);

        int tableId = testTableId();
        ZonePartitionId replicationGroupId = new ZonePartitionId(zoneId(tableId), PARTITION_ID);

        makeSurePartitionMvDataExistsOnDisk(ignite0, tableId);

        // We don't want the dropped table to be destroyed before restart on node 0.
        disallowLwmRaiseUntilRestart(ignite0);

        executeUpdate("DROP TABLE " + TABLE_NAME);
        HybridTimestamp tsAfterDrop = latestCatalogVersionTs(ignite0);

        cluster.stopNode(0);

        // This is created to trigger Catalog compaction as it doesn't happen if the version we want to retain is the only one that
        // will remain.
        createCatalogVersionNotRelatedToPartitions();

        IgniteImpl ignite1 = unwrapIgniteImpl(cluster.node(1));
        waitTillCatalogDoesNotContainTargetTable(ignite1, TABLE_NAME);

        // Simulate a situation when an LWM was raised (and persisted) and the node was stopped immediately, so we were not
        // able to destroy the dropped table yet, even though its drop moment is already under the LWM.
        raisePersistedLwm(workDir0, tsAfterDrop.tick());

        IgniteImpl restartedIgnite0 = unwrapIgniteImpl(cluster.startNode(0));

        verifyPartitionMvDataGetsRemovedFromDisk(restartedIgnite0, tableId, replicationGroupId);
    }

    @Test
    void testDurableLogDestruction() {
        cluster.startAndInit(1);

        IgniteImpl ignite = unwrapIgniteImpl(cluster.node(0));

        String groupId = "test-group";
        String groupName = "partition"; // Copied from "org.apache.ignite.internal.app.IgniteImpl.PARTITION_GROUP_NAME".

        var destructionIntents = new VaultGroupStoragesDestructionIntents(ignite.vault());
        destructionIntents.saveStorageDestructionIntent(new StorageDestructionIntent(groupId, groupName, false));

        LogStorage logStorage = createAndInitCustomLogStorage(ignite, groupId);

        LogEntry entry = new LogEntry();
        entry.setId(new LogId(1, 1));
        entry.setType(EntryType.ENTRY_TYPE_NO_OP);

        logStorage.appendEntries(List.of(entry));

        ignite = unwrapIgniteImpl(cluster.restartNode(0));

        LogStorage logStorage0 = createAndInitCustomLogStorage(ignite, groupId);

        // Destruction might be asynchronous.
        await().timeout(2, SECONDS).until(() -> logStorage0.getEntry(1), is(nullValue()));
    }

    private static void raisePersistedLwm(Path workDir, HybridTimestamp newLwm) {
        VaultService vaultService = new PersistentVaultService(IgnitePaths.vaultPath(workDir));

        try {
            vaultService.start();
            vaultService.put(LowWatermarkImpl.LOW_WATERMARK_VAULT_KEY, newLwm.toBytes());
        } finally {
            vaultService.close();
        }
    }

    private static void flushTxStateStorageToDisk(IgniteImpl ignite) {
        ignite.sharedTxStateStorage().flush();
    }

    private static void disallowLwmRaiseUntilRestart(IgniteImpl ignite0) {
        ignite0.transactions().begin(new TransactionOptions().readOnly(true));
    }

    private static HybridTimestamp latestCatalogVersionTs(IgniteImpl ignite) {
        return HybridTimestamp.hybridTimestamp(ignite.catalogManager().latestCatalog().time());
    }

    private void createZoneAndTableWith1Partition(int replicas) {
        executeUpdate(
                "CREATE ZONE " + ZONE_NAME + " (REPLICAS " + replicas + ", PARTITIONS 1) STORAGE PROFILES ['"
                        + DEFAULT_AIPERSIST_PROFILE_NAME + "']"
        );
        executeUpdate("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR) ZONE test_zone");
    }

    private void executeUpdate(String query) {
        SqlTestUtils.executeUpdate(query, cluster.aliveNode().sql());
    }

    private void makePutInExplicitTxToTestTable() {
        Ignite ignite = cluster.aliveNode();
        Table table = ignite.tables().table(TABLE_NAME);

        ignite.transactions().runInTransaction(tx -> {
            table.keyValueView(Integer.class, String.class).put(tx, 1, "one");
        });
    }

    private int testTableId() {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.node(0));
        TableImpl tableImpl = unwrapTableImpl(ignite.tables().table(TABLE_NAME));
        return tableImpl.tableId();
    }

    private int zoneId(int tableId) {
        CatalogTableDescriptor table = igniteImpl(0)
                .catalogManager()
                .activeCatalog(Long.MAX_VALUE)
                .table(tableId);
        assertThat(table, is(notNullValue()));

        return table.zoneId();
    }

    private static void makeSurePartitionExistsOnDisk(IgniteImpl ignite, int tableId, ZonePartitionId replicationGroupId) {
        makeSurePartitionMvDataExistsOnDisk(ignite, tableId);

        assertTrue(hasSomethingInTxStateStorage(ignite, replicationGroupId));

        LogStorage logStorage = partitionLogStorage(ignite, replicationGroupId);
        assertThat(logStorage.getLastLogIndex(), is(greaterThan(0L)));

        File raftMetaFile = partitionRaftMetaFile(ignite, replicationGroupId);
        assertThat(raftMetaFile, is(anExistingFile()));
    }

    private static void makeSurePartitionMvDataExistsOnDisk(IgniteImpl ignite, int tableId) {
        File partitionFile = testTablePartition0File(ignite, tableId);
        assertThat(partitionFile, is(anExistingFile()));
    }

    private static boolean hasSomethingInTxStateStorage(IgniteImpl ignite, PartitionGroupId replicationGroupId) {
        byte[] lowerBound = txStatePartitionStartPrefix(replicationGroupId);
        byte[] upperBound = txStatePartitionEndPrefix(replicationGroupId);

        try (
                ReadOptions readOptions = new ReadOptions().setIterateUpperBound(new Slice(upperBound));
                RocksIterator iterator = ignite.sharedTxStateStorage().txStateColumnFamily().newIterator(readOptions)
        ) {
            iterator.seek(lowerBound);

            if (iterator.isValid()) {
                try {
                    iterator.status();
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }

                return true;
            } else {
                return false;
            }
        }
    }

    private static byte[] txStatePartitionStartPrefix(PartitionGroupId replicationGroupId) {
        //noinspection NumericCastThatLosesPrecision
        return ByteBuffer.allocate(TxStateRocksDbPartitionStorage.PREFIX_SIZE_BYTES).order(BIG_ENDIAN)
                .putInt(replicationGroupId.objectId())
                .putShort((short) replicationGroupId.partitionId())
                .array();
    }

    private static byte[] txStatePartitionEndPrefix(PartitionGroupId replicationGroupId) {
        //noinspection NumericCastThatLosesPrecision
        return ByteBuffer.allocate(TxStateRocksDbPartitionStorage.PREFIX_SIZE_BYTES).order(BIG_ENDIAN)
                .putInt(replicationGroupId.objectId())
                .putShort((short) (replicationGroupId.partitionId() + 1))
                .array();
    }

    private static File partitionRaftMetaFile(IgniteImpl ignite, ZonePartitionId replicationGroupId) {
        String relativePath = partitionRaftNodeIdStringForStorage(ignite, replicationGroupId) + "/meta/raft_meta";
        Path raftMetaFilePath = ignite.partitionsWorkDir().metaPath().resolve(relativePath);
        return raftMetaFilePath.toFile();
    }

    private static LogStorage partitionLogStorage(IgniteImpl ignite, ZonePartitionId replicationGroupId) {
        return ignite.partitionsLogStorageFactory().createLogStorage(
                partitionRaftNodeIdStringForStorage(ignite, replicationGroupId),
                new RaftOptions()
        );
    }

    private static String partitionRaftNodeIdStringForStorage(IgniteImpl ignite, ZonePartitionId replicationGroupId) {
        RaftNodeId partitionRaftNodeId = new RaftNodeId(replicationGroupId, new Peer(ignite.name()));
        return partitionRaftNodeId.nodeIdStringForStorage();
    }

    private static File testTablePartition0File(IgniteImpl ignite, int tableId) {
        return ignite.partitionsWorkDir().dbPath().resolve("db/table-" + tableId + "/part-" + PARTITION_ID + ".bin").toFile();
    }

    private static void verifyPartitionGetsFullyRemovedFromDisk(
            IgniteImpl ignite,
            int tableId,
            ZonePartitionId replicationGroupId
    ) throws InterruptedException {
        verifyPartitionGetsRemovedFromDisk(ignite, tableId, replicationGroupId, true);
    }

    private static void verifyPartitionMvDataGetsRemovedFromDisk(
            IgniteImpl ignite,
            int tableId,
            ZonePartitionId replicationGroupId
    ) throws InterruptedException {
        verifyPartitionGetsRemovedFromDisk(ignite, tableId, replicationGroupId, false);
    }

    private static void verifyPartitionGetsRemovedFromDisk(
            IgniteImpl ignite,
            int tableId,
            ZonePartitionId replicationGroupId,
            boolean concernNonMvData
    ) throws InterruptedException {
        File partitionFile = testTablePartition0File(ignite, tableId);
        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Partition file " + partitionFile.getAbsolutePath() + " was not removed in time.",
                            !partitionFile.exists()
                    );
                });

        if (concernNonMvData) {
            await().atMost(10, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(
                                "Tx state storage was not destroyed in time.",
                                !hasSomethingInTxStateStorage(ignite, replicationGroupId)
                        );
                    });

            await().atMost(10, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(
                                "Partition Raft log was not removed in time.",
                                partitionLogStorage(ignite, replicationGroupId).getLastLogIndex(), is(0L)
                        );
                    });

            File raftMetaFile = partitionRaftMetaFile(ignite, replicationGroupId);
            await().atMost(10, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(
                                "Partition Raft meta file " + raftMetaFile.getAbsolutePath() + " was not removed in time.",
                                !raftMetaFile.exists()
                        );
                    });
        }
    }

    private static void verifyPartitionNonMvDataExistsOnDisk(IgniteImpl ignite, ZonePartitionId replicationGroupId) {
        assertTrue(hasSomethingInTxStateStorage(ignite, replicationGroupId), "Tx state storage was unexpectedly destroyed");

        assertTrue(partitionLogStorage(ignite, replicationGroupId).getLastLogIndex() > 0L, "Partition Raft log was unexpectedly removed.");

        File raftMetaFile = partitionRaftMetaFile(ignite, replicationGroupId);
        assertTrue(raftMetaFile.exists(), "Partition Raft meta file " + raftMetaFile.getAbsolutePath() + " was unexpectedly removed.");
    }

    private static void waitTillCatalogDoesNotContainTargetTable(IgniteImpl ignite, String tableName) throws InterruptedException {
        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Did not observe catalog truncation in time.",
                            !catalogHistoryContainsTargetTable(ignite, tableName)
                    );
                });
    }

    private static boolean catalogHistoryContainsTargetTable(IgniteImpl ignite, String tableName) {
        CatalogManager catalogManager = ignite.catalogManager();

        Set<String> tableNames = IntStream.rangeClosed(catalogManager.earliestCatalogVersion(), catalogManager.latestCatalogVersion())
                .mapToObj(catalogVersion -> {
                    try {
                        return catalogManager.catalog(catalogVersion);
                    } catch (CatalogNotFoundException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .flatMap(catalog -> catalog.tables().stream().map(CatalogTableDescriptor::name))
                .collect(toSet());

        return tableNames.contains(tableName);
    }

    private void createCatalogVersionNotRelatedToPartitions() {
        executeUpdate("CREATE SCHEMA UNRELATED_SCHEMA");
    }

    @Test
    void testPartitionIsDestroyedWhenItIsEvictedFromNode() throws Exception {
        cluster.startAndInit(2);

        createZoneAndTableWith1Partition(2);
        makePutInExplicitTxToTestTable();

        int tableId = testTableId();
        ZonePartitionId replicationGroupId = new ZonePartitionId(zoneId(tableId), PARTITION_ID);

        IgniteImpl ignite0 = unwrapIgniteImpl(cluster.node(0));
        IgniteImpl ignite1 = unwrapIgniteImpl(cluster.node(1));

        waitTillAssignmentCountReaches(2, replicationGroupId);

        makeSurePartitionExistsOnDisk(ignite0, tableId, replicationGroupId);
        makeSurePartitionExistsOnDisk(ignite1, tableId, replicationGroupId);

        executeUpdate("ALTER ZONE " + ZONE_NAME + " SET (REPLICAS 1)");

        IgniteImpl notHostingIgnite = nodeNotHostingPartition(replicationGroupId);
        verifyPartitionGetsFullyRemovedFromDisk(notHostingIgnite, tableId, replicationGroupId);
    }

    private void waitTillAssignmentCountReaches(int targetAssignmentCount, ZonePartitionId replicationGroupId)
            throws InterruptedException {
        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Did not see assignments count reaching.",
                            partitionAssignments(replicationGroupId).size() == targetAssignmentCount
                    );
                });
    }

    private Set<Assignment> partitionAssignments(ZonePartitionId replicationGroupId) {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());

        CompletableFuture<TokenizedAssignments> assignmentsFuture = ignite.placementDriver()
                .getAssignments(replicationGroupId, ignite.clock().now());
        assertThat(assignmentsFuture, willCompleteSuccessfully());

        TokenizedAssignments assignments = assignmentsFuture.join();
        assertThat(assignments, is(notNullValue()));

        return assignments.nodes();
    }

    private IgniteImpl nodeNotHostingPartition(ZonePartitionId replicationGroupId) throws InterruptedException {
        waitTillAssignmentCountReaches(1, replicationGroupId);

        Set<Assignment> assignments = partitionAssignments(replicationGroupId);
        assertThat(assignments, hasSize(1));
        Assignment assignment = assignments.iterator().next();

        return cluster.runningNodes()
                .filter(node -> !node.name().equals(assignment.consistentId()))
                .findAny()
                .map(TestWrappers::unwrapIgniteImpl)
                .orElseThrow();
    }

    private static void verifyAssignmentKeysWereRemovedFromMetaStorage(IgniteImpl ignite, ZonePartitionId zonePartitionId)
            throws InterruptedException {
        MetaStorageManager metaStorage = unwrapIgniteImpl(ignite).metaStorageManager();

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    Entry entry = metaStorage.getLocally(stablePartAssignmentsKey(zonePartitionId));
                    assertThat(
                            "Stable assignments were not removed from meta storage in time.",
                            entry.tombstone() || entry.empty()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    Entry entry = metaStorage.getLocally(pendingPartAssignmentsQueueKey(zonePartitionId));
                    assertThat(
                            "Pending assignments were not removed from meta storage in time.",
                            entry.tombstone() || entry.empty()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    Entry entry = metaStorage.getLocally(pendingChangeTriggerKey(zonePartitionId));
                    assertThat(
                            "Pending change trigger key was not removed from meta storage in time.",
                            entry.tombstone() || entry.empty()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    Entry entry = metaStorage.getLocally(plannedPartAssignmentsKey(zonePartitionId));
                    assertThat(
                            "Planned assignments were not removed from meta storage in time.",
                            entry.tombstone() || entry.empty()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    Entry entry = metaStorage.getLocally(switchAppendKey(zonePartitionId));
                    assertThat(
                            "Switch append assignments were not removed from meta storage in time.",
                            entry.tombstone() || entry.empty()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    Entry entry = metaStorage.getLocally(switchReduceKey(zonePartitionId));
                    assertThat(
                            "Switch reduce assignments were not removed from meta storage in time.",
                            entry.tombstone() || entry.empty()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    Entry entry = metaStorage.getLocally(assignmentsChainKey(zonePartitionId));
                    assertThat(
                            "Assignments chain was not removed from meta storage in time.",
                            entry.tombstone() || entry.empty()
                    );
                });
    }

    private static void verifyZoneDistributionZoneManagerResourcesArePresent(IgniteImpl ignite, int zoneId) throws InterruptedException {
        MetaStorageManager metaStorage = unwrapIgniteImpl(ignite).metaStorageManager();

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Zone data nodes are not present in meta storage.",
                            !metaStorage.getLocally(zoneDataNodesHistoryKey(zoneId)).empty()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Zone scale up timer is not present in meta storage.",
                            !metaStorage.getLocally(zoneScaleUpTimerKey(zoneId)).empty()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Zone scale down timer is not present in meta storage.",
                            !metaStorage.getLocally(zoneScaleDownTimerKey(zoneId)).empty()
                    );
                });
    }

    private static void verifyZoneDistributionZoneManagerResourcesWereRemovedFromMetaStorage(IgniteImpl ignite, int zoneId)
            throws InterruptedException {
        MetaStorageManager metaStorage = unwrapIgniteImpl(ignite).metaStorageManager();

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Zone data nodes were not removed from meta storage in time.",
                            metaStorage.getLocally(zoneDataNodesHistoryKey(zoneId)).tombstone()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Zone scale up timer was not removed from meta storage in time.",
                            metaStorage.getLocally(zoneScaleUpTimerKey(zoneId)).tombstone()
                    );
                });

        await().atMost(10, SECONDS)
                .untilAsserted(() -> {
                    assertThat(
                            "Zone scale down timer was not removed from meta storage in time.",
                            metaStorage.getLocally(zoneScaleDownTimerKey(zoneId)).tombstone()
                    );
                });
    }

    private static LogStorage createAndInitCustomLogStorage(IgniteImpl ignite, String groupId) {
        RaftOptions raftOptions = ignite.raftManager().server().options().getRaftOptions();
        LogStorage logStorage = ignite.partitionsLogStorageFactory().createLogStorage(groupId, raftOptions);

        LogStorageOptions logStorageOptions = new LogStorageOptions();
        logStorageOptions.setConfigurationManager(new ConfigurationManager());
        logStorageOptions.setLogEntryCodecFactory(DefaultLogEntryCodecFactory.getInstance());

        logStorage.init(logStorageOptions);
        return logStorage;
    }
}
