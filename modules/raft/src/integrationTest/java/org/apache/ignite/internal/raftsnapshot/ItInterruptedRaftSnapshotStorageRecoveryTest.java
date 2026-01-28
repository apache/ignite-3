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

package org.apache.ignite.internal.raftsnapshot;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbPartitionStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.table.KeyValueView;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
class ItInterruptedRaftSnapshotStorageRecoveryTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";
    private static final String ZONE_NAME = "TEST_ZONE";

    @InjectConfiguration("mock.profiles.default {engine = aipersist, sizeBytes = " + Constants.GiB + "}")
    private StorageConfiguration storageConfig;

    @InjectExecutorService
    private ExecutorService executor;

    @InjectExecutorService
    private ScheduledExecutorService scheduler;

    @Test
    void raftSnapshotWorksAfterNonFinishedRaftSnapshotInstallOnMvStorage() {
        testRaftSnapshotWorksAfterNonFinishedRaftSnapshotInstall(this::simulateNonFinishedRaftSnapshotInstallInMvStorage);
    }

    @Test
    void raftSnapshotWorksAfterNonFinishedRaftSnapshotInstallOnTxStateStorage() {
        testRaftSnapshotWorksAfterNonFinishedRaftSnapshotInstall(this::simulateNonFinishedRaftSnapshotInstallInTxStateStorage);
    }

    private void testRaftSnapshotWorksAfterNonFinishedRaftSnapshotInstall(Consumer<Path> executeOnStoppedNode2StoragePath) {
        createTableWithOnePartitionAnd3Replicas(TABLE_NAME);

        // Using explicit transaction to be sure that TxStateStorage gets written.
        putOneRowInExplicitTransaction();

        waitForAllNodesToHave1RowInTable(TABLE_NAME);

        // Truncate log prefix to force snapshot installation to node 2 when its storages will be cleared on startup.
        // This also causes flushes of both MV and TxState storages, so, after we simulate non-finished rebalance in either MV or
        // TX state storage and restart node 2, the corresponding storage data will not be rewritten by reapplying the log.
        truncateLogPrefixOnAllNodes(cluster.solePartitionId(ZONE_NAME));

        Path node2PartitionsDbPath = unwrapIgniteImpl(cluster.node(2)).partitionsWorkDir().dbPath();

        cluster.stopNode(2);

        executeOnStoppedNode2StoragePath.accept(node2PartitionsDbPath);

        Ignite node2 = cluster.startNode(2);

        waitForNodeToHave1RowInTable(TABLE_NAME, node2);
    }

    private void createTableWithOnePartitionAnd3Replicas(String tableName) {
        cluster.aliveNode().sql().executeScript("CREATE ZONE " + ZONE_NAME + " (PARTITIONS 1, REPLICAS 3) STORAGE PROFILES ['"
                + CatalogService.DEFAULT_STORAGE_PROFILE + "'];"
                + "CREATE TABLE " + tableName + " (ID INT PRIMARY KEY, VAL VARCHAR) ZONE " + ZONE_NAME + ";");
    }

    private void putOneRowInExplicitTransaction() {
        KeyValueView<Integer, String> kvView = cluster.aliveNode().tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);
        cluster.aliveNode().transactions().runInTransaction(tx -> {
            kvView.put(tx, 1, "one");
        });
    }

    private void waitForAllNodesToHave1RowInTable(String tableName) {
        cluster.nodes().parallelStream().forEach(node -> waitForNodeToHave1RowInTable(tableName, node));
    }

    private static void waitForNodeToHave1RowInTable(String tableName, Ignite node) {
        TableViewInternal table = unwrapTableViewInternal(node.tables().table(tableName));

        MvPartitionStorage partition = table.internalTable().storage().getMvPartition(0);
        assertThat(partition, is(notNullValue()));

        await().until(partition::estimatedSize, is(1L));
    }

    private void truncateLogPrefixOnAllNodes(ReplicationGroupId replicationGroupId) {
        cluster.nodes().parallelStream().forEach(node -> truncateLogPrefix(replicationGroupId, node));
    }

    private static void truncateLogPrefix(ReplicationGroupId replicationGroupId, Ignite node) {
        RaftGroupService raftGroupService = Cluster.raftGroupService(unwrapIgniteImpl(node), replicationGroupId);
        assertThat(raftGroupService, is(notNullValue()));

        doSnapshotOn(raftGroupService);
    }

    private static void doSnapshotOn(RaftGroupService raftGroupService) {
        var fut = new CompletableFuture<Status>();
        raftGroupService.getRaftNode().snapshot(fut::complete, true);

        assertThat(fut, willCompleteSuccessfully());
        assertEquals(RaftError.SUCCESS, fut.join().getRaftError());
    }

    private StorageTableDescriptor storageTableDescriptor() {
        TableViewInternal table = unwrapTableViewInternal(cluster.aliveNode().tables().table(TABLE_NAME));
        MvTableStorage mvTableStorage = table.internalTable().storage();
        return mvTableStorage.getTableDescriptor();
    }

    private int zoneId() {
        CatalogZoneDescriptor zone = unwrapIgniteImpl(cluster.aliveNode())
                .catalogManager()
                .latestCatalog()
                .zone(ZONE_NAME);

        assertThat(zone, is(notNullValue()));

        return zone.id();
    }

    private void simulateNonFinishedRaftSnapshotInstallInMvStorage(Path storagePath) {
        assertTrue(Files.exists(storagePath));
        assertTrue(Files.isDirectory(storagePath));

        var metricManager = new TestMetricManager();

        StorageEngine engine = createPersistentPageMemoryEngine(storagePath, metricManager);

        engine.start();

        try {
            assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

            StorageTableDescriptor tableDescriptor = storageTableDescriptor();

            MvTableStorage tableStorage = engine.createMvTable(tableDescriptor, new StorageIndexDescriptorSupplier() {
                @Override
                public @Nullable StorageIndexDescriptor get(int indexId) {
                    return null;
                }
            });
            assertThat(tableStorage.createMvPartition(0), willCompleteSuccessfully());
            assertThat(tableStorage.startRebalancePartition(0), willCompleteSuccessfully());

            // Flush to make sure it gets written to disk.
            flushPartition0(tableStorage);
        } finally {
            engine.stop();
        }
    }

    private StorageEngine createPersistentPageMemoryEngine(Path storagePath, MetricManager metricManager) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemoryStorageEngine(
                "test",
                metricManager,
                storageConfig,
                null,
                ioRegistry,
                storagePath,
                null,
                mock(FailureManager.class),
                mock(LogSyncer.class),
                scheduler,
                new HybridClockImpl()
        );
    }

    private static void flushPartition0(MvTableStorage tableStorage) {
        MvPartitionStorage partitionStorage = tableStorage.getMvPartition(0);
        assertThat(partitionStorage, is(notNullValue()));
        assertThat(partitionStorage.flush(), willCompleteSuccessfully());
    }

    private void simulateNonFinishedRaftSnapshotInstallInTxStateStorage(Path storagePath) {
        Path txStateStoragePath = storagePath.resolve("tx-state");
        assertTrue(Files.exists(txStateStoragePath));
        assertTrue(Files.isDirectory(txStateStoragePath));

        var sharedStorage = new TxStateRocksDbSharedStorage(
                "test",
                txStateStoragePath,
                scheduler,
                executor,
                mock(LogSyncer.class),
                mock(FailureProcessor.class),
                () -> 0
        );
        assertThat(sharedStorage.startAsync(new ComponentContext()), willCompleteSuccessfully());

        try {
            var zoneTxStateStorage = new TxStateRocksDbStorage(zoneId(), storageTableDescriptor().getPartitions(), sharedStorage);
            zoneTxStateStorage.start();

            TxStateRocksDbPartitionStorage txStatePartitionStorage = zoneTxStateStorage.getOrCreatePartitionStorage(0);
            assertThat(txStatePartitionStorage.startRebalance(), willCompleteSuccessfully());

            // Flush to make sure it gets written to disk.
            sharedStorage.flush();
        } finally {
            assertThat(sharedStorage.stopAsync(), willCompleteSuccessfully());
        }
    }
}
