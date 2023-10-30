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

package org.apache.ignite.internal.index;

import static java.util.Collections.emptyIterator;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.index.IndexManagementUtils.inProgressBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.assertMetastoreKeyAbsent;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.assertMetastoreKeyPresent;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.awaitTillGlobalMetastoreRevisionIsApplied;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createIndex;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.indexDescriptor;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.indexId;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.makeIndexAvailable;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.tableId;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.TestRocksDbKeyValueStorage;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** For {@link IndexAvailabilityControllerRestorer} testing. */
@ExtendWith(WorkDirectoryExtension.class)
public class IndexAvailabilityControllerRestorerTest extends BaseIgniteAbstractTest {
    @WorkDirectory
    private Path workDir;

    private final HybridClock clock = new HybridClockImpl();

    private final PlacementDriver placementDriver = mock(PlacementDriver.class);

    private final ClusterService clusterService = mock(ClusterService.class);

    private final IndexManager indexManager = mock(IndexManager.class);

    private final VaultManager vaultManager = new VaultManager(new InMemoryVaultService());

    private KeyValueStorage keyValueStorage;

    private MetaStorageManagerImpl metaStorageManager;

    private CatalogManager catalogManager;

    private IndexAvailabilityControllerRestorer restorer;

    @BeforeEach
    void setUp() throws Exception {
        keyValueStorage = new TestRocksDbKeyValueStorage(NODE_NAME, workDir);

        metaStorageManager = StandaloneMetaStorageManager.create(vaultManager, keyValueStorage);

        catalogManager = CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock, metaStorageManager);

        Stream.of(vaultManager, metaStorageManager, catalogManager).forEach(IgniteComponent::start);

        deployWatches();

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                restorer == null ? null : restorer::close,
                catalogManager == null ? null : catalogManager::stop,
                metaStorageManager == null ? null : metaStorageManager::stop,
                vaultManager::stop
        );
    }

    @Test
    void testRemoveInProgressBuildIndexMetastoreKeyForAvailableIndexes() throws Exception {
        createIndex(catalogManager, TABLE_NAME, INDEX_NAME + 0, COLUMN_NAME);
        createIndex(catalogManager, TABLE_NAME, INDEX_NAME + 1, COLUMN_NAME);

        int indexId0 = indexId(catalogManager, INDEX_NAME + 0, clock);
        int indexId1 = indexId(catalogManager, INDEX_NAME + 1, clock);

        makeIndexAvailable(catalogManager, indexId0);
        makeIndexAvailable(catalogManager, indexId1);

        // Let's put the inProgressBuildIndexMetastoreKey for only one index in the metastore.
        putInProgressBuildIndexMetastoreKeyInMetastore(indexId0);

        restartComponentsAndPerformRecovery();

        // Let's do checks.
        assertMetastoreKeyAbsent(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId0));
        assertMetastoreKeyAbsent(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId1));

        assertTrue(indexDescriptor(catalogManager, INDEX_NAME + 0, clock).available());
        assertTrue(indexDescriptor(catalogManager, INDEX_NAME + 1, clock).available());
    }

    @Test
    void testMakeIndexAvailableIfNoLeftKeysBuildingIndexForPartitionInMetastore() throws Exception {
        createIndex(catalogManager, TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        int indexId = indexId(catalogManager, INDEX_NAME, clock);

        putInProgressBuildIndexMetastoreKeyInMetastore(indexId);

        restartComponentsAndPerformRecovery();

        // Let's do checks.
        assertMetastoreKeyPresent(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId));
        assertTrue(indexDescriptor(catalogManager, INDEX_NAME, clock).available());
    }

    @Test
    void testRemovePartitionBuildIndexMetastoreKeyForRegisteredIndex() throws Exception {
        createIndex(catalogManager, TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        int tableId = tableId(catalogManager, TABLE_NAME, clock);
        int indexId = indexId(catalogManager, INDEX_NAME, clock);
        int partitionId = 0;

        putInProgressBuildIndexMetastoreKeyInMetastore(indexId);
        putPartitionBuildIndexMetastoreKeyInMetastore(indexId, partitionId);

        TablePartitionId replicaGroupId = new TablePartitionId(tableId, partitionId);
        ClusterNode localNode = new ClusterNodeImpl(NODE_NAME + "_ID", NODE_NAME, mock(NetworkAddress.class));

        HybridTimestamp startTime = clock.now();
        HybridTimestamp expirationTime = startTime.addPhysicalTime(TimeUnit.DAYS.toMillis(1));
        ReplicaMeta primaryReplicaMeta = newPrimaryReplicaMeta(localNode, replicaGroupId, startTime, expirationTime);

        // An empty array on purpose.
        setIndexStorageToIndexManager(replicaGroupId, indexId);
        setLocalNodeToClusterService(localNode);
        setPrimaryReplicaMetaToPlacementDriver(replicaGroupId, primaryReplicaMeta);

        restartComponentsAndPerformRecovery();

        // Let's do checks.
        assertMetastoreKeyPresent(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId));
        assertMetastoreKeyAbsent(metaStorageManager, partitionBuildIndexMetastoreKey(indexId, partitionId));

        assertFalse(indexDescriptor(catalogManager, INDEX_NAME, clock).available());
    }

    @Test
    void testNotRemovePartitionBuildIndexMetastoreKeyForRegisteredIndexIfBuildingIndexNotComplete() throws Exception {
        createIndex(catalogManager, TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        int tableId = tableId(catalogManager, TABLE_NAME, clock);
        int indexId = indexId(catalogManager, INDEX_NAME, clock);
        int partitionId = 0;

        putInProgressBuildIndexMetastoreKeyInMetastore(indexId);
        putPartitionBuildIndexMetastoreKeyInMetastore(indexId, partitionId);

        TablePartitionId replicaGroupId = new TablePartitionId(tableId, partitionId);
        ClusterNode localNode = new ClusterNodeImpl(NODE_NAME + "_ID", NODE_NAME, mock(NetworkAddress.class));

        HybridTimestamp startTime = clock.now();
        HybridTimestamp expirationTime = startTime.addPhysicalTime(TimeUnit.DAYS.toMillis(1));
        ReplicaMeta primaryReplicaMeta = newPrimaryReplicaMeta(localNode, replicaGroupId, startTime, expirationTime);

        setIndexStorageToIndexManager(replicaGroupId, indexId, new RowId(partitionId));
        setLocalNodeToClusterService(localNode);
        setPrimaryReplicaMetaToPlacementDriver(replicaGroupId, primaryReplicaMeta);

        restartComponentsAndPerformRecovery();

        // Let's do checks.
        assertMetastoreKeyPresent(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId));
        assertMetastoreKeyPresent(metaStorageManager, partitionBuildIndexMetastoreKey(indexId, partitionId));

        assertFalse(indexDescriptor(catalogManager, INDEX_NAME, clock).available());
    }

    @Test
    void testNotRemovePartitionBuildIndexMetastoreKeyForRegisteredIndexIfPrimaryReplicaMetaNull() throws Exception {
        createIndex(catalogManager, TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        int tableId = tableId(catalogManager, TABLE_NAME, clock);
        int indexId = indexId(catalogManager, INDEX_NAME, clock);
        int partitionId = 0;

        putInProgressBuildIndexMetastoreKeyInMetastore(indexId);
        putPartitionBuildIndexMetastoreKeyInMetastore(indexId, partitionId);

        TablePartitionId replicaGroupId = new TablePartitionId(tableId, partitionId);
        ClusterNode localNode = new ClusterNodeImpl(NODE_NAME + "_ID", NODE_NAME, mock(NetworkAddress.class));

        // An empty array on purpose.
        setIndexStorageToIndexManager(replicaGroupId, indexId);
        setLocalNodeToClusterService(localNode);
        setPrimaryReplicaMetaToPlacementDriver(replicaGroupId, null);

        restartComponentsAndPerformRecovery();

        // Let's do checks.
        assertMetastoreKeyPresent(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId));
        assertMetastoreKeyPresent(metaStorageManager, partitionBuildIndexMetastoreKey(indexId, partitionId));

        assertFalse(indexDescriptor(catalogManager, INDEX_NAME, clock).available());
    }

    @Test
    void testNotRemovePartitionBuildIndexMetastoreKeyForRegisteredIndexIfPrimaryReplicaMetaChanges() throws Exception {
        createIndex(catalogManager, TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        int tableId = tableId(catalogManager, TABLE_NAME, clock);
        int indexId = indexId(catalogManager, INDEX_NAME, clock);
        int partitionId = 0;

        putInProgressBuildIndexMetastoreKeyInMetastore(indexId);
        putPartitionBuildIndexMetastoreKeyInMetastore(indexId, partitionId);

        TablePartitionId replicaGroupId = new TablePartitionId(tableId, partitionId);
        ClusterNode localNode = new ClusterNodeImpl(NODE_NAME + "_ID", NODE_NAME, mock(NetworkAddress.class));

        // TODO: IGNITE-20678 There should be a node ID change only
        ClusterNode previousLocalNode = new ClusterNodeImpl(NODE_NAME + "_ID_OLD", NODE_NAME + "_OLD", mock(NetworkAddress.class));
        ReplicaMeta primaryReplicaMeta = newPrimaryReplicaMeta(previousLocalNode, replicaGroupId, clock.now(), clock.now());

        // An empty array on purpose.
        setIndexStorageToIndexManager(replicaGroupId, indexId);
        setLocalNodeToClusterService(localNode);
        setPrimaryReplicaMetaToPlacementDriver(replicaGroupId, primaryReplicaMeta);

        restartComponentsAndPerformRecovery();

        // Let's do checks.
        assertMetastoreKeyPresent(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId));
        assertMetastoreKeyPresent(metaStorageManager, partitionBuildIndexMetastoreKey(indexId, partitionId));

        assertFalse(indexDescriptor(catalogManager, INDEX_NAME, clock).available());
    }

    private void putInProgressBuildIndexMetastoreKeyInMetastore(int indexId) {
        assertThat(metaStorageManager.put(inProgressBuildIndexMetastoreKey(indexId), BYTE_EMPTY_ARRAY), willCompleteSuccessfully());
    }

    private void putPartitionBuildIndexMetastoreKeyInMetastore(int indexId, int partitionId) {
        assertThat(
                metaStorageManager.put(partitionBuildIndexMetastoreKey(indexId, partitionId), BYTE_EMPTY_ARRAY),
                willCompleteSuccessfully()
        );
    }

    private void restartComponentsAndPerformRecovery() throws Exception {
        stopAndRestartComponentsNoDeployWatches();

        assertThat(recoveryRestorer(), willCompleteSuccessfully());

        deployWatches();
    }

    private void stopAndRestartComponentsNoDeployWatches() throws Exception {
        awaitTillGlobalMetastoreRevisionIsApplied(metaStorageManager);

        IgniteUtils.closeAll(
                catalogManager == null ? null : catalogManager::stop,
                metaStorageManager == null ? null : metaStorageManager::stop
        );

        keyValueStorage = new TestRocksDbKeyValueStorage(NODE_NAME, workDir);

        metaStorageManager = StandaloneMetaStorageManager.create(vaultManager, keyValueStorage);

        catalogManager = spy(CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock, metaStorageManager));

        Stream.of(metaStorageManager, catalogManager).forEach(IgniteComponent::start);
    }

    private void deployWatches() throws Exception {
        assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());

        awaitTillGlobalMetastoreRevisionIsApplied(metaStorageManager);
    }

    private CompletableFuture<Void> recoveryRestorer() throws Exception {
        if (restorer != null) {
            restorer.close();
        }

        restorer = new IndexAvailabilityControllerRestorer(
                catalogManager,
                metaStorageManager,
                indexManager,
                placementDriver,
                clusterService,
                clock
        );

        CompletableFuture<Long> metastoreRecoveryFuture = metaStorageManager.recoveryFinishedFuture();

        assertThat(metastoreRecoveryFuture, willBe(greaterThan(0L)));

        return restorer.recover(metastoreRecoveryFuture.join());
    }

    private void setIndexStorageToIndexManager(TablePartitionId replicaGroupId, int indexId, RowId... rowIdsToBuild) {
        MvTableStorage mvTableStorage = mock(MvTableStorage.class);
        IndexStorage indexStorage = mock(IndexStorage.class);

        Iterator<RowId> it = nullOrEmpty(rowIdsToBuild) ? emptyIterator() : List.of(rowIdsToBuild).iterator();

        when(indexStorage.getNextRowIdToBuild()).then(invocation -> it.hasNext() ? it.next() : null);

        when(mvTableStorage.getIndex(replicaGroupId.partitionId(), indexId)).thenReturn(indexStorage);
        when(indexManager.getMvTableStorage(anyLong(), eq(replicaGroupId.tableId()))).thenReturn(completedFuture(mvTableStorage));
    }

    private void setLocalNodeToClusterService(ClusterNode clusterNode) {
        TopologyService topologyService = mock(TopologyService.class, invocation -> clusterNode);

        when(clusterService.topologyService()).thenReturn(topologyService);
    }

    private void setPrimaryReplicaMetaToPlacementDriver(TablePartitionId replicaGroupId, @Nullable ReplicaMeta primaryReplicaMeta) {
        when(placementDriver.getPrimaryReplica(eq(replicaGroupId), any())).thenReturn(completedFuture(primaryReplicaMeta));
    }

    private static ReplicaMeta newPrimaryReplicaMeta(
            ClusterNode clusterNode,
            TablePartitionId replicaGroupId,
            HybridTimestamp startTime,
            HybridTimestamp expirationTime
    ) {
        return new Lease(clusterNode.name(), startTime, expirationTime, replicaGroupId);
    }
}
