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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.index.IndexManagementUtils.getPartitionCountFromCatalog;
import static org.apache.ignite.internal.index.IndexManagementUtils.inProgressBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.isAnyMetastoreKeyPresentLocally;
import static org.apache.ignite.internal.index.IndexManagementUtils.isMetastoreKeyAbsentLocally;
import static org.apache.ignite.internal.index.IndexManagementUtils.isPrimaryReplica;
import static org.apache.ignite.internal.index.IndexManagementUtils.makeIndexAvailableInCatalogWithoutFuture;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKeyPrefix;
import static org.apache.ignite.internal.index.IndexManagementUtils.putBuildIndexMetastoreKeysIfAbsent;
import static org.apache.ignite.internal.index.IndexManagementUtils.removeMetastoreKeyIfPresent;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * Component responsible for restoring the algorithm from {@link IndexAvailabilityController} if a node fails at some step.
 *
 * <p>Approximate recovery algorithm:</p>
 * <ul>
 *     <li>For registered indexes: <ul>
 *         <li>If the new index did not have time to add
 *         {@link IndexManagementUtils#putBuildIndexMetastoreKeysIfAbsent(MetaStorageManager, int, int) index building keys}, then add them
 *         to the metastore if they are <b>absent</b>.</li>
 *         <li>If there are no {@link IndexManagementUtils#partitionBuildIndexMetastoreKey(int, int) partition index building keys} left for
 *         the index in the metastore, then we {@link MakeIndexAvailableCommand make the index available} in the catalog.</li>
 *         <li>For partitions for which index building has not completed, we will wait until the primary replica is elected (which will make
 *         sure it has applied all the commands from the replication log). If after this we find out that the index has been built, we will
 *         remove the {@link IndexManagementUtils#partitionBuildIndexMetastoreKey(int, int) partition index building key} from the metastore
 *         if it is <b>present</b>.</li>
 *     </ul></li>
 *     <li>For available indexes: <ul>
 *         <li>Delete the {@link IndexManagementUtils#inProgressBuildIndexMetastoreKey(int) “index construction from progress” key} in the
 *         metastore if it is <b>present</b>.</li>
 *     </ul></li>
 * </ul>
 */
public class IndexAvailabilityControllerRestorer implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(IndexAvailabilityControllerRestorer.class);

    private final CatalogManager catalogManager;

    private final MetaStorageManager metaStorageManager;

    private final IndexManager indexManager;

    private final PlacementDriver placementDriver;

    private final ClusterService clusterService;

    private final HybridClock clock;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Constructor. */
    public IndexAvailabilityControllerRestorer(
            CatalogManager catalogManager,
            MetaStorageManager metaStorageManager,
            IndexManager indexManager,
            PlacementDriver placementDriver,
            ClusterService clusterService,
            HybridClock clock
    ) {
        this.catalogManager = catalogManager;
        this.metaStorageManager = metaStorageManager;
        this.indexManager = indexManager;
        this.placementDriver = placementDriver;
        this.clusterService = clusterService;
        this.clock = clock;
    }

    /**
     * Recovers index availability.
     *
     * <p>NOTE: Should only be executed on node recovery.</p>
     *
     * @param recoveryRevision Metastore revision on recovery.
     * @return Future of recovery execution.
     */
    public CompletableFuture<Void> recover(long recoveryRevision) {
        return inBusyLockAsync(busyLock, () -> {
            // It is expected that the method will only be called on recovery, when the deploy of metastore watches has not yet occurred.
            int catalogVersion = catalogManager.latestCatalogVersion();

            List<CompletableFuture<?>> futures = catalogManager.indexes(catalogVersion).stream()
                    .map(indexDescriptor -> {
                        if (indexDescriptor.available()) {
                            return recoveryForAvailableIndexBusy(indexDescriptor, recoveryRevision);
                        } else {
                            return recoveryForRegisteredIndexBusy(indexDescriptor, recoveryRevision, catalogVersion);
                        }
                    })
                    .collect(toList());

            return allOf(futures.toArray(CompletableFuture[]::new));
        });
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    private CompletableFuture<?> recoveryForAvailableIndexBusy(CatalogIndexDescriptor indexDescriptor, long recoveryRevision) {
        assert indexDescriptor.available() : indexDescriptor.id();

        int indexId = indexDescriptor.id();

        ByteArray inProgressBuildIndexMetastoreKey = inProgressBuildIndexMetastoreKey(indexId);

        if (isMetastoreKeyAbsentLocally(metaStorageManager, inProgressBuildIndexMetastoreKey, recoveryRevision)) {
            return completedFuture(null);
        }

        return removeMetastoreKeyIfPresent(metaStorageManager, inProgressBuildIndexMetastoreKey);
    }

    private CompletableFuture<?> recoveryForRegisteredIndexBusy(
            CatalogIndexDescriptor indexDescriptor,
            long recoveryRevision,
            int catalogVersion
    ) {
        assert !indexDescriptor.available() : indexDescriptor.id();

        int indexId = indexDescriptor.id();

        if (isMetastoreKeyAbsentLocally(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId), recoveryRevision)) {
            // After creating the index, we did not have time to create the keys for building the index in the metastore.
            return putBuildIndexMetastoreKeysIfAbsent(
                    metaStorageManager,
                    indexId,
                    getPartitionCountFromCatalog(catalogManager, indexId, catalogVersion)
            );
        }

        if (!isAnyMetastoreKeyPresentLocally(metaStorageManager, partitionBuildIndexMetastoreKeyPrefix(indexId), recoveryRevision)) {
            // Without wait, since the metastore watches deployment will be only after the start of the components is completed and this
            // will cause a dead lock.
            makeIndexAvailableInCatalogWithoutFuture(catalogManager, indexId, LOG);

            return completedFuture(null);
        }

        return recoveryForRemainingPartitionsOfRegisteredIndexBusy(indexDescriptor, recoveryRevision);
    }

    private CompletableFuture<?> recoveryForRemainingPartitionsOfRegisteredIndexBusy(
            CatalogIndexDescriptor indexDescriptor,
            long recoveryRevision
    ) {
        assert !indexDescriptor.available() : indexDescriptor.id();

        int indexId = indexDescriptor.id();

        try (Cursor<Entry> cursor = metaStorageManager.prefixLocally(partitionBuildIndexMetastoreKeyPrefix(indexId), recoveryRevision)) {
            CompletableFuture<?>[] futures = cursor.stream()
                    .filter(entry -> entry.value() != null)
                    .map(Entry::key)
                    .map(IndexManagementUtils::toPartitionBuildIndexMetastoreKeyString)
                    .mapToInt(IndexManagementUtils::extractPartitionIdFromPartitionBuildIndexKey)
                    .mapToObj(partitionId -> recoveryForPartitionOfRegisteredIndexBusy(indexDescriptor, partitionId, recoveryRevision))
                    .toArray(CompletableFuture[]::new);

            return allOf(futures);
        }
    }

    private CompletableFuture<?> recoveryForPartitionOfRegisteredIndexBusy(
            CatalogIndexDescriptor indexDescriptor,
            int partitionId,
            long recoveryRevision
    ) {
        int indexId = indexDescriptor.id();
        int tableId = indexDescriptor.tableId();

        return indexManager.getMvTableStorage(recoveryRevision, tableId)
                .thenCompose(mvTableStorage -> inBusyLockAsync(busyLock, () -> {
                    var replicationGroupId = new TablePartitionId(tableId, partitionId);

                    return placementDriver.getPrimaryReplica(replicationGroupId, clock.now())
                            .thenCompose(primaryReplicaMeta -> inBusyLockAsync(busyLock, () -> {
                                ClusterNode localNode = clusterService.topologyService().localMember();

                                if (primaryReplicaMeta == null || !isPrimaryReplica(primaryReplicaMeta, localNode, clock.now())) {
                                    // Local node is not the primary replica, so we expect a primary replica to be elected (which will make
                                    // sure it has applied all the commands from the replication log). If a local node is elected, then
                                    // IndexAvailabilityController will get rid of the partitionBuildIndexMetastoreKey from the metastore on
                                    // its own by IndexBuildCompletionListener.onBuildCompletion event.
                                    return completedFuture(null);
                                }

                                IndexStorage indexStorage = mvTableStorage.getIndex(partitionId, indexId);

                                assert indexStorage != null : "indexId=" + indexId + ", partitionId=" + partitionId;

                                if (indexStorage.getNextRowIdToBuild() != null) {
                                    // Building of the index has not yet been completed, so we have nothing to do yet.
                                    return completedFuture(null);
                                }

                                return removeMetastoreKeyIfPresent(
                                        metaStorageManager,
                                        partitionBuildIndexMetastoreKey(indexId, partitionId)
                                );
                            }));
                }));
    }
}
