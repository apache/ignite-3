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
import static org.apache.ignite.internal.index.IndexManagementUtils.isMetastoreKeyAbsentLocally;
import static org.apache.ignite.internal.index.IndexManagementUtils.isMetastoreKeysPresentLocally;
import static org.apache.ignite.internal.index.IndexManagementUtils.makeIndexAvailableInCatalogWithoutFuture;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKeyPrefix;
import static org.apache.ignite.internal.index.IndexManagementUtils.putBuildIndexMetastoreKeysIfAbsent;
import static org.apache.ignite.internal.index.IndexManagementUtils.removeMetastoreKeyIfPresent;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.List;
import java.util.Objects;
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
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Component responsible for restoring the algorithm from {@link IndexBuildController} if a node fails at some step.
 *
 * <p>Approximate recovery algorithm:</p>
 * <ul>
 *     <li>For registered indexes: <ul>
 *         <li>If the new index did not have time to add
 *         {@link IndexManagementUtils#putBuildIndexMetastoreKeysIfAbsent(MetaStorageManager, int, int) index building keys}, then add them
 *         to the metastore if they are <b>absent</b>.</li>
 *         <li>If there are no {@link IndexManagementUtils#partitionBuildIndexMetastoreKey(int, int) partition index building keys} left for
 *         the index in the metastore, then we {@link MakeIndexAvailableCommand make the index available} in the catalog.</li>
 *         <li>For partitions for which index building has not completed, we will wait until the primary replica is elected so that the
 *         replication log will be applied. If after this we find out that the index has been build, we will remove the
 *         {@link IndexManagementUtils#partitionBuildIndexMetastoreKey(int, int) partition index building key} from the metastore if it is
 *         <b>present</b>.</li>
 *     </ul></li>
 *     <li>For available indexes: <ul>
 *         <li>Delete the {@link IndexManagementUtils#inProgressBuildIndexMetastoreKey(int) “index construction in progress” key} in the
 *         metastore if it is <b>present</b>.</li>
 *     </ul></li>
 * </ul>
 */
public class IndexAvailabilityRestorer implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(IndexAvailabilityRestorer.class);

    private final CatalogManager catalogManager;

    private final MetaStorageManager metaStorageManager;

    private final IndexManager indexManager;

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Constructor. */
    public IndexAvailabilityRestorer(
            CatalogManager catalogManager,
            MetaStorageManager metaStorageManager,
            IndexManager indexManager,
            PlacementDriver placementDriver,
            HybridClock clock
    ) {
        this.catalogManager = catalogManager;
        this.metaStorageManager = metaStorageManager;
        this.indexManager = indexManager;
        this.placementDriver = placementDriver;
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
    public CompletableFuture<Void> recovery(long recoveryRevision) {
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

        if (isMetastoreKeysPresentLocally(metaStorageManager, partitionBuildIndexMetastoreKeyPrefix(indexId), recoveryRevision)) {
            // Without wait, since the deploy metastore watches will be only after the start of the components is completed and there will
            // be dead lock.
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
                    .map(Entry::value)
                    .filter(Objects::nonNull)
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

        TablePartitionId replicationGroupId = new TablePartitionId(tableId, partitionId);

        CompletableFuture<ReplicaMeta> getPrimaryReplicaFuture = placementDriver.getPrimaryReplica(replicationGroupId, clock.now());

        return indexManager.getMvTableStorage(recoveryRevision, tableId)
                .thenCombine(getPrimaryReplicaFuture, (mvTableStorage, replicaMeta) -> inBusyLockAsync(busyLock, () -> {
                    IndexStorage indexStorage = mvTableStorage.getIndex(partitionId, indexId);

                    assert indexStorage != null : "indexId=" + indexId + ", partitionId=" + partitionId;

                    if (indexStorage.getNextRowIdToBuild() != null) {
                        // Building of the index has not yet been completed, so we have nothing to do yet.
                        return completedFuture(null);
                    }

                    // Since we know that the index has already been build, we do not need to wait for the primary node for it to delete the
                    // key. Also, since the index has already been built, the event of building the index during recovery (or quickly enough
                    // after it) will not happen.
                    return removeMetastoreKeyIfPresent(metaStorageManager, partitionBuildIndexMetastoreKey(indexId, partitionId));
                }));
    }
}
