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
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.apache.ignite.internal.index.IndexManagementUtils.PARTITION_BUILD_INDEX_KEY_PREFIX;
import static org.apache.ignite.internal.index.IndexManagementUtils.extractIndexIdFromPartitionBuildIndexKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.getPartitionCountFromCatalog;
import static org.apache.ignite.internal.index.IndexManagementUtils.inProgressBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.index;
import static org.apache.ignite.internal.index.IndexManagementUtils.isAnyMetastoreKeyPresentLocally;
import static org.apache.ignite.internal.index.IndexManagementUtils.isMetastoreKeyAbsentLocally;
import static org.apache.ignite.internal.index.IndexManagementUtils.makeIndexAvailableInCatalogWithoutFuture;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKeyPrefix;
import static org.apache.ignite.internal.index.IndexManagementUtils.putBuildIndexMetastoreKeysIfAbsent;
import static org.apache.ignite.internal.index.IndexManagementUtils.removeMetastoreKeyIfPresent;
import static org.apache.ignite.internal.index.IndexManagementUtils.toPartitionBuildIndexMetastoreKeyString;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * This component is responsible for ensuring that an index, upon completion of a distributed index building for all partitions, becomes
 * available.
 *
 * <p>An approximate algorithm for making an index available:</p>
 * <ul>
 *     <li>On {@link CatalogEvent#INDEX_BUILDING},
 *     {@link IndexManagementUtils#putBuildIndexMetastoreKeysIfAbsent(MetaStorageManager, int, int) index building keys} are created in the
 *     metastore.</li>
 *     <li>Then it is expected that the distributed index building event will be triggered for all partitions via
 *     {@link IndexBuildCompletionListener} (from {@link IndexBuilder#listen}); as a result of each of these events, the corresponding
 *     {@link IndexManagementUtils#partitionBuildIndexMetastoreKey(int, int) partition building index key} will be deleted from
 *     metastore.</li>
 *     <li>When <b>all</b> the {@link IndexManagementUtils#partitionBuildIndexMetastoreKey(int, int) partition index building key} in the
 *     metastore are deleted, {@link MakeIndexAvailableCommand} will be executed for the corresponding index.</li>
 *     <li>At {@link CatalogEvent#INDEX_AVAILABLE},
 *     {@link IndexManagementUtils#inProgressBuildIndexMetastoreKey(int) in progress index building key} in the metastore will be
 *     deleted.</li>
 * </ul>
 *
 * <p>Notes:</p>
 * <ul>
 *     <li>At {@link CatalogEvent#INDEX_REMOVED},
 *     {@link IndexManagementUtils#putBuildIndexMetastoreKeysIfAbsent(MetaStorageManager, int, int) index building keys} in the metastore
 *     are deleted.</li>
 *     <li>Handling of {@link CatalogEvent#INDEX_BUILDING}, {@link CatalogEvent#INDEX_REMOVED}, {@link CatalogEvent#INDEX_AVAILABLE}
 *     and watch prefix {@link IndexManagementUtils#PARTITION_BUILD_INDEX_KEY_PREFIX} is made by the whole cluster (and only one node makes
 *     a write to the metastore) as these events are global, but only one node (a primary replica owning a partition) handles
 *     {@link IndexBuildCompletionListener#onBuildCompletion} (form {@link IndexBuilder#listen}) event.</li>
 *     <li>Restoring index availability occurs in {@link #recover(long)}.</li>
 * </ul>
 *
 * <p>Approximate recovery algorithm:</p>
 * <ul>
 *     <li>For building indexes: <ul>
 *         <li>If the building index did not have time to add
 *         {@link IndexManagementUtils#putBuildIndexMetastoreKeysIfAbsent(MetaStorageManager, int, int) index building keys}, then add them
 *         to the metastore if they are <b>absent</b>.</li>
 *         <li>If there are no {@link IndexManagementUtils#partitionBuildIndexMetastoreKey(int, int) partition index building keys} left for
 *         the index in the metastore, then we {@link MakeIndexAvailableCommand make the index available} in the catalog.</li>
 *         <li>For partitions for which index building has not completed, we will not take any action, since after the node starts,
 *         {@link PrimaryReplicaEvent#PRIMARY_REPLICA_ELECTED} will be fired that will trigger the building of indexes as during normal
 *         operation of the node. Which will lead to the usual index availability algorithm.</li>
 *     </ul></li>
 *     <li>For available indexes: <ul>
 *         <li>Delete the {@link IndexManagementUtils#inProgressBuildIndexMetastoreKey(int) “index construction from progress” key} in the
 *         metastore if it is <b>present</b>.</li>
 *     </ul></li>
 * </ul>
 *
 * @see CatalogIndexDescriptor#status()
 * @see CatalogIndexStatus
 */
// TODO: IGNITE-22520 проверить код тут
class IndexAvailabilityController implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(IndexAvailabilityController.class);

    private final CatalogManager catalogManager;

    private final MetaStorageManager metaStorageManager;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Constructor. */
    IndexAvailabilityController(CatalogManager catalogManager, MetaStorageManager metaStorageManager, IndexBuilder indexBuilder) {
        this.catalogManager = catalogManager;
        this.metaStorageManager = metaStorageManager;

        addListeners(catalogManager, metaStorageManager, indexBuilder);
    }

    @Override
    public void close() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    /**
     * Recovers index availability on node recovery.
     *
     * @param recoveryRevision Metastore revision on recovery.
     */
    public void recover(long recoveryRevision) {
        inBusyLock(busyLock, () -> {
            // It is expected that the method will only be called on recovery, when the deploy of metastore watches has not yet occurred.
            int catalogVersion = catalogManager.latestCatalogVersion();

            List<CompletableFuture<?>> futures = catalogManager.indexes(catalogVersion).stream()
                    .map(indexDescriptor -> {
                        switch (indexDescriptor.status()) {
                            case BUILDING:
                                return recoveryForBuildingIndexBusy(indexDescriptor, recoveryRevision, catalogVersion);
                            case AVAILABLE:
                                return recoveryForAvailableIndexBusy(indexDescriptor, recoveryRevision);
                            default:
                                return nullCompletedFuture();
                        }
                    })
                    .filter(not(CompletableFuture::isDone))
                    .collect(toList());

            allOf(futures.toArray(CompletableFuture[]::new)).whenComplete((unused, throwable) -> {
                if (throwable != null && !(unwrapCause(throwable) instanceof NodeStoppingException)) {
                    LOG.error("Error when trying to recover index availability", throwable);
                } else if (!futures.isEmpty()) {
                    LOG.debug("Successful recovery of index availability");
                }
            });
        });
    }

    private void addListeners(CatalogService catalogService, MetaStorageManager metaStorageManager, IndexBuilder indexBuilder) {
        catalogService.listen(CatalogEvent.INDEX_BUILDING, (StartBuildingIndexEventParameters parameters) -> {
            return onIndexBuilding(parameters).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.INDEX_REMOVED, (RemoveIndexEventParameters parameters) -> {
            return onIndexRemoved(parameters).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.INDEX_AVAILABLE, (MakeIndexAvailableEventParameters parameters) -> {
            return onIndexAvailable(parameters).thenApply(unused -> false);
        });

        metaStorageManager.registerPrefixWatch(ByteArray.fromString(PARTITION_BUILD_INDEX_KEY_PREFIX), new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                return onUpdatePartitionBuildIndexKey(event).thenApply(unused -> null);
            }

            @Override
            public void onError(Throwable e) {
                LOG.error("Error on handle partition build index key", e);
            }
        });

        indexBuilder.listen(new IndexBuildCompletionListener() {
            @Override
            public void onBuildCompletion(int indexId, int tableId, int partitionId) {
                onIndexBuildCompletionForPartition(indexId, partitionId);
            }
        });
    }

    private CompletableFuture<?> onIndexBuilding(StartBuildingIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int indexId = parameters.indexId();

            int partitions = getPartitionCountFromCatalog(catalogManager, indexId, parameters.catalogVersion());

            return putBuildIndexMetastoreKeysIfAbsent(metaStorageManager, indexId, partitions);
        });
    }

    private CompletableFuture<?> onIndexRemoved(RemoveIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int indexId = parameters.indexId();

            CatalogIndexDescriptor indexBeforeRemoval = index(catalogManager, indexId, parameters.catalogVersion() - 1);

            if (indexBeforeRemoval.status() == STOPPING) {
                // It has already been built, nothing do to here.
                return nullCompletedFuture();
            }

            int partitions = getPartitionCountFromCatalog(catalogManager, indexId, parameters.catalogVersion() - 1);

            ByteArray inProgressBuildIndexKey = inProgressBuildIndexMetastoreKey(indexId);

            List<Operation> removePartitionBuildIndexMetastoreKeyOperations = IntStream.range(0, partitions)
                    .mapToObj(partitionId -> partitionBuildIndexMetastoreKey(indexId, partitionId))
                    .map(Operations::remove)
                    .collect(toList());

            return metaStorageManager.invoke(
                    exists(inProgressBuildIndexKey),
                    concat(
                            List.of(remove(inProgressBuildIndexKey)),
                            removePartitionBuildIndexMetastoreKeyOperations
                    ),
                    List.of(noop())
            );
        });
    }

    private CompletableFuture<?> onIndexAvailable(MakeIndexAvailableEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            ByteArray inProgressBuildIndexMetastoreKey = inProgressBuildIndexMetastoreKey(parameters.indexId());

            return removeMetastoreKeyIfPresent(metaStorageManager, inProgressBuildIndexMetastoreKey);
        });
    }

    private CompletableFuture<?> onUpdatePartitionBuildIndexKey(WatchEvent event) {
        return inBusyLockAsync(busyLock, () -> {
            if (!event.single()) {
                // We don't need to handle keys on index creation or deletion.
                return nullCompletedFuture();
            }

            Entry entry = event.entryEvent().newEntry();

            if (entry.value() != null) {
                // In case an index was created when there was only one partition.
                return nullCompletedFuture();
            }

            String partitionBuildIndexKey = toPartitionBuildIndexMetastoreKeyString(entry.key());

            int indexId = extractIndexIdFromPartitionBuildIndexKey(partitionBuildIndexKey);

            long metastoreRevision = entry.revision();

            if (isAnyMetastoreKeyPresentLocally(metaStorageManager, partitionBuildIndexMetastoreKeyPrefix(indexId), metastoreRevision)
                    || isMetastoreKeyAbsentLocally(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId), metastoreRevision)) {
                return nullCompletedFuture();
            }

            // We will not wait for the command to be executed, since we will then find ourselves in a dead lock since we will not be able
            // to free the metastore thread.
            makeIndexAvailableInCatalogWithoutFuture(catalogManager, indexId, LOG);

            return nullCompletedFuture();
        });
    }

    private void onIndexBuildCompletionForPartition(int indexId, int partitionId) {
        inBusyLock(busyLock, () -> {
            ByteArray partitionBuildIndexKey = partitionBuildIndexMetastoreKey(indexId, partitionId);

            // Intentionally not waiting for the operation to complete or returning the future because it is not necessary.
            metaStorageManager
                    .invoke(exists(partitionBuildIndexKey), remove(partitionBuildIndexKey), noop())
                    .whenComplete((operationResult, throwable) -> {
                        if (throwable != null && !(unwrapCause(throwable) instanceof NodeStoppingException)) {
                            LOG.error(
                                    "Error processing the operation to delete the partition index building key: "
                                            + "[indexId={}, partitionId={}]",
                                    throwable,
                                    indexId, partitionId
                            );
                        }
                    });
        });
    }

    private CompletableFuture<?> recoveryForAvailableIndexBusy(CatalogIndexDescriptor indexDescriptor, long recoveryRevision) {
        assert indexDescriptor.status() == AVAILABLE : indexDescriptor.id();

        int indexId = indexDescriptor.id();

        ByteArray inProgressBuildIndexMetastoreKey = inProgressBuildIndexMetastoreKey(indexId);

        if (isMetastoreKeyAbsentLocally(metaStorageManager, inProgressBuildIndexMetastoreKey, recoveryRevision)) {
            return nullCompletedFuture();
        }

        return removeMetastoreKeyIfPresent(metaStorageManager, inProgressBuildIndexMetastoreKey);
    }

    private CompletableFuture<?> recoveryForBuildingIndexBusy(
            CatalogIndexDescriptor indexDescriptor,
            long recoveryRevision,
            int catalogVersion
    ) {
        assert indexDescriptor.status() == BUILDING : indexDescriptor.id();

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
        }

        return nullCompletedFuture();
    }
}
