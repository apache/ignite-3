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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.index.IndexManagementUtils.PARTITION_BUILD_INDEX_KEY_PREFIX;
import static org.apache.ignite.internal.index.IndexManagementUtils.extractIndexIdFromPartitionBuildIndexKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.getPartitionCountFromCatalog;
import static org.apache.ignite.internal.index.IndexManagementUtils.inProgressBuildIndexMetastoreKey;
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
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
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
import org.apache.ignite.internal.table.distributed.index.IndexBuildCompletionListener;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * This component is responsible for ensuring that an index, upon completion of a distributed index building for all partitions, becomes
 * available.
 *
 * <p>An approximate algorithm for making an index available:</p>
 * <ul>
 *     <li>On {@link CatalogEvent#INDEX_CREATE},
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
 *     <li>At {@link CatalogEvent#INDEX_DROP},
 *     {@link IndexManagementUtils#putBuildIndexMetastoreKeysIfAbsent(MetaStorageManager, int, int) index building keys} in the metastore
 *     are deleted.</li>
 *     <li>Handling of {@link CatalogEvent#INDEX_CREATE}, {@link CatalogEvent#INDEX_DROP}, {@link CatalogEvent#INDEX_AVAILABLE} and watch
 *     prefix {@link IndexManagementUtils#PARTITION_BUILD_INDEX_KEY_PREFIX} is made by the whole cluster (and only one node makes a write to
 *     the metastore) as these events are global, but only one node (a primary replica owning a partition) handles
 *     {@link IndexBuildCompletionListener#onBuildCompletion} (form {@link IndexBuilder#listen}) event.</li>
 *     <li>Restoring index availability occurs in {@link IndexAvailabilityControllerRestorer}.</li>
 * </ul>
 *
 * @see CatalogIndexDescriptor#available()
 */
// TODO: IGNITE-20638 Need integration with the IgniteImpl
public class IndexAvailabilityController implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(IndexAvailabilityController.class);

    private final CatalogManager catalogManager;

    private final MetaStorageManager metaStorageManager;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Constructor. */
    public IndexAvailabilityController(CatalogManager catalogManager, MetaStorageManager metaStorageManager, IndexBuilder indexBuilder) {
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

    private void addListeners(CatalogService catalogService, MetaStorageManager metaStorageManager, IndexBuilder indexBuilder) {
        catalogService.listen(CatalogEvent.INDEX_CREATE, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexCreate((CreateIndexEventParameters) parameters).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.INDEX_DROP, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexDrop((DropIndexEventParameters) parameters).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.INDEX_AVAILABLE, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexAvailable((MakeIndexAvailableEventParameters) parameters).thenApply(unused -> false);
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

        indexBuilder.listen((indexId, tableId, partitionId) -> onIndexBuildCompletionForPartition(indexId, partitionId));
    }

    private CompletableFuture<?> onIndexCreate(CreateIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int indexId = parameters.indexDescriptor().id();

            int partitions = getPartitionCountFromCatalog(catalogManager, indexId, parameters.catalogVersion());

            return putBuildIndexMetastoreKeysIfAbsent(metaStorageManager, indexId, partitions);
        });
    }

    private CompletableFuture<?> onIndexDrop(DropIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int indexId = parameters.indexId();

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
                return completedFuture(null);
            }

            Entry entry = event.entryEvent().newEntry();

            if (entry.value() != null) {
                // In case an index was created when there was only one partition.
                return completedFuture(null);
            }

            String partitionBuildIndexKey = toPartitionBuildIndexMetastoreKeyString(entry.key());

            int indexId = extractIndexIdFromPartitionBuildIndexKey(partitionBuildIndexKey);

            long metastoreRevision = entry.revision();

            if (isAnyMetastoreKeyPresentLocally(metaStorageManager, partitionBuildIndexMetastoreKeyPrefix(indexId), metastoreRevision)
                    || isMetastoreKeyAbsentLocally(metaStorageManager, inProgressBuildIndexMetastoreKey(indexId), metastoreRevision)) {
                return completedFuture(null);
            }

            // We will not wait for the command to be executed, since we will then find ourselves in a dead lock since we will not be able
            // to free the metastore thread.
            makeIndexAvailableInCatalogWithoutFuture(catalogManager, indexId, LOG);

            return completedFuture(null);
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
}
