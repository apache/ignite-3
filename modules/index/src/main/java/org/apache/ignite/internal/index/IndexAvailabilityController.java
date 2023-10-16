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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.IndexAlreadyAvailableValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
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
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * This component is responsible for ensuring that an index, upon completion of a distributed index building for all partitions, becomes
 * available for read-write.
 *
 * <p>An approximate algorithm for making an index available for read-write:</p>
 * <ul>
 *     <li>On {@link CatalogEvent#INDEX_CREATE}, keys are created in the metastore: {@code indexBuild.inProgress.<indexId>} and
 *     {@code indexBuild.partition.<indexId>.<partitionId_0>}...{@code indexBuild.partition.<indexId>.<partitionId_N>}.</li>
 *     <li>Then it is expected that the distributed index building event will be triggered for all partitions via
 *     {@link IndexBuildCompletionListener} (from {@link IndexBuilder#listen}); as a result of each of these events, the corresponding key
 *     {@code indexBuild.partition.<indexId>.<partitionId>} will be deleted from metastore.</li>
 *     <li>When all the {@code indexBuild.partition.<indexId>.<partitionId>} keys in the metastore are deleted,
 *     {@link MakeIndexAvailableCommand} will be executed for the corresponding index.</li>
 *     <li>At {@link CatalogEvent#INDEX_AVAILABLE}, key {@code indexBuild.inProgress.<indexId>} in the metastore will be deleted.</li>
 * </ul>
 *
 * <p>Notes:</p>
 * <ul>
 *     <li>At {@link CatalogEvent#INDEX_DROP}, the keys in the metastore are deleted: {@code indexBuild.inProgress.<indexId>} and
 *     {@code indexBuild.partition.<indexId>.<partitionId_0>}...{@code indexBuild.partition.<indexId>.<partitionId_N>}.</li>
 *     <li>Handling of {@link CatalogEvent#INDEX_CREATE}, {@link CatalogEvent#INDEX_DROP}, {@link CatalogEvent#INDEX_AVAILABLE} and watch
 *     prefix {@link #PARTITION_BUILD_INDEX_KEY_PREFIX} is made by the whole cluster (and only one node makes a write to the metastore) as
 *     these events are global, but only one node (a primary replica owning a partition) handles
 *     {@link IndexBuildCompletionListener#onBuildCompletion} (form {@link IndexBuilder#listen}) event.</li>
 * </ul>
 *
 * @see CatalogIndexDescriptor#writeOnly()
 */
// TODO: IGNITE-20637 Recovery needs to be implemented
// TODO: IGNITE-20637 Need integration with the IgniteImpl
public class IndexAvailabilityController implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(IndexAvailabilityController.class);

    private static final String IN_PROGRESS_BUILD_INDEX_KEY_PREFIX = "indexBuild.inProgress.";

    private static final String PARTITION_BUILD_INDEX_KEY_PREFIX = "indexBuild.partition.";

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

            int partitions = getPartitionCountFromCatalog(indexId, parameters.catalogVersion());

            ByteArray inProgressBuildIndexKey = inProgressBuildIndexKey(indexId);

            return metaStorageManager.invoke(
                    notExists(inProgressBuildIndexKey),
                    concat(
                            List.of(put(inProgressBuildIndexKey, BYTE_EMPTY_ARRAY)),
                            putPartitionBuildIndexOperations(indexId, partitions)
                    ),
                    List.of(noop())
            );
        });
    }

    private CompletableFuture<?> onIndexDrop(DropIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int indexId = parameters.indexId();

            int partitions = getPartitionCountFromCatalog(indexId, parameters.catalogVersion() - 1);

            ByteArray inProgressBuildIndexKey = inProgressBuildIndexKey(indexId);

            return metaStorageManager.invoke(
                    exists(inProgressBuildIndexKey),
                    concat(
                            List.of(remove(inProgressBuildIndexKey)),
                            removePartitionBuildIndexOperations(indexId, partitions)
                    ),
                    List.of(noop())
            );
        });
    }

    private CompletableFuture<?> onIndexAvailable(MakeIndexAvailableEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            ByteArray inProgressBuildIndexKey = inProgressBuildIndexKey(parameters.indexId());

            return metaStorageManager.invoke(exists(inProgressBuildIndexKey), remove(inProgressBuildIndexKey), noop());
        });
    }

    private CompletableFuture<?> onUpdatePartitionBuildIndexKey(WatchEvent event) {
        return inBusyLockAsync(busyLock, () -> {
            if (!event.single()) {
                // We don't need to handle keys on index creation or deletion.
                return completedFuture(null);
            }

            Entry entry = event.entryEvent().newEntry();

            if (!entry.tombstone()) {
                // In case an index was created when there was only one partition.
                return completedFuture(null);
            }

            String partitionBuildIndexKey = new String(entry.key(), UTF_8);

            int indexId = parseIndexIdFromPartitionBuildIndexKey(partitionBuildIndexKey);

            ByteArray inProgressBuildIndexKey = inProgressBuildIndexKey(indexId);

            long metastoreRevision = entry.revision();

            if (isRemainingPartitionBuildIndexKeys(indexId, metastoreRevision)
                    || isMetastoreKeyAbsent(inProgressBuildIndexKey, metastoreRevision)) {
                return completedFuture(null);
            }

            // We will not wait for the command to be executed, since we will then find ourselves in a dead lock since we will not be able
            // to free the metastore thread.
            catalogManager
                    .execute(buildMakeIndexAvailableCommand(indexId))
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            Throwable unwrapCause = unwrapCause(throwable);

                            if (!(unwrapCause instanceof IndexNotFoundValidationException)
                                    && !(unwrapCause instanceof IndexAlreadyAvailableValidationException)
                                    && !(unwrapCause instanceof NodeStoppingException)) {
                                LOG.error("Error processing the command to make the index available: {}", unwrapCause, indexId);
                            }
                        }
                    });

            return completedFuture(null);
        });
    }

    private void onIndexBuildCompletionForPartition(int indexId, int partitionId) {
        inBusyLock(busyLock, () -> {
            ByteArray partitionBuildIndexKey = partitionBuildIndexKey(indexId, partitionId);

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

    private int getPartitionCountFromCatalog(int indexId, int catalogVersion) {
        CatalogIndexDescriptor indexDescriptor = getIndexDescriptorStrict(indexId, catalogVersion);

        CatalogTableDescriptor tableDescriptor = catalogManager.table(indexDescriptor.tableId(), catalogVersion);

        assert tableDescriptor != null : "tableId=" + indexDescriptor.tableId() + ", catalogVersion=" + catalogVersion;

        CatalogZoneDescriptor zoneDescriptor = catalogManager.zone(tableDescriptor.zoneId(), catalogVersion);

        assert zoneDescriptor != null : "zoneId=" + tableDescriptor.zoneId() + ", catalogVersion=" + catalogVersion;

        return zoneDescriptor.partitions();
    }

    private CatalogIndexDescriptor getIndexDescriptorStrict(int indexId, int catalogVersion) {
        CatalogIndexDescriptor indexDescriptor = catalogManager.index(indexId, catalogVersion);

        assert indexDescriptor != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;

        return indexDescriptor;
    }

    private boolean isRemainingPartitionBuildIndexKeys(int indexId, long metastoreRevision) {
        try (Cursor<Entry> cursor = metaStorageManager.prefixLocally(partitionBuildIndexKeyPrefix(indexId), metastoreRevision)) {
            return cursor.stream().anyMatch(not(Entry::tombstone));
        }
    }

    private boolean isMetastoreKeyAbsent(ByteArray key, long metastoreRevision) {
        return metaStorageManager.getLocally(key, metastoreRevision).value() == null;
    }

    private static ByteArray inProgressBuildIndexKey(int indexId) {
        return ByteArray.fromString(IN_PROGRESS_BUILD_INDEX_KEY_PREFIX + indexId);
    }

    private static ByteArray partitionBuildIndexKeyPrefix(int indexId) {
        return ByteArray.fromString(PARTITION_BUILD_INDEX_KEY_PREFIX + indexId);
    }

    private static ByteArray partitionBuildIndexKey(int indexId, int partitionId) {
        return ByteArray.fromString(PARTITION_BUILD_INDEX_KEY_PREFIX + indexId + '.' + partitionId);
    }

    private static Collection<Operation> putPartitionBuildIndexOperations(int indexId, int partitions) {
        return IntStream.range(0, partitions)
                .mapToObj(partitionId -> partitionBuildIndexKey(indexId, partitionId))
                .map(key -> put(key, BYTE_EMPTY_ARRAY))
                .collect(toList());
    }

    private static Collection<Operation> removePartitionBuildIndexOperations(int indexId, int partitions) {
        return IntStream.range(0, partitions)
                .mapToObj(partitionId -> partitionBuildIndexKey(indexId, partitionId))
                .map(Operations::remove)
                .collect(toList());
    }

    private static int parseIndexIdFromPartitionBuildIndexKey(String key) {
        assert key.startsWith(PARTITION_BUILD_INDEX_KEY_PREFIX) : key;

        int indexIdFromIndex = PARTITION_BUILD_INDEX_KEY_PREFIX.length();

        int indexIdToIndex = key.indexOf('.', indexIdFromIndex);

        return Integer.parseInt(key.substring(indexIdFromIndex, indexIdToIndex));
    }

    private static CatalogCommand buildMakeIndexAvailableCommand(int indexId) {
        return MakeIndexAvailableCommand.builder().indexId(indexId).build();
    }
}
