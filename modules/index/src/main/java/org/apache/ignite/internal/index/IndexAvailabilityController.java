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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** No doc. */
// TODO: IGNITE-19276 Документация, тесты и реализация
public class IndexAvailabilityController implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(IndexAvailabilityController.class);

    private static final String START_BUILD_INDEX_KEY_PREFIX = "startBuildIndex";

    private static final String PARTITION_BUILD_INDEX_KEY_PREFIX = "partitionBuildIndex";

    private final CatalogService catalogService;

    private final MetaStorageManager metaStorageManager;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Constructor. */
    public IndexAvailabilityController(CatalogService catalogService, MetaStorageManager metaStorageManager) {
        this.catalogService = catalogService;
        this.metaStorageManager = metaStorageManager;
    }

    @Override
    public void start() {
        addListeners();
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    private void addListeners() {
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
    }

    private CompletableFuture<?> onIndexCreate(CreateIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int indexId = parameters.indexDescriptor().id();

            int partitions = getPartitionCountFromCatalog(indexId, parameters.catalogVersion());

            ByteArray startBuildIndexKey = startBuildIndexKey(indexId);

            return metaStorageManager.invoke(
                    notExists(startBuildIndexKey),
                    concat(
                            List.of(put(startBuildIndexKey, BYTE_EMPTY_ARRAY)),
                            putPartitionBuildIndexOperations(indexId, partitions)
                    ),
                    List.of(noop())
            );
        });
    }

    private CompletableFuture<?> onIndexDrop(DropIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            // TODO: IGNITE-19276 реализовать
            throw new UnsupportedOperationException();
        });
    }

    private CompletableFuture<?> onUpdatePartitionBuildIndexKey(WatchEvent event) {
        return inBusyLockAsync(busyLock, () -> {
            if (!event.single()) {
                // We don't need to handle keys on index creation or deletion.
                return completedFuture(null);
            }

            Entry entry = event.entryEvent().newEntry();

            String partitionBuildIndexKey = new String(entry.key(), UTF_8);

            // We expect that when the index building for a specific partition is completed, the key will be deleted.
            assert entry.tombstone() : partitionBuildIndexKey;

            int indexId = parseIndexIdFromPartitionBuildIndexKey(partitionBuildIndexKey);

            if (isRemainingPartitionBuildIndexKeys(indexId, entry.revision())) {
                return completedFuture(null);
            }

            ByteArray startBuildIndexKey = startBuildIndexKey(indexId);

            // TODO: IGNITE-19276 перед этим нужно будет обновить каталог
            return metaStorageManager.invoke(exists(startBuildIndexKey), remove(startBuildIndexKey), noop());
        });
    }

    private int getPartitionCountFromCatalog(int indexId, int catalogVersion) {
        CatalogIndexDescriptor indexDescriptor = catalogService.index(indexId, catalogVersion);

        assert indexDescriptor != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;

        CatalogTableDescriptor tableDescriptor = catalogService.table(indexDescriptor.tableId(), catalogVersion);

        assert tableDescriptor != null : "tableId=" + indexDescriptor.tableId() + ", catalogVersion=" + catalogVersion;

        CatalogZoneDescriptor zoneDescriptor = catalogService.zone(tableDescriptor.zoneId(), catalogVersion);

        assert zoneDescriptor != null : "zoneId=" + tableDescriptor.zoneId() + ", catalogVersion=" + catalogVersion;

        return zoneDescriptor.partitions();
    }

    private boolean isRemainingPartitionBuildIndexKeys(int indexId, long metastoreRevision) {
        try (Cursor<Entry> cursor = metaStorageManager.prefixLocally(partitionBuildIndexKeyPrefix(indexId), metastoreRevision)) {
            return cursor.stream().anyMatch(not(Entry::tombstone));
        }
    }

    private static ByteArray startBuildIndexKey(int indexId) {
        return ByteArray.fromString(START_BUILD_INDEX_KEY_PREFIX + '.' + indexId);
    }

    private static ByteArray partitionBuildIndexKeyPrefix(int indexId) {
        return ByteArray.fromString(PARTITION_BUILD_INDEX_KEY_PREFIX + '.' + indexId);
    }

    private static ByteArray partitionBuildIndexKey(int indexId, int partitionId) {
        return ByteArray.fromString(PARTITION_BUILD_INDEX_KEY_PREFIX + '.' + indexId + '.' + partitionId);
    }

    private static Collection<Operation> putPartitionBuildIndexOperations(int indexId, int partitions) {
        return IntStream.range(0, partitions)
                .mapToObj(partitionId -> partitionBuildIndexKey(indexId, partitionId))
                .map(key -> put(key, BYTE_EMPTY_ARRAY))
                .collect(toList());
    }

    private static int parseIndexIdFromPartitionBuildIndexKey(String key) {
        assert key.startsWith(PARTITION_BUILD_INDEX_KEY_PREFIX) : key;

        int indexIdFromIndex = PARTITION_BUILD_INDEX_KEY_PREFIX.length() + 1;

        int indexIdToIndex = key.indexOf('.', indexIdFromIndex);

        return Integer.parseInt(key.substring(indexIdFromIndex, indexIdToIndex));
    }
}
