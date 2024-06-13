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

package org.apache.ignite.internal.table.distributed.index;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_AVAILABLE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_BUILDING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_REMOVED;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_STOPPING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_ALTER;
import static org.apache.ignite.internal.event.EventListener.fromFunction;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.READ_ONLY;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REMOVED;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.statusOnRemoveIndex;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.StringUtils.incrementLastChar;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.IndexEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.RenameTableEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StoppingIndexEventParameters;
import org.apache.ignite.internal.catalog.events.TableEventParameters;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Local storage of {@link IndexMeta index metadata}, based on a {@link CatalogIndexDescriptor} and stored in vault, is responsible for
 * their life cycle, as well as recovery when the node is restarted. Any change to index metadata is saved in vault. The main difference
 * from the {@link CatalogIndexDescriptor} is that index metadata is independent of catalog compaction and stores all the necessary
 * information for the user when working with indexes.
 *
 * <p>Events that affect changes to index metadata:</p>
 * <ul>
 *     <li>{@link CatalogEvent#INDEX_CREATE} - creates metadata for the new index.</li>
 *     <li>{@link CatalogEvent#INDEX_BUILDING} - changes the {@link IndexMeta#status()} in the metadata to
 *     {@link MetaIndexStatus#BUILDING}.</li>
 *     <li>{@link CatalogEvent#INDEX_AVAILABLE} - changes the {@link IndexMeta#status()} in the metadata to
 *     {@link MetaIndexStatus#AVAILABLE}.</li>
 *     <li>{@link CatalogEvent#INDEX_STOPPING} - changes the {@link IndexMeta#status()} in the metadata to
 *     {@link MetaIndexStatus#STOPPING}.</li>
 *     <li>{@link CatalogEvent#INDEX_REMOVED} - changes the {@link IndexMeta#status()} in the metadata depending on the previous status:<ul>
 *         <li>{@link MetaIndexStatus#STOPPING} or {@link MetaIndexStatus#AVAILABLE} (on table deletion) -
 *         {@link MetaIndexStatus#READ_ONLY}</li>
 *         <li>{@link MetaIndexStatus#REGISTERED} or {@link MetaIndexStatus#BUILDING} - {@link MetaIndexStatus#REMOVED}</li>
 *     </ul></li>
 *     <li>{@link CatalogEvent#TABLE_ALTER} (only table rename) - changes the {@link IndexMeta#indexName() index name} in the metadata.</li>
 *     <li>{@link LowWatermarkEvent#LOW_WATERMARK_CHANGED} - deletes metadata of indexes that were in status
 *     {@link MetaIndexStatus#READ_ONLY} or {@link MetaIndexStatus#REMOVED} and catalog versions in which their status was updated
 *     less than or equal to the active catalog version for the new watermark.</li>
 * </ul>
 */
// TODO: IGNITE-22470 - поменять код/доработать тесты/поправить документацию
public class IndexMetaStorage implements IgniteComponent {
    // TODO: IGNITE-22470 избавиться
    private static final String INDEX_META_KEY_PREFIX = "index.meta.";

    private static final String INDEX_META_VERSION_KEY_PREFIX = "index.meta.version.";

    private static final String INDEX_META_VALUE_KEY_PREFIX = "index.meta.value.";

    private final CatalogService catalogService;

    private final LowWatermark lowWatermark;

    // TODO: IGNITE-22470 избавиться
    private final VaultManager vaultManager;

    private final MetaStorageManager metaStorageManager;

    private final Map<Integer, IndexMeta> indexMetaByIndexId = new ConcurrentHashMap<>();

    /** Constructor. */
    public IndexMetaStorage(
            CatalogService catalogService,
            LowWatermark lowWatermark,
            VaultManager vaultManager,
            MetaStorageManager metaStorageManager
    ) {
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
        this.vaultManager = vaultManager;
        this.metaStorageManager = metaStorageManager;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        catalogService.listen(INDEX_CREATE, fromFunction(this::onCatalogIndexCreateEvent));
        catalogService.listen(INDEX_REMOVED, fromFunction(this::onCatalogIndexRemovedEvent));
        catalogService.listen(INDEX_BUILDING, fromFunction(this::onCatalogIndexBuildingEvent));
        catalogService.listen(INDEX_AVAILABLE, fromFunction(this::onCatalogIndexAvailableEvent));
        catalogService.listen(INDEX_STOPPING, fromFunction(this::onCatalogIndexStoppingEvent));

        catalogService.listen(TABLE_ALTER, fromFunction(this::onCatalogTableAlterEvent));

        lowWatermark.listen(LOW_WATERMARK_CHANGED, fromFunction(this::onLwmChanged));

        recoverIndexMetas();

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        indexMetaByIndexId.clear();

        return nullCompletedFuture();
    }

    /**
     * Returns the index meta if exists.
     *
     * @param indexId Index ID.
     */
    public @Nullable IndexMeta indexMeta(int indexId) {
        return indexMetaByIndexId.get(indexId);
    }

    /** Returns a snapshot of all {@link IndexMeta}s. */
    @TestOnly
    Collection<IndexMeta> indexMetasSnapshot() {
        return List.copyOf(indexMetaByIndexId.values());
    }

    private CompletableFuture<Boolean> onCatalogIndexCreateEvent(CreateIndexEventParameters parameters) {
        CatalogIndexDescriptor catalogIndexDescriptor = parameters.indexDescriptor();

        Catalog catalog = catalog(parameters.catalogVersion());

        return updateAndSaveIndexMetaToMetastore(catalogIndexDescriptor.id(), indexMeta -> {
            assert indexMeta == null : "indexId=" + catalogIndexDescriptor.id() + "catalogVersion=" + catalog.version();

            return IndexMeta.of(catalogIndexDescriptor, catalog);
        }).thenApply(unused -> false);
    }

    private CompletableFuture<Boolean> onCatalogIndexRemovedEvent(RemoveIndexEventParameters parameters) {
        int indexId = parameters.indexId();

        int eventCatalogVersion = parameters.catalogVersion();

        Catalog catalog = catalog(eventCatalogVersion);

        CompletableFuture<?>[] resultFuture = new CompletableFuture<?>[1];

        lowWatermark.getLowWatermarkSafe(lwm -> {
            int lwmCatalogVersion = catalogService.activeCatalogVersion(hybridTimestampToLong(lwm));

            if (eventCatalogVersion <= lwmCatalogVersion) {
                // There is no need to add a read-only index, since the index should be destroyed under the updated low watermark.
                IndexMeta removed = indexMetaByIndexId.remove(indexId);

                assert removed != null : "indexId=" + indexId + ", catalogVersion=" + catalog.version();

                resultFuture[0] = removeFromMetastore(removed);
            } else {
                resultFuture[0] = updateAndSaveIndexMetaToMetastore(indexId, indexMeta -> {
                    assert indexMeta != null : "indexId=" + indexId + ", catalogVersion=" + catalog.version();

                    return setNewStatus(indexMeta, statusOnRemoveIndex(indexMeta.status()), catalog);
                });
            }
        });

        assert resultFuture[0] != null : "indexId=" + indexId + ", catalogVersion=" + catalog.version();

        return resultFuture[0].thenApply(unused -> false);
    }

    private CompletableFuture<Boolean> onCatalogIndexBuildingEvent(StartBuildingIndexEventParameters parameters) {
        return updateIndexStatus(parameters);
    }

    private CompletableFuture<Boolean> onCatalogIndexAvailableEvent(MakeIndexAvailableEventParameters parameters) {
        int catalogVersion = parameters.catalogVersion();
        int indexId = parameters.indexId();

        CatalogIndexDescriptor catalogIndexDescriptor = catalogIndexDescriptor(indexId, catalogVersion);
        CatalogTableDescriptor catalogTableDescriptor = catalogTableDescriptor(catalogIndexDescriptor.tableId(), catalogVersion);

        if (indexId != catalogTableDescriptor.primaryKeyIndexId()) {
            return updateIndexStatus(parameters);
        }

        return falseCompletedFuture();
    }

    private CompletableFuture<Boolean> onCatalogIndexStoppingEvent(StoppingIndexEventParameters parameters) {
        return updateIndexStatus(parameters);
    }

    private CompletableFuture<Boolean> updateIndexStatus(IndexEventParameters parameters) {
        CatalogIndexDescriptor catalogIndexDescriptor = catalogIndexDescriptor(parameters.indexId(), parameters.catalogVersion());

        Catalog catalog = catalog(parameters.catalogVersion());

        return updateAndSaveIndexMetaToMetastore(catalogIndexDescriptor.id(), indexMeta -> {
            assert indexMeta != null : "indexId=" + catalogIndexDescriptor.id() + ", catalogVersion=" + catalog.version();

            return setNewStatus(indexMeta, MetaIndexStatus.convert(catalogIndexDescriptor.status()), catalog);
        }).thenApply(unused -> false);
    }

    private CompletableFuture<Boolean> onCatalogTableAlterEvent(TableEventParameters parameters) {
        if (parameters instanceof RenameTableEventParameters) {
            int catalogVersion = parameters.catalogVersion();

            CatalogTableDescriptor catalogTableDescriptor = catalogTableDescriptor(parameters.tableId(), catalogVersion);

            int indexId = catalogTableDescriptor.primaryKeyIndexId();

            CatalogIndexDescriptor catalogIndexDescriptor = catalogIndexDescriptor(indexId, catalogVersion);

            return updateAndSaveIndexMetaToMetastore(indexId, indexMeta -> {
                assert indexMeta != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;

                return indexMeta.indexName(parameters.catalogVersion(), catalogIndexDescriptor.name());
            }).thenApply(unused -> false);
        }

        return falseCompletedFuture();
    }

    private CompletableFuture<Boolean> onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        int lwmCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

        Iterator<IndexMeta> it = indexMetaByIndexId.values().iterator();

        var futures = new ArrayList<CompletableFuture<?>>();

        while (it.hasNext()) {
            IndexMeta indexMeta = it.next();

            if (shouldBeRemoved(indexMeta, lwmCatalogVersion)) {
                it.remove();

                futures.add(removeFromMetastore(indexMeta));
            }
        }

        return allOf(futures.toArray(CompletableFuture[]::new)).thenApply(unused -> false);
    }

    // TODO: IGNITE-22442 Improve recovery
    private void recoverIndexMetas() {
        Catalog catalog = catalog(catalogService.latestCatalogVersion());

        Map<Integer, IndexMeta> fromVault = readAllFromVault();
        Map<Integer, CatalogIndexDescriptor> fromCatalogLatest = readAllFromCatalog(catalog.version());

        for (CatalogIndexDescriptor catalogIndexDescriptor : fromCatalogLatest.values()) {
            int indexId = catalogIndexDescriptor.id();

            IndexMeta indexMetaFromVault = fromVault.get(indexId);

            if (indexMetaFromVault == null) {
                // We did not have time to save at the index creation event.
                updateAndSaveIndexMetaToVault(indexId, indexMeta -> IndexMeta.of(catalogIndexDescriptor, catalog));
            } else if (!catalogIndexDescriptor.name().equals(indexMetaFromVault.indexName())) {
                // We did not have time to process the index renaming event.
                updateAndSaveIndexMetaToVault(
                        indexId,
                        indexMeta -> indexMetaFromVault.indexName(catalog.version(), catalogIndexDescriptor.name())
                );
            } else if (MetaIndexStatus.convert(catalogIndexDescriptor.status()) != indexMetaFromVault.status()) {
                // We did not have time to process the index status change event.
                updateAndSaveIndexMetaToVault(
                        indexId,
                        indexMeta -> setNewStatus(indexMetaFromVault, MetaIndexStatus.convert(catalogIndexDescriptor.status()), catalog)
                );
            } else {
                indexMetaByIndexId.put(indexId, indexMetaFromVault);
            }
        }

        // Let’s deal with read-only indexes.
        int lwmCatalogVersion = catalogService.activeCatalogVersion(hybridTimestampToLong(lowWatermark.getLowWatermark()));

        for (IndexMeta indexMeta : fromVault.values()) {
            int indexId = indexMeta.indexId();

            if (indexMetaByIndexId.containsKey(indexId)) {
                continue;
            }

            if (indexMeta.status() == READ_ONLY || indexMeta.status() == REMOVED) {
                if (shouldBeRemoved(indexMeta, lwmCatalogVersion)) {
                    // We did not have time to process the lwm change event.
                    removeFromVault(indexMeta);
                } else {
                    indexMetaByIndexId.put(indexId, indexMeta);
                }
            } else {
                // We did not have time to process the index removal event.
                if (catalog.version() <= lwmCatalogVersion) {
                    // There is no need to add a read-only indexes, since the index should be destroyed under the updated low watermark.
                    removeFromVault(indexMeta);
                } else {
                    updateAndSaveIndexMetaToVault(
                            indexId,
                            meta -> setNewStatus(indexMeta, statusOnRemoveIndex(indexMeta.status()), catalog)
                    );
                }
            }
        }
    }

    private CatalogIndexDescriptor catalogIndexDescriptor(int indexId, int catalogVersion) {
        CatalogIndexDescriptor catalogIndexDescriptor = catalogService.index(indexId, catalogVersion);

        assert catalogIndexDescriptor != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;

        return catalogIndexDescriptor;
    }

    private CatalogTableDescriptor catalogTableDescriptor(int tableId, int catalogVersion) {
        CatalogTableDescriptor catalogTableDescriptor = catalogService.table(tableId, catalogVersion);

        assert catalogTableDescriptor != null : "tableId=" + tableId + ", catalogVersion=" + catalogVersion;

        return catalogTableDescriptor;
    }

    private Catalog catalog(int catalogVersion) {
        Catalog catalog = catalogService.catalog(catalogVersion);

        assert catalog != null : catalogVersion;

        return catalog;
    }

    private void updateAndSaveIndexMetaToVault(int indexId, Function<@Nullable IndexMeta, IndexMeta> updateFunction) {
        IndexMeta newMeta = indexMetaByIndexId.compute(indexId, (id, indexMeta) -> updateFunction.apply(indexMeta));

        saveToVault(newMeta);
    }

    private void saveToVault(IndexMeta indexMeta) {
        vaultManager.put(indexMetaVaultKey(indexMeta), toBytes(indexMeta));
    }

    private void removeFromVault(IndexMeta indexMeta) {
        vaultManager.remove(indexMetaVaultKey(indexMeta));
    }

    private Map<Integer, IndexMeta> readAllFromVault() {
        ByteArray fromKey = ByteArray.fromString(INDEX_META_KEY_PREFIX);
        ByteArray toKey = ByteArray.fromString(incrementLastChar(INDEX_META_KEY_PREFIX));

        try (Cursor<VaultEntry> cursor = vaultManager.range(fromKey, toKey)) {
            return cursor.stream()
                    .map(vaultEntry -> (IndexMeta) ByteUtils.fromBytes(vaultEntry.value()))
                    .collect(toMap(IndexMeta::indexId, Function.identity()));
        }
    }

    private Map<Integer, CatalogIndexDescriptor> readAllFromCatalog(int catalogVersion) {
        return catalogService.indexes(catalogVersion).stream()
                .collect(toMap(CatalogObjectDescriptor::id, Function.identity()));
    }

    private static IndexMeta setNewStatus(IndexMeta indexMeta, MetaIndexStatus newStatus, Catalog catalog) {
        return indexMeta.status(newStatus, catalog.version(), catalog.time());
    }

    private static ByteArray indexMetaVaultKey(IndexMeta indexMeta) {
        return ByteArray.fromString(INDEX_META_KEY_PREFIX + indexMeta.indexId());
    }

    private static boolean shouldBeRemoved(IndexMeta indexMeta, int lwmCatalogVersion) {
        MetaIndexStatus status = indexMeta.status();

        return (status == READ_ONLY || status == REMOVED) && indexMeta.statusChanges().get(status).catalogVersion() <= lwmCatalogVersion;
    }

    private static ByteArray indexMetaVersionKey(IndexMeta indexMeta) {
        return ByteArray.fromString(INDEX_META_VERSION_KEY_PREFIX + indexMeta.indexId());
    }

    private static ByteArray indexMetaValueKey(IndexMeta indexMeta) {
        return ByteArray.fromString(INDEX_META_VALUE_KEY_PREFIX + indexMeta.indexId());
    }

    private CompletableFuture<?> saveToMetastore(@Nullable IndexMeta oldMeta, IndexMeta newMeta) {
        assert oldMeta == null || oldMeta.indexId() == newMeta.indexId() :
                "oldMeta.indexId=" + oldMeta.indexId() + ", newMeta.indexId" + newMeta.indexId();
        assert oldMeta == null || oldMeta.catalogVersion() < newMeta.catalogVersion() :
                "oldMeta.catalogVersion=" + oldMeta.catalogVersion() + ", newMeta.catalogVersion=" + newMeta.catalogVersion();

        ByteArray versionKey = indexMetaVersionKey(newMeta);

        return metaStorageManager.invoke(
                oldMeta == null ? notExists(versionKey) : value(versionKey).eq(intToBytes(oldMeta.catalogVersion())),
                List.of(
                        put(versionKey, intToBytes(newMeta.catalogVersion())),
                        put(indexMetaValueKey(newMeta), toBytes(newMeta))
                ),
                List.of(noop())
        );
    }

    private CompletableFuture<?> removeFromMetastore(IndexMeta indexMeta) {
        ByteArray versionKey = indexMetaVersionKey(indexMeta);

        return metaStorageManager.invoke(
                exists(versionKey),
                List.of(remove(versionKey), remove(indexMetaValueKey(indexMeta))),
                List.of(noop())
        );
    }

    private CompletableFuture<?> updateAndSaveIndexMetaToMetastore(
            int indexId,
            Function<@Nullable IndexMeta, IndexMeta> updateFunction
    ) {
        IndexMeta[] oldMeta = new IndexMeta[1];

        IndexMeta newMeta = indexMetaByIndexId.compute(
                indexId,
                (id, indexMeta) -> {
                    oldMeta[0] = indexMeta;

                    return updateFunction.apply(indexMeta);
                }
        );

        return saveToMetastore(oldMeta[0], newMeta);
    }
}
