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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_REMOVED;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.util.CollectionUtils.view;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatus;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Index chooser for full state transfer. */
// TODO: IGNITE-21502 Deal with the case of drop a table
// TODO: IGNITE-21502 Stop writing to a dropped index that was in status before AVAILABLE
public class FullStateTransferIndexChooser implements ManuallyCloseable {
    private final CatalogService catalogService;

    private final LowWatermark lowWatermark;

    private final NavigableSet<ReadOnlyIndexInfo> readOnlyIndexes = new ConcurrentSkipListSet<>(
            comparingInt(ReadOnlyIndexInfo::tableId)
                    .thenComparingLong(ReadOnlyIndexInfo::activationTs)
                    .thenComparingInt(ReadOnlyIndexInfo::indexId)
    );

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    private final IndexMetaStorage indexMetaStorage;

    /** Constructor. */
    public FullStateTransferIndexChooser(CatalogService catalogService, LowWatermark lowWatermark, IndexMetaStorage indexMetaStorage) {
        this.catalogService = catalogService;
        this.lowWatermark = lowWatermark;
        this.indexMetaStorage = indexMetaStorage;
    }

    /** Starts the component. */
    public void start() {
        inBusyLockSafe(busyLock, () -> {
            addListenersBusy();

            recoverStructuresBusy();
        });
    }

    @Override
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        readOnlyIndexes.clear();
    }

    /**
     * Collect indexes for {@link PartitionMvStorageAccess#addWrite} (write intent).
     *
     * <p>NOTE: When updating a low watermark, the index storages that were returned from the method may begin to be destroyed, such a
     * situation should be handled by the calling code.</p>
     *
     * <p>Index selection algorithm:</p>
     * <ul>
     *     <li>If the index in the snapshot catalog version is in status {@link CatalogIndexStatus#BUILDING},
     *     {@link CatalogIndexStatus#AVAILABLE} or {@link CatalogIndexStatus#STOPPING} and not removed in the latest catalog version.</li>
     *     <li>If the index in status {@link CatalogIndexStatus#REGISTERED} and it is in this status on the active version of the catalog
     *     for {@code beginTs} and not removed in the latest catalog version.</li>
     *     <li>For a read-only index, if {@code beginTs} is strictly less than the activation time of dropping the index and not removed due
     *     to low watermark.</li>
     * </ul>
     *
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @param tableId Table ID for which indexes will be chosen.
     * @param beginTs Begin timestamp of the transaction.
     * @return List of {@link IndexIdAndTableVersion} sorted in ascending order by index ID.
     */
    public List<IndexIdAndTableVersion> chooseForAddWrite(int catalogVersion, int tableId, HybridTimestamp beginTs) {
        return inBusyLock(busyLock, () -> {
            List<Integer> fromCatalog = chooseFromCatalogBusy(catalogVersion, tableId, index -> {
                if (index.status() == REGISTERED) {
                    IndexMeta indexMeta = indexMetaStorage.indexMeta(index.id());
                    if (indexMeta == null) {
                        // No index meta, allow the index to be used.
                        return true;
                    }

                    MetaIndexStatus statusAtTxBegin = indexMeta.statusAt(beginTs.longValue());
                    return statusAtTxBegin == MetaIndexStatus.REGISTERED;
                }

                return true;
            });

            List<Integer> fromReadOnlyIndexes = chooseFromReadOnlyIndexesBusy(tableId, beginTs);

            return enrichWithTableVersions(mergeWithoutDuplicates(fromCatalog, fromReadOnlyIndexes));
        });
    }

    /**
     * Collect indexes for {@link PartitionMvStorageAccess#addWriteCommitted} (write committed only).
     *
     * <p>NOTE: When updating a low watermark, the index storages that were returned from the method may begin to be destroyed, such a
     * situation should be handled by the calling code.</p>
     *
     * <p>Index selection algorithm:</p>
     * <ul>
     *     <li>If the index in the snapshot catalog version is in status {@link CatalogIndexStatus#BUILDING},
     *     {@link CatalogIndexStatus#AVAILABLE} or {@link CatalogIndexStatus#STOPPING} and not removed in the latest catalog version.</li>
     *     <li>For a read-only index, if {@code commitTs} is strictly less than the activation time of dropping the index and not removed
     *     due to low watermark.</li>
     * </ul>
     *
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @param tableId Table ID for which indexes will be chosen.
     * @param commitTs Timestamp to associate with committed value.
     * @return List of {@link IndexIdAndTableVersion} sorted in ascending order by index ID.
     */
    public List<IndexIdAndTableVersion> chooseForAddWriteCommitted(int catalogVersion, int tableId, HybridTimestamp commitTs) {
        return inBusyLock(busyLock, () -> {
            List<Integer> fromCatalog = chooseFromCatalogBusy(catalogVersion, tableId, index -> index.status() != REGISTERED);

            List<Integer> fromReadOnlyIndexes = chooseFromReadOnlyIndexesBusy(tableId, commitTs);

            return enrichWithTableVersions(mergeWithoutDuplicates(fromCatalog, fromReadOnlyIndexes));
        });
    }

    private List<Integer> chooseFromCatalogBusy(int catalogVersion, int tableId, Predicate<CatalogIndexDescriptor> filter) {
        List<CatalogIndexDescriptor> indexes = catalogService.catalog(catalogVersion).indexes(tableId);

        if (indexes.isEmpty()) {
            return List.of();
        }

        var result = new ArrayList<CatalogIndexDescriptor>(indexes.size());

        for (CatalogIndexDescriptor index : indexes) {
            switch (index.status()) {
                case REGISTERED:
                case BUILDING:
                case AVAILABLE:
                case STOPPING:
                    if (filter.test(index)) {
                        result.add(index);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown index status: " + index.status());
            }
        }

        return view(result, CatalogObjectDescriptor::id);
    }

    private List<Integer> chooseFromReadOnlyIndexesBusy(int tableId, HybridTimestamp fromTsExcluded) {
        ReadOnlyIndexInfo fromKeyIncluded = new ReadOnlyIndexInfo(tableId, fromTsExcluded.longValue() + 1, 0, 0);
        ReadOnlyIndexInfo toKeyExcluded = new ReadOnlyIndexInfo(tableId + 1, 0, 0, 0);

        NavigableSet<ReadOnlyIndexInfo> subSet = readOnlyIndexes.subSet(fromKeyIncluded, true, toKeyExcluded, false);

        if (subSet.isEmpty()) {
            return List.of();
        }

        return subSet.stream().map(ReadOnlyIndexInfo::indexId).sorted().collect(toList());
    }

    private static List<Integer> mergeWithoutDuplicates(List<Integer> l0, List<Integer> l1) {
        if (l0.isEmpty()) {
            return l1;
        } else if (l1.isEmpty()) {
            return l0;
        }

        var result = new ArrayList<Integer>(l0.size() + l1.size());

        for (int i0 = 0, i1 = 0; i0 < l0.size() || i1 < l1.size(); ) {
            if (i0 >= l0.size()) {
                result.add(l1.get(i1++));
            } else if (i1 >= l1.size()) {
                result.add(l0.get(i0++));
            } else {
                Integer indexId0 = l0.get(i0);
                Integer indexId1 = l1.get(i1);

                if (indexId0 < indexId1) {
                    result.add(indexId0);
                    i0++;
                } else if (indexId0 > indexId1) {
                    result.add(indexId1);
                    i1++;
                } else {
                    result.add(indexId0);
                    i0++;
                    i1++;
                }
            }
        }

        return result;
    }

    private void addListenersBusy() {
        catalogService.listen(INDEX_REMOVED, fromConsumer(this::onIndexRemoved));

        lowWatermark.listen(LOW_WATERMARK_CHANGED, fromConsumer(this::onLwmChanged));
    }

    private void onIndexRemoved(RemoveIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            int indexId = parameters.indexId();
            int catalogVersion = parameters.catalogVersion();

            lowWatermark.getLowWatermarkSafe(lwm -> {
                int lwmCatalogVersion = lwm == null
                        ? catalogService.earliestCatalogVersion()
                        : catalogService.activeCatalogVersion(lwm.longValue());

                if (catalogVersion <= lwmCatalogVersion) {
                    // There is no need to add a read-only indexes, since the index should be destroyed under the updated low watermark.
                    return;
                }

                IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

                assert indexMeta != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;

                if (indexMeta.status() == MetaIndexStatus.READ_ONLY) {
                    readOnlyIndexes.add(toReadOnlyIndexInfo(indexMeta));
                }
            });
        });
    }

    private void recoverStructuresBusy() {
        indexMetaStorage.indexMetas().stream()
                .filter(indexMeta -> indexMeta.status() == MetaIndexStatus.READ_ONLY)
                .map(FullStateTransferIndexChooser::toReadOnlyIndexInfo)
                .forEach(readOnlyIndexes::add);
    }

    private List<IndexIdAndTableVersion> enrichWithTableVersions(List<Integer> indexIds) {
        return indexIds.stream()
                .map(indexId -> {
                    IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

                    if (indexMeta == null || indexMeta.status() == MetaIndexStatus.REMOVED) {
                        return null;
                    }

                    return new IndexIdAndTableVersion(indexId, indexMeta.tableVersion());
                })
                .filter(Objects::nonNull)
                .collect(toCollection(() -> new ArrayList<>(indexIds.size())));
    }

    private void onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        inBusyLockSafe(busyLock, () -> {
            int lwmCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

            readOnlyIndexes.removeIf(readOnlyIndexInfo -> readOnlyIndexInfo.indexRemovalCatalogVersion() <= lwmCatalogVersion);
        });
    }

    private static long activationTs(IndexMeta indexMeta, MetaIndexStatus status) {
        return indexMeta.statusChange(status).activationTimestamp();
    }

    private static ReadOnlyIndexInfo toReadOnlyIndexInfo(IndexMeta indexMeta) {
        assert indexMeta.status() == MetaIndexStatus.READ_ONLY : "indexId=" + indexMeta.indexId() + ", status=" + indexMeta.status();

        long activationTs;

        if (indexMeta.statusChanges().containsKey(MetaIndexStatus.STOPPING)) {
            activationTs = activationTs(indexMeta, MetaIndexStatus.STOPPING);
        } else {
            activationTs = activationTs(indexMeta, MetaIndexStatus.READ_ONLY);
        }

        return new ReadOnlyIndexInfo(
                indexMeta.tableId(),
                activationTs,
                indexMeta.indexId(),
                indexMeta.statusChange(MetaIndexStatus.READ_ONLY).catalogVersion()
        );
    }
}
