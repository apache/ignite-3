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

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_REMOVED;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CollectionUtils.view;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Index chooser for full state transfer. */
// TODO: IGNITE-21502 Deal with the case of drop a table
// TODO: IGNITE-21502 Stop writing to a dropped index that was in status before AVAILABLE
// TODO: IGNITE-21514 Stop writing to indexes that are destroyed during catalog compaction
public class FullStateTransferIndexChooser implements ManuallyCloseable {
    private final CatalogService catalogService;

    private final NavigableSet<ReadOnlyIndexInfo> readOnlyIndexes = new ConcurrentSkipListSet<>();

    private final Map<Integer, Integer> tableVersionByIndexId = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Constructor. */
    public FullStateTransferIndexChooser(CatalogService catalogService) {
        this.catalogService = catalogService;
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
     * Collect indexes for {@link PartitionAccess#addWrite(RowId, BinaryRow, UUID, int, int, int)} (write intent).
     *
     * <p>Index selection algorithm:</p>
     * <ul>
     *     <li>If the index in the snapshot catalog version is in status {@link CatalogIndexStatus#BUILDING},
     *     {@link CatalogIndexStatus#AVAILABLE} or {@link CatalogIndexStatus#STOPPING}.</li>
     *     <li>If the index in status {@link CatalogIndexStatus#REGISTERED} and it is in this status on the active version of the catalog
     *     for {@code beginTs}.</li>
     *     <li>For a read-only index, if {@code beginTs} is strictly less than the activation time of dropping the index.</li>
     * </ul>
     *
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @param tableId Table ID for which indexes will be chosen.
     * @param beginTs Begin timestamp of the transaction.
     * @return List of {@link IndexIdAndTableVersion} sorted in ascending order by index ID.
     */
    public List<IndexIdAndTableVersion> chooseForAddWrite(int catalogVersion, int tableId, HybridTimestamp beginTs) {
        return inBusyLock(busyLock, () -> {
            int activeCatalogVersionAtBeginTxTs = catalogService.activeCatalogVersion(beginTs.longValue());

            List<Integer> fromCatalog = chooseFromCatalogBusy(catalogVersion, tableId, index -> {
                if (index.status() == REGISTERED) {
                    CatalogIndexDescriptor indexAtBeginTs = catalogService.index(index.id(), activeCatalogVersionAtBeginTxTs);

                    return indexAtBeginTs != null && indexAtBeginTs.status() == REGISTERED;
                }

                return true;
            });

            List<Integer> fromReadOnlyIndexes = chooseFromReadOnlyIndexesBusy(tableId, beginTs);

            return toIndexIdAndTableVersionList(mergeWithoutDuplicates(fromCatalog, fromReadOnlyIndexes));
        });
    }

    /**
     * Collect indexes for {@link PartitionAccess#addWriteCommitted(RowId, BinaryRow, HybridTimestamp, int)} (write committed only).
     *
     * <p>Index selection algorithm:</p>
     * <ul>
     *     <li>If the index in the snapshot catalog version is in status {@link CatalogIndexStatus#BUILDING},
     *     {@link CatalogIndexStatus#AVAILABLE} or {@link CatalogIndexStatus#STOPPING}.</li>
     *     <li>For a read-only index, if {@code commitTs} is strictly less than the activation time of dropping the index.</li>
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

            return toIndexIdAndTableVersionList(mergeWithoutDuplicates(fromCatalog, fromReadOnlyIndexes));
        });
    }

    private List<Integer> chooseFromCatalogBusy(int catalogVersion, int tableId, Predicate<CatalogIndexDescriptor> filter) {
        List<CatalogIndexDescriptor> indexes = catalogService.indexes(catalogVersion, tableId);

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
        ReadOnlyIndexInfo fromKeyIncluded = new ReadOnlyIndexInfo(tableId, fromTsExcluded.longValue() + 1, 0);
        ReadOnlyIndexInfo toKeyExcluded = new ReadOnlyIndexInfo(tableId + 1, 0, 0);

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
        catalogService.listen(INDEX_CREATE, fromConsumer(this::onIndexCreated));
        catalogService.listen(INDEX_REMOVED, fromConsumer(this::onIndexRemoved));
    }

    private void onIndexRemoved(RemoveIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            int indexId = parameters.indexId();
            int catalogVersion = parameters.catalogVersion();

            CatalogIndexDescriptor index = indexBusy(indexId, catalogVersion - 1);

            if (index.status() == AVAILABLE) {
                // On drop table event.
                readOnlyIndexes.add(new ReadOnlyIndexInfo(index, catalogActivationTimestampBusy(catalogVersion)));
            } else if (index.status() == STOPPING) {
                readOnlyIndexes.add(new ReadOnlyIndexInfo(index, findStoppingActivationTsBusy(indexId, catalogVersion - 1)));
            }
        });
    }

    private void onIndexCreated(CreateIndexEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            CatalogIndexDescriptor index = parameters.indexDescriptor();

            int tableVersion = tableVersionBusy(index, parameters.catalogVersion());

            tableVersionByIndexId.put(index.id(), tableVersion);
        });
    }

    private long catalogActivationTimestampBusy(int catalogVersion) {
        Catalog catalog = catalogService.catalog(catalogVersion);

        assert catalog != null : catalogVersion;

        return catalog.time();
    }

    private void recoverStructuresBusy() {
        int earliestCatalogVersion = catalogService.earliestCatalogVersion();
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        var readOnlyIndexById = new HashMap<Integer, ReadOnlyIndexInfo>();
        var previousCatalogVersionTableIds = Set.<Integer>of();
        var tableVersionById = new HashMap<Integer, Integer>();

        // TODO: IGNITE-21514 Deal with catalog compaction
        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            long activationTs = catalogActivationTimestampBusy(catalogVersion);
            int finalCatalogVersion = catalogVersion;

            for (CatalogIndexDescriptor index : catalogService.indexes(catalogVersion)) {
                tableVersionById.computeIfAbsent(index.id(), i -> tableVersionBusy(index, finalCatalogVersion));

                if (index.status() == STOPPING) {
                    readOnlyIndexById.computeIfAbsent(index.id(), i -> new ReadOnlyIndexInfo(index, activationTs));
                }
            }

            Set<Integer> currentCatalogVersionTableIds = tableIds(catalogVersion);

            // Here we look for indices that transitioned directly from AVAILABLE to [deleted] (corresponding to the logical READ_ONLY
            // state) as such transitions only happen when a table is dropped.
            difference(previousCatalogVersionTableIds, currentCatalogVersionTableIds).stream()
                    .flatMap(droppedTableId -> catalogService.indexes(finalCatalogVersion - 1, droppedTableId).stream())
                    .filter(index -> index.status() == AVAILABLE)
                    .forEach(index -> readOnlyIndexById.computeIfAbsent(index.id(), i -> new ReadOnlyIndexInfo(index, activationTs)));

            previousCatalogVersionTableIds = currentCatalogVersionTableIds;
        }

        readOnlyIndexes.addAll(readOnlyIndexById.values());
        tableVersionByIndexId.putAll(tableVersionById);
    }

    private CatalogIndexDescriptor indexBusy(int indexId, int catalogVersion) {
        CatalogIndexDescriptor index = catalogService.index(indexId, catalogVersion);

        assert index != null : "indexId=" + indexId + ", catalogVersion=" + catalogVersion;

        return index;
    }

    private long findStoppingActivationTsBusy(int indexId, int toCatalogVersionIncluded) {
        int earliestCatalogVersion = catalogService.earliestCatalogVersion();

        for (int catalogVersion = toCatalogVersionIncluded; catalogVersion >= earliestCatalogVersion; catalogVersion--) {
            if (indexBusy(indexId, catalogVersion).status() == AVAILABLE) {
                return catalogActivationTimestampBusy(catalogVersion + 1);
            }
        }

        throw new AssertionError(format(
                "{} status activation timestamp was not found for index: [indexId={}, toCatalogVersionIncluded={}]",
                STOPPING, indexId, toCatalogVersionIncluded
        ));
    }

    private Set<Integer> tableIds(int catalogVersion) {
        return catalogService.tables(catalogVersion).stream().map(CatalogObjectDescriptor::id).collect(toSet());
    }

    private int tableVersionBusy(CatalogIndexDescriptor index, int catalogVersion) {
        CatalogTableDescriptor table = catalogService.table(index.tableId(), catalogVersion);

        assert table != null : "indexId=" + index.id() + ", tableId=" + index.tableId() + ", catalogVersion=" + catalogVersion;

        return table.tableVersion();
    }

    private List<IndexIdAndTableVersion> toIndexIdAndTableVersionList(List<Integer> indexIds) {
        return indexIds.stream()
                .map(indexId -> {
                    Integer tableVersion = tableVersionByIndexId.get(indexId);

                    return tableVersion == null ? null : new IndexIdAndTableVersion(indexId, tableVersion);
                })
                .filter(Objects::nonNull)
                .collect(toCollection(() -> new ArrayList<>(indexIds.size())));
    }
}
