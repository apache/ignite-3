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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.INDEX_STOPPING;
import static org.apache.ignite.internal.util.CollectionUtils.view;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.events.StoppingIndexEventParameters;
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

            recoveryReadOnlyIndexesBusy();
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
     * Collect indexes for {@link PartitionAccess#addWrite(RowId, BinaryRow, UUID, int, int, int)}.
     *
     * <p>Approximate index selection algorithm:</p>
     * <ul>
     *     <li>If the index in the snapshot catalog version is in status {@link CatalogIndexStatus#BUILDING},
     *     {@link CatalogIndexStatus#AVAILABLE} or {@link CatalogIndexStatus#STOPPING}.</li>
     *     <li>If the index in status {@link CatalogIndexStatus#REGISTERED} and it is in this status on the active version of the catalog
     *     for {@code beginTs}.</li>
     *     <li>For read-only indexes, if their {@link CatalogIndexStatus#STOPPING} status activation time is strictly less than
     *     {@code beginTs}.</li>
     * </ul>
     *
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @param tableId Table ID for which indexes will be chosen.
     * @param beginTs Begin timestamp of the transaction.
     * @return List of index IDs sorted in ascending order.
     */
    public List<Integer> chooseForAddWrite(int catalogVersion, int tableId, HybridTimestamp beginTs) {
        return inBusyLock(busyLock, () -> {
            int activeCatalogVersionAtBeginTxTs = catalogService.activeCatalogVersion(beginTs.longValue());

            List<Integer> fromCatalog = chooseFromCatalogBusy(catalogVersion, tableId, index -> {
                CatalogIndexDescriptor index0 = catalogService.index(index.id(), activeCatalogVersionAtBeginTxTs);

                return index0 != null && index0.status() == REGISTERED;
            });

            List<Integer> fromReadOnlyIndexes = chooseFromReadOnlyIndexesBusy(tableId, beginTs);

            return mergeWithoutDuplicates(fromCatalog, fromReadOnlyIndexes);
        });
    }

    /**
     * Collect indexes for {@link PartitionAccess#addWriteCommitted(RowId, BinaryRow, HybridTimestamp, int)}.
     *
     * <p>Approximate index selection algorithm:</p>
     * <ul>
     *     <li>If the index in the snapshot catalog version is in status {@link CatalogIndexStatus#BUILDING},
     *     {@link CatalogIndexStatus#AVAILABLE} or {@link CatalogIndexStatus#STOPPING}.</li>
     *     <li>For read-only indexes, if their {@link CatalogIndexStatus#STOPPING} status activation time is strictly less than
     *     {@code beginTs}.</li>
     * </ul>
     *
     * @param catalogVersion Catalog version of the incoming partition snapshot.
     * @param tableId Table ID for which indexes will be chosen.
     * @param commitTs Timestamp to associate with committed value.
     * @return List of index IDs sorted in ascending order.
     */
    public List<Integer> chooseForAddWriteCommitted(int catalogVersion, int tableId, HybridTimestamp commitTs) {
        return inBusyLock(busyLock, () -> {
            List<Integer> fromCatalog = chooseFromCatalogBusy(catalogVersion, tableId, index -> false);

            List<Integer> fromReadOnlyIndexes = chooseFromReadOnlyIndexesBusy(tableId, commitTs);

            return mergeWithoutDuplicates(fromCatalog, fromReadOnlyIndexes);
        });
    }

    private List<Integer> chooseFromCatalogBusy(
            int catalogVersion,
            int tableId,
            Predicate<CatalogIndexDescriptor> addRegisteredIndexPredicate
    ) {
        List<CatalogIndexDescriptor> indexes = catalogService.indexes(catalogVersion, tableId);

        assert !indexes.isEmpty() : "tableId=" + tableId + ", catalogVersion=" + catalogVersion;

        // Lazy set.
        List<CatalogIndexDescriptor> result = null;

        for (int i = 0; i < indexes.size(); i++) {
            CatalogIndexDescriptor index = indexes.get(i);

            switch (index.status()) {
                case REGISTERED:
                    if (!addRegisteredIndexPredicate.test(index)) {
                        if (result == null) {
                            List<CatalogIndexDescriptor> subList = indexes.subList(0, i);

                            result = i == indexes.size() - 1 ? subList : new ArrayList<>(subList);
                        }

                        continue;
                    }

                    break;
                case BUILDING:
                case AVAILABLE:
                case STOPPING:
                    break;
                default:
                    throw new IllegalStateException("Unknown index status: " + index.status());
            }

            if (result != null) {
                result.add(index);
            }
        }

        return view(result == null ? indexes : result, CatalogObjectDescriptor::id);
    }

    private List<Integer> chooseFromReadOnlyIndexesBusy(int tableId, HybridTimestamp fromTsExcluded) {
        ReadOnlyIndexInfo fromKeyIncluded = new ReadOnlyIndexInfo(tableId, fromTsExcluded.longValue() + 1, 0);
        ReadOnlyIndexInfo toKeyExcluded = new ReadOnlyIndexInfo(tableId + 1, 0, 0);

        NavigableSet<ReadOnlyIndexInfo> subSet = readOnlyIndexes.subSet(fromKeyIncluded, true, toKeyExcluded, false);

        if (subSet.isEmpty()) {
            return List.of();
        }

        return subSet.stream().map(ReadOnlyIndexInfo::indexId).collect(toList());
    }

    private static List<Integer> mergeWithoutDuplicates(List<Integer> l0, List<Integer> l1) {
        if (l0.isEmpty()) {
            return l1;
        } else if (l1.isEmpty()) {
            return l0;
        }

        List<Integer> result = new ArrayList<>(l0.size() + l1.size());

        for (int i0 = 0, i1 = 0; i0 < l0.size() || i1 < l1.size(); ) {
            if (i0 >= l0.size()) {
                result.add(l1.get(i1++));
            } else if (i1 >= l1.size()) {
                result.add(l0.get(i0++));
            } else {
                Integer indexId0 = l0.get(i0);
                Integer indexId1 = l1.get(i1);

                int cmp = indexId0.compareTo(indexId1);

                if (cmp > 0) {
                    result.add(indexId1);
                    i1++;
                } else if (cmp < 0) {
                    result.add(indexId0);
                    i0++;
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
        catalogService.listen(INDEX_STOPPING, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexStopping((StoppingIndexEventParameters) parameters).thenApply(unused -> false);
        });
    }

    private CompletableFuture<?> onIndexStopping(StoppingIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            ReadOnlyIndexInfo readOnlyIndexInfo = new ReadOnlyIndexInfo(
                    parameters.tableId(),
                    catalogActivationTimestampBusy(parameters.catalogVersion()),
                    parameters.indexId()
            );

            readOnlyIndexes.add(readOnlyIndexInfo);

            return nullCompletedFuture();
        });
    }

    private long catalogActivationTimestampBusy(int catalogVersion) {
        Catalog catalog = catalogService.catalog(catalogVersion);

        assert catalog != null : catalogVersion;

        return catalog.time();
    }

    private void recoveryReadOnlyIndexesBusy() {
        int earliestCatalogVersion = catalogService.earliestCatalogVersion();
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        // At the moment, we will only use tables from the latest version (not dropped), since so far only replicas for them are started
        // on the node.
        int[] tableIds = catalogService.tables(latestCatalogVersion).stream().mapToInt(CatalogObjectDescriptor::id).toArray();

        Map<Integer, ReadOnlyIndexInfo> readOnlyIndexes = new HashMap<>();

        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            assert catalog != null : catalogVersion;

            for (int tableId : tableIds) {
                for (CatalogIndexDescriptor index : catalog.indexes(tableId)) {
                    if (index.status() == STOPPING) {
                        readOnlyIndexes.computeIfAbsent(index.id(), indexId -> new ReadOnlyIndexInfo(tableId, catalog.time(), indexId));
                    }
                }
            }
        }

        this.readOnlyIndexes.addAll(readOnlyIndexes.values());
    }
}
