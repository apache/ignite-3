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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.util.CollectionUtils.view;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Index chooser for full state transfer. */
public class FullStateTransferIndexChooser implements ManuallyCloseable {
    private final CatalogService catalogService;

    private final NavigableSet<ReadOnlyIndexInfo> readOnlyIndexes = new ConcurrentSkipListSet<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Constructor. */
    public FullStateTransferIndexChooser(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    /** Recovers internal structures on node recovery. */
    public void start() {
        inBusyLockSafe(busyLock, () -> {
        });
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    /**
     * Скоро.
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
     * Скоро.
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

        for (int i0 = 0, i1 = 0; i0 < l0.size() && i1 < l1.size(); ) {
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
}
