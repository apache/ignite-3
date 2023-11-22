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

import static java.util.Collections.binarySearch;
import static java.util.Collections.unmodifiableList;
import static java.util.Comparator.comparingInt;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Index collector for various operations, for example for RW transactions. */
class IndexCollector implements ManuallyCloseable {
    private static final Comparator<CatalogIndexDescriptor> INDEX_COMPARATOR = comparingInt(CatalogObjectDescriptor::id);

    private final CatalogService catalogService;

    /**
     * Map that, for each key, contains a list of all dropped available table indexes (sorted by {@link #INDEX_COMPARATOR}) for all known
     * catalog versions.
     *
     * <p>Let's look at an example, let's say we have the following versions of a catalog with indexes:</p>
     * <pre>
     *     0: I0(A) I1(A)
     *     1: IO(A)
     *     2: I0(A) I2(R) I3(A)
     *     3: I0(A)
     *     4: I0(A)
     * </pre>
     *
     * <p>Then the map will have the following values:</p>
     * <pre>
     *     (0, 1) -> [I1(A)]
     *     (0, 3) -> [I1(A), I3(A)]
     * </pre>
     *
     * <p>Then, when {@link #getDroppedAvailableIndexes(int, int) getting dropped available indexes}, we will return the following:</p>
     * <pre>
     *     (0, 0) -> []
     *     (1, 0) -> [I1(A)]
     *     (2, 0) -> [I1(A)]
     *     (3, 0) -> [I1(A), I3(A)]
     *     (4, 0) -> [I1(A), I3(A)]
     * </pre>
     *
     * <p>Updated on {@link #recover() node recovery} and a catalog events processing.</p>
     */
    private final ConcurrentSkipListMap<TableIdCatalogVersion, List<CatalogIndexDescriptor>> droppedAvailableTableIndexes
            = new ConcurrentSkipListMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Constructor. */
    IndexCollector(CatalogService catalogService) {
        this.catalogService = catalogService;

        addListeners();
    }

    /** Recovers internal structures on node recovery. */
    void recover() {
        inBusyLock(busyLock, () -> {
            // It is expected that the methods will be called only on recovery, when the deploy of metastore watches has not yet occurred.
            int earliestCatalogVersion = catalogService.earliestCatalogVersion();
            int latestCatalogVersion = catalogService.latestCatalogVersion();

            // At the moment, we will only use tables from the latest version, since so far only replicas for them are started on the node.
            for (CatalogTableDescriptor table : catalogService.tables(latestCatalogVersion)) {
                for (int catalogVersion = earliestCatalogVersion; catalogVersion < latestCatalogVersion; catalogVersion++) {
                    int tableId = table.id();
                    int nextCatalogVersion = catalogVersion + 1;

                    List<CatalogIndexDescriptor> tableIndexes = catalogService.indexes(catalogVersion, tableId);

                    if (tableIndexes.isEmpty()) {
                        // Table does not exist yet.
                        continue;
                    }

                    List<CatalogIndexDescriptor> nextCatalogVersionTableIndexes = catalogService.indexes(nextCatalogVersion, tableId);

                    assert !nextCatalogVersionTableIndexes.isEmpty()
                            : String.format("Table should not be dropped: [catalogVersion=%s, tableId=%s]", nextCatalogVersion, tableId);

                    for (CatalogIndexDescriptor tableIndex : tableIndexes) {
                        if (tableIndex.available() && !contains(nextCatalogVersionTableIndexes, tableIndex)) {
                            addDroppedAvailableIndex(tableIndex, nextCatalogVersion);
                        }
                    }
                }
            }
        });
    }

    @Override
    public void close() {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        droppedAvailableTableIndexes.clear();
    }

    /**
     * Collects a list of table indexes that will need to be used for an update operation in RW a transaction. List consists of all indexes
     * (available and registered) on the requested catalog version, as well as all dropped available indexes from previous catalog versions.
     *
     * <p>Returned list is sorted by {@link CatalogObjectDescriptor#id()}. It is expected that the table exists at the time the method is
     * called.</p>
     *
     * @param catalogVersion Catalog version.
     * @param tableId Table ID.
     */
    List<CatalogIndexDescriptor> collectForRwTxOperation(int catalogVersion, int tableId) {
        return inBusyLock(busyLock, () -> {
            List<CatalogIndexDescriptor> tableIndexes = catalogService.indexes(catalogVersion, tableId);

            assert !tableIndexes.isEmpty() : "catalogVersion=" + catalogVersion + ", tableId=" + tableId;

            List<CatalogIndexDescriptor> droppedAvailableTableIndexes = getDroppedAvailableIndexes(catalogVersion, tableId);

            if (droppedAvailableTableIndexes.isEmpty()) {
                return tableIndexes;
            }

            return unmodifiableList(merge(tableIndexes, droppedAvailableTableIndexes));
        });
    }

    private void addListeners() {
        catalogService.listen(CatalogEvent.INDEX_DROP, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onDropIndex((DropIndexEventParameters) parameters).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.TABLE_DROP, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onTableDrop((DropTableEventParameters) parameters).thenApply(unused -> false);
        });
    }

    private CompletableFuture<?> onDropIndex(DropIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int previousCatalogVersion = parameters.catalogVersion() - 1;

            CatalogIndexDescriptor droppedIndexDescriptor = catalogService.index(parameters.indexId(), previousCatalogVersion);

            assert droppedIndexDescriptor != null : "indexId=" + parameters.indexId() + ", catalogVersion=" + previousCatalogVersion;

            if (!droppedIndexDescriptor.available()) {
                return completedFuture(null);
            }

            addDroppedAvailableIndex(droppedIndexDescriptor, parameters.catalogVersion());

            return completedFuture(null);
        });
    }

    private CompletableFuture<?> onTableDrop(DropTableEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            droppedAvailableTableIndexes.entrySet().removeIf(entry -> parameters.tableId() == entry.getKey().tableId);

            return completedFuture(null);
        });
    }

    /**
     * Returns a list of dropped available indexes (sorted by {@link #INDEX_COMPARATOR}) for the catalog version of interest from
     * {@link #droppedAvailableTableIndexes}. If there is no list for the requested catalog version, the closest previous catalog version
     * will be returned.
     */
    private List<CatalogIndexDescriptor> getDroppedAvailableIndexes(int catalogVersion, int tableId) {
        var key = new TableIdCatalogVersion(tableId, catalogVersion);

        Entry<TableIdCatalogVersion, List<CatalogIndexDescriptor>> entry = droppedAvailableTableIndexes.floorEntry(key);

        return entry != null && tableId == entry.getKey().tableId ? entry.getValue() : List.of();
    }

    /**
     * Adds the dropped available index to {@link #droppedAvailableTableIndexes}.
     *
     * <p>If the list is missing for the catalog version from the arguments, then we create it by merging the indexes from the previous
     * catalog version and the new index. Otherwise, we simply add to the existing list. List are sorted by {@link #INDEX_COMPARATOR}</p>
     *
     * @param droppedIndex Drooped index.
     * @param catalogVersion Catalog version on which the index was dropped.
     */
    private void addDroppedAvailableIndex(CatalogIndexDescriptor droppedIndex, int catalogVersion) {
        assert droppedIndex.available() : droppedIndex.id();

        int tableId = droppedIndex.tableId();

        // For now, there is no need to worry about parallel changes to the map, it will change on recovery and in catalog event listeners
        // and won't interfere with each other.
        List<CatalogIndexDescriptor> previousCatalogVersionDroppedIndexes = getDroppedAvailableIndexes(catalogVersion - 1, tableId);

        droppedAvailableTableIndexes.compute(
                new TableIdCatalogVersion(tableId, catalogVersion),
                (tableIdCatalogVersion, droppedAvailableIndexes) -> {
                    List<CatalogIndexDescriptor> res;

                    if (droppedAvailableIndexes == null) {
                        res = new ArrayList<>(1 + previousCatalogVersionDroppedIndexes.size());

                        res.addAll(previousCatalogVersionDroppedIndexes);
                    } else {
                        res = new ArrayList<>(1 + droppedAvailableIndexes.size());

                        res.addAll(droppedAvailableIndexes);
                    }

                    res.add(droppedIndex);

                    res.sort(INDEX_COMPARATOR);

                    return unmodifiableList(res);
                });
    }

    private static boolean contains(List<CatalogIndexDescriptor> indexes, CatalogIndexDescriptor index) {
        return binarySearch(indexes, index, INDEX_COMPARATOR) >= 0;
    }

    private static List<CatalogIndexDescriptor> merge(List<CatalogIndexDescriptor> list0, List<CatalogIndexDescriptor> list1) {
        assert !list0.isEmpty();
        assert !list1.isEmpty();

        var res = new ArrayList<CatalogIndexDescriptor>(list0.size() + list1.size());

        for (int i = 0, i0 = 0, i1 = 0; i < list0.size() + list1.size(); i++) {
            if (i0 >= list0.size()) {
                res.add(list1.get(i1++));
            } else if (i1 >= list1.size()) {
                res.add(list0.get(i0++));
            } else {
                CatalogIndexDescriptor index0 = list0.get(i0);
                CatalogIndexDescriptor index1 = list1.get(i1);

                if (INDEX_COMPARATOR.compare(index0, index1) <= 0) {
                    res.add(index0);
                    i0++;
                } else {
                    res.add(index1);
                    i1++;
                }
            }
        }

        return res;
    }
}
