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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Index chooser for various operations, for example for RW transactions. */
class IndexChooser implements ManuallyCloseable {
    private static final Comparator<CatalogIndexDescriptor> INDEX_COMPARATOR = comparingInt(CatalogObjectDescriptor::id);

    private final CatalogService catalogService;

    /**
     * Map that, for each key, contains a list of all indexes that were available, but now are removed (sorted by {@link #INDEX_COMPARATOR})
     * for all known catalog versions.
     *
     * <p>Examples below will be within the same table ID.</p>
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
     *     1 -> [I1(A)]
     *     3 -> [I1(A), I3(A)]
     * </pre>
     *
     * <p>Then, when {@link #getRemovedAvailableIndexes(int, int) getting removed available indexes}, we will return the following:</p>
     * <pre>
     *     0 -> []
     *     1 -> [I1(A)]
     *     2 -> [I1(A)]
     *     3 -> [I1(A), I3(A)]
     *     4 -> [I1(A), I3(A)]
     * </pre>
     *
     * <p>Updated on {@link #recover() node recovery} and a catalog events processing.</p>
     */
    // TODO: IGNITE-20121 We may need to worry about parallel map changes when deleting catalog version
    // TODO: IGNITE-20934 Worry about cleaning up removed indexes earlier
    private final NavigableMap<TableIdCatalogVersion, List<CatalogIndexDescriptor>> removedAvailableTableIndexes
            = new ConcurrentSkipListMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Constructor. */
    IndexChooser(CatalogService catalogService) {
        this.catalogService = catalogService;

        addListeners();
    }

    /** Recovers internal structures on node recovery. */
    void recover() {
        inBusyLock(busyLock, () -> {
            // It is expected that the methods will be called only on recovery, when the deploy of metastore watches has not yet occurred.
            int earliestCatalogVersion = catalogService.earliestCatalogVersion();
            int latestCatalogVersion = catalogService.latestCatalogVersion();

            // At the moment, we will only use tables from the latest version (not dropped), since so far only replicas for them are started
            // on the node.
            for (CatalogTableDescriptor table : catalogService.tables(latestCatalogVersion)) {
                int tableId = table.id();

                for (int catalogVersion = earliestCatalogVersion; catalogVersion < latestCatalogVersion; catalogVersion++) {
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
                        if (tableIndex.status() == STOPPING && !contains(nextCatalogVersionTableIndexes, tableIndex)) {
                            addRemovedAvailableIndex(tableIndex, nextCatalogVersion);
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

        removedAvailableTableIndexes.clear();
    }

    /**
     * Collects a list of table indexes that will need to be used for an update operation in an RW transaction. The list consists of
     * registered, building and available indexes on the requested catalog version, as well as all indexes that were available at
     * some moment, but then were removed, from previous catalog versions.
     *
     * <p>Returned list is sorted by {@link CatalogObjectDescriptor#id()}. The table is expected to exist in the catalog at the requested
     * version.</p>
     *
     * @param catalogVersion Catalog version.
     * @param tableId Table ID.
     */
    List<CatalogIndexDescriptor> chooseForRwTxUpdateOperation(int catalogVersion, int tableId) {
        return inBusyLock(busyLock, () -> {
            List<CatalogIndexDescriptor> tableIndexes = catalogService.indexes(catalogVersion, tableId);

            assert !tableIndexes.isEmpty() : "catalogVersion=" + catalogVersion + ", tableId=" + tableId;

            List<CatalogIndexDescriptor> removedAvailableTableIndexes = getRemovedAvailableIndexes(catalogVersion, tableId);

            if (removedAvailableTableIndexes.isEmpty()) {
                return tableIndexes;
            }

            return unmodifiableList(merge(tableIndexes, removedAvailableTableIndexes));
        });
    }

    private void addListeners() {
        catalogService.listen(CatalogEvent.INDEX_REMOVED, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onIndexRemoved((RemoveIndexEventParameters) parameters).thenApply(unused -> false);
        });

        catalogService.listen(CatalogEvent.TABLE_DROP, (parameters, exception) -> {
            if (exception != null) {
                return failedFuture(exception);
            }

            return onDropTable((DropTableEventParameters) parameters).thenApply(unused -> false);
        });
    }

    private CompletableFuture<?> onIndexRemoved(RemoveIndexEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            int previousCatalogVersion = parameters.catalogVersion() - 1;

            CatalogIndexDescriptor removedIndexDescriptor = catalogService.index(parameters.indexId(), previousCatalogVersion);

            assert removedIndexDescriptor != null : "indexId=" + parameters.indexId() + ", catalogVersion=" + previousCatalogVersion;

            if (removedIndexDescriptor.status() != AVAILABLE && removedIndexDescriptor.status() != STOPPING) {
                return nullCompletedFuture();
            }

            addRemovedAvailableIndex(removedIndexDescriptor, parameters.catalogVersion());

            return nullCompletedFuture();
        });
    }

    private CompletableFuture<?> onDropTable(DropTableEventParameters parameters) {
        return inBusyLockAsync(busyLock, () -> {
            // We can remove removed indexes on table drop as we need such indexes only for writing, and write operations will be denied
            // right after a table drop has been activated.
            removedAvailableTableIndexes.entrySet().removeIf(entry -> parameters.tableId() == entry.getKey().tableId);

            return nullCompletedFuture();
        });
    }

    /**
     * Returns a list of indexes that are/were available, but then were removed (sorted by {@link #INDEX_COMPARATOR}) for the catalog
     * version of interest from {@link #removedAvailableTableIndexes}. If there is no list for the requested catalog version, the closest
     * previous catalog version will be returned.
     */
    private List<CatalogIndexDescriptor> getRemovedAvailableIndexes(int catalogVersion, int tableId) {
        var key = new TableIdCatalogVersion(tableId, catalogVersion);

        Entry<TableIdCatalogVersion, List<CatalogIndexDescriptor>> entry = removedAvailableTableIndexes.floorEntry(key);

        return entry != null && tableId == entry.getKey().tableId ? entry.getValue() : List.of();
    }

    /**
     * Adds a removed index that is (or was) available to {@link #removedAvailableTableIndexes}.
     *
     * <p>If the list is missing for the catalog version from the arguments, then we create it by merging the indexes from the previous
     * catalog version and the new index. Otherwise, we simply add to the existing list. Lists are sorted by {@link #INDEX_COMPARATOR}.</p>
     *
     * @param removedIndex Removed index.
     * @param catalogVersion Catalog version on which the index was removed.
     */
    private void addRemovedAvailableIndex(CatalogIndexDescriptor removedIndex, int catalogVersion) {
        assert removedIndex.status() == AVAILABLE || removedIndex.status() == STOPPING : removedIndex.id();

        int tableId = removedIndex.tableId();

        // For now, there is no need to worry about parallel changes to the map, it will change on recovery and in catalog event listeners
        // and won't interfere with each other.
        // TODO: IGNITE-20121 We may need to worry about parallel map changes when deleting catalog version
        List<CatalogIndexDescriptor> previousCatalogVersionRemovedIndexes = getRemovedAvailableIndexes(catalogVersion - 1, tableId);

        removedAvailableTableIndexes.compute(
                new TableIdCatalogVersion(tableId, catalogVersion),
                (tableIdCatalogVersion, removedAvailableIndexes) -> {
                    List<CatalogIndexDescriptor> res;

                    if (removedAvailableIndexes == null) {
                        res = new ArrayList<>(1 + previousCatalogVersionRemovedIndexes.size());

                        res.addAll(previousCatalogVersionRemovedIndexes);
                    } else {
                        res = new ArrayList<>(1 + removedAvailableIndexes.size());

                        res.addAll(removedAvailableIndexes);
                    }

                    res.add(removedIndex);

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
