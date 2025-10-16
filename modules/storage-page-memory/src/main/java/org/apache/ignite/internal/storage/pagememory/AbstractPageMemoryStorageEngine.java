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

package org.apache.ignite.internal.storage.pagememory;

import static java.util.Comparator.comparing;
import static org.apache.ignite.internal.util.IgniteUtils.lexicographicListComparator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.pagememory.index.sorted.comparator.JitComparator;
import org.apache.ignite.internal.storage.pagememory.index.sorted.comparator.JitComparatorGenerator;
import org.apache.ignite.internal.storage.pagememory.index.sorted.comparator.JitComparatorOptions;
import org.apache.ignite.internal.type.NativeType;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/** Abstract implementation of the storage engine based on memory {@link PageMemory}. */
public abstract class AbstractPageMemoryStorageEngine implements StorageEngine {
    public static final String LEGACY_PAGE_MEMERY_SORTED_INDEX_COMPARATOR_PROPERTY = "legacyPageMemorySortedIndexComparator";

    protected final @Nullable SystemLocalConfiguration systemLocalConfig;

    private final HybridClock clock;

    private final ConcurrentMap<StorageSortedIndexDescriptor, CachedComparator> cachedSortedIndexComparators
            = new ConcurrentSkipListMap<>(comparing(
                    StorageSortedIndexDescriptor::columns,
                    lexicographicListComparator(comparing(StorageSortedIndexColumnDescriptor::type)
                            .thenComparing(StorageSortedIndexColumnDescriptor::nullable)
                            .thenComparing(StorageSortedIndexColumnDescriptor::nullsFirst)
                            .thenComparing(StorageSortedIndexColumnDescriptor::asc)
                    )
            ));

    /** Constructor. */
    AbstractPageMemoryStorageEngine(@Nullable SystemLocalConfiguration systemLocalConfig, HybridClock clock) {
        this.systemLocalConfig = systemLocalConfig;
        this.clock = clock;
    }

    /**
     * Creates a Global remove ID for structures based on a {@link BplusTree}, always creating monotonically increasing ones even after
     * recovery node, so that there are no errors after restoring trees.
     */
    public AtomicLong generateGlobalRemoveId() {
        return new AtomicLong(clock.nowLong());
    }

    /**
     * Creates a new instance of {@link JitComparator} for the given sorted index descriptor.
     */
    @VisibleForTesting
    public static JitComparator createNewJitComparator(StorageSortedIndexDescriptor desc) {
        List<StorageSortedIndexColumnDescriptor> columns = desc.columns();
        List<CatalogColumnCollation> collations = new ArrayList<>(columns.size());
        List<NativeType> types = new ArrayList<>(columns.size());
        List<Boolean> nullableFlags = new ArrayList<>(columns.size());

        for (StorageSortedIndexColumnDescriptor col : columns) {
            collations.add(CatalogColumnCollation.get(col.asc(), col.nullsFirst()));
            types.add(col.type());
            nullableFlags.add(true);
        }

        return JitComparatorGenerator.createComparator(JitComparatorOptions.builder()
                .columnCollations(collations)
                .columnTypes(types)
                .nullableFlags(nullableFlags)
                .supportPrefixes(true)
                .supportPartialComparison(true)
                .build()
        );
    }

    /**
     * Creates or retrieves from cache a {@link JitComparator} for the given sorted index descriptor. Returns a cached comparator value if
     * it already exists (was not disposed with {@link #disposeSortedIndexComparator(StorageSortedIndexDescriptor)}) for a given descriptor.
     */
    public @Nullable JitComparator createSortedIndexComparator(StorageSortedIndexDescriptor indexDescriptor) {
        if (systemLocalConfig != null) {
            SystemPropertyView legacyComparator = systemLocalConfig.value().properties()
                    .get(LEGACY_PAGE_MEMERY_SORTED_INDEX_COMPARATOR_PROPERTY);

            if (legacyComparator != null && "true".equalsIgnoreCase(legacyComparator.propertyValue())) {
                return null;
            }
        }

        CachedComparator c = cachedSortedIndexComparators.compute(indexDescriptor, (desc, cmp) -> {
            if (cmp != null) {
                return cmp.incrementUsage();
            }

            JitComparator jitComparator = createNewJitComparator(desc);

            return new CachedComparator(jitComparator);
        });

        return c.jitComparator();
    }

    /**
     * Marks that a comparator, created previously with {@link #createSortedIndexComparator(StorageSortedIndexDescriptor)}, will no longer
     * be used, and the internal cache of comparators may react to this information by removing the comparator from the cache and freeing
     * associated resources.
     */
    public void disposeSortedIndexComparator(StorageSortedIndexDescriptor indexDescriptor) {
        cachedSortedIndexComparators.compute(indexDescriptor, (desc, cmp) -> {
            assert cmp != null;

            return cmp.decrementUsage();
        });
    }

    private static class CachedComparator {
        private final JitComparator comparator;
        private final int usageCount;

        private CachedComparator(JitComparator comparator, int usageCount) {
            assert usageCount > 0;

            this.comparator = comparator;
            this.usageCount = usageCount;
        }

        CachedComparator(JitComparator comparator) {
            this(comparator, 1);
        }

        JitComparator jitComparator() {
            return comparator;
        }

        CachedComparator incrementUsage() {
            return new CachedComparator(comparator, usageCount + 1);
        }

        @Nullable AbstractPageMemoryStorageEngine.CachedComparator decrementUsage() {
            return usageCount == 1 ? null : new CachedComparator(comparator, usageCount - 1);
        }
    }
}
