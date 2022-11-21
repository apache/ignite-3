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

package org.apache.ignite.internal.pagememory.persistence.store;

import static java.util.Collections.unmodifiableCollection;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;

/**
 * Proxy class for map (groupId -> {@link GroupPageStores}) that wraps data adding and replacing operations to disallow concurrent execution
 * simultaneously with cleanup of file page storage.
 *
 * <p>Wrapping of data removing operations is not needed.
 *
 * @param <T> Type of {@link PageStore}.
 */
public class GroupPageStoresMap<T extends PageStore> {
    /** Mapping: grpId -> list of page stores. */
    private final ConcurrentHashMap<Integer, GroupPageStores<T>> groupIdPageStores = new ConcurrentHashMap<>();

    /** Executor that wraps data adding and replacing operations. */
    private final LongOperationAsyncExecutor longOperationAsyncExecutor;

    /**
     * Constructor.
     *
     * @param longOperationAsyncExecutor Executor that wraps data adding and replacing operations.
     */
    public GroupPageStoresMap(LongOperationAsyncExecutor longOperationAsyncExecutor) {
        this.longOperationAsyncExecutor = longOperationAsyncExecutor;
    }

    /**
     * Puts the page store for the group partition.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @param pageStore Page store.
     * @return Previous page store.
     */
    public @Nullable T put(Integer groupId, Integer partitionId, T pageStore) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> {
                    PartitionPageStore<T> previous = groupIdPageStores
                            .computeIfAbsent(groupId, id -> new GroupPageStores<>(groupId))
                            .partitionIdPageStore
                            .put(partitionId, new PartitionPageStore<>(partitionId, pageStore));

                    return previous == null ? null : previous.pageStore;
                }
        );
    }

    /**
     * Removes the page store for the group partition.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @return Removed page store.
     */
    public @Nullable T remove(Integer groupId, Integer partitionId) {
        AtomicReference<PartitionPageStore<T>> partitionPageStoreRef = new AtomicReference<>();

        groupIdPageStores.compute(groupId, (id, groupPageStores) -> {
            if (groupPageStores == null) {
                return null;
            }

            partitionPageStoreRef.set(groupPageStores.partitionIdPageStore.remove(partitionId));

            if (groupPageStores.partitionIdPageStore.isEmpty()) {
                return null;
            }

            return groupPageStores;
        });

        PartitionPageStore<T> partitionPageStore = partitionPageStoreRef.get();

        return partitionPageStore == null ? null : partitionPageStore.pageStore;
    }

    /**
     * Returns the page stores for the group.
     *
     * @param groupId Group ID.
     */
    public @Nullable GroupPageStores<T> get(Integer groupId) {
        return groupIdPageStores.get(groupId);
    }

    /**
     * Returns {@code true} if a page store exists for the group partition.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     */
    public boolean contains(Integer groupId, Integer partitionId) {
        GroupPageStores<T> groupPageStores = groupIdPageStores.get(groupId);

        return groupPageStores != null && groupPageStores.partitionIdPageStore.containsKey(partitionId);
    }

    /**
     * Returns a view of all page stores of all groups.
     */
    public Collection<GroupPageStores<T>> getAll() {
        return unmodifiableCollection(groupIdPageStores.values());
    }

    /**
     * Clears all page stores of all groups.
     */
    public void clear() {
        groupIdPageStores.clear();
    }

    /**
     * Returns the number of groups for which there are page stores.
     */
    public int groupCount() {
        return groupIdPageStores.size();
    }

    /**
     * Group partition page stores.
     */
    public static class GroupPageStores<T extends PageStore> {
        private final int groupId;

        private final ConcurrentMap<Integer, PartitionPageStore<T>> partitionIdPageStore = new ConcurrentHashMap<>();

        private GroupPageStores(int groupId) {
            this.groupId = groupId;
        }

        /**
         * Returns the group ID.
         */
        public int groupId() {
            return groupId;
        }

        /**
         * Returns the partition's page store.
         */
        @Nullable
        public PartitionPageStore<T> get(Integer partitionId) {
            return partitionIdPageStore.get(partitionId);
        }

        /**
         * Returns a view of the group's partition page stores.
         */
        public Collection<PartitionPageStore<T>> getAll() {
            return unmodifiableCollection(partitionIdPageStore.values());
        }
    }

    /**
     * Partition page store.
     */
    public static class PartitionPageStore<T extends PageStore> {
        private final int partitionId;

        private final T pageStore;

        private PartitionPageStore(int partitionId, T pageStore) {
            this.partitionId = partitionId;
            this.pageStore = pageStore;
        }

        /**
         * Returns the partition ID.
         */
        public int partitionId() {
            return partitionId;
        }

        /**
         * Returns the page store.
         */
        public T pageStore() {
            return pageStore;
        }
    }
}
