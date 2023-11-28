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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.jetbrains.annotations.Nullable;

/**
 * Proxy class for map ({@link GroupPartitionId} -> {@link PageStore}) that wraps data adding and replacing operations to disallow
 * concurrent execution simultaneously with cleanup of page storage.
 *
 * <p>Wrapping of data removing operations is not needed.
 *
 * @param <T> Type of {@link PageStore}.
 */
public class GroupPageStoresMap<T extends PageStore> {
    private final ConcurrentHashMap<GroupPartitionId, T> groupPartitionIdPageStore = new ConcurrentHashMap<>();

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
     * @param groupPartitionId Pair of group ID with partition ID.
     * @param pageStore Page store.
     * @return Previous page store.
     */
    public @Nullable T put(GroupPartitionId groupPartitionId, T pageStore) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> groupPartitionIdPageStore.put(groupPartitionId, pageStore));
    }

    /**
     * Gets the page store for the group partition.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     */
    public @Nullable T get(GroupPartitionId groupPartitionId) {
        return groupPartitionIdPageStore.get(groupPartitionId);
    }

    /**
     * Removes the page store for the group partition.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @return Removed page store.
     */
    public @Nullable T remove(GroupPartitionId groupPartitionId) {
        return groupPartitionIdPageStore.remove(groupPartitionId);
    }

    /**
     * Returns {@code true} if a page store exists for the group partition.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     */
    public boolean contains(GroupPartitionId groupPartitionId) {
        return groupPartitionIdPageStore.containsKey(groupPartitionId);
    }

    /**
     * Returns a view of all page stores of all groups.
     */
    public Stream<GroupPartitionPageStore<T>> getAll() {
        return groupPartitionIdPageStore.entrySet().stream().map(GroupPartitionPageStore::new);
    }

    /**
     * Clears all page stores of all groups.
     */
    public void clear() {
        groupPartitionIdPageStore.clear();
    }

    /**
     * Attempts to compute a mapping for the group partition and its current mapped value (or {@code null} if there is no current mapping).
     * The entire method invocation is performed atomically.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @param remappingFunction Function to compute a value.
     */
    public T compute(GroupPartitionId groupPartitionId, Function<? super T, ? extends T> remappingFunction) {
        return groupPartitionIdPageStore.compute(groupPartitionId, (groupPartitionId1, t) -> remappingFunction.apply(t));
    }

    /**
     * Group partition page storage.
     */
    public static class GroupPartitionPageStore<T> {
        private final GroupPartitionId groupPartitionId;

        private final T pageStore;

        private GroupPartitionPageStore(Map.Entry<GroupPartitionId, T> entry) {
            this.groupPartitionId = entry.getKey();
            this.pageStore = entry.getValue();
        }

        /**
         * Returns pair of group ID with partition ID.
         */
        public GroupPartitionId groupPartitionId() {
            return groupPartitionId;
        }

        /**
         * Returns page store.
         */
        public T pageStore() {
            return pageStore;
        }
    }
}
