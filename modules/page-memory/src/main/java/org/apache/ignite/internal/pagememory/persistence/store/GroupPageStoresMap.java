/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.Nullable;

/**
 * Proxy class for map (grpId -> list of page stores) that wraps data adding and replacing operations to disallow concurrent execution
 * simultaneously with cleanup of file page storage.
 *
 * <p>Wrapping of data removing operations is not needed.
 *
 * @param <T> Type of {@link PageStore}.
 */
class GroupPageStoresMap<T extends PageStore> {
    /** Mapping: grpId -> list of page stores. */
    private final ConcurrentHashMap<Integer, List<T>> groupPageStores = new ConcurrentHashMap<>();

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
     * Puts the page stores for the group.
     *
     * @param grpId Group ID.
     * @param pageStores Page stores.
     * @return Previous page stores.
     */
    public @Nullable List<T> put(Integer grpId, List<T> pageStores) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> groupPageStores.put(grpId, pageStores));
    }

    /**
     * Returns the page stores for the group.
     *
     * @param grpId Group ID.
     */
    public @Nullable List<T> get(Integer grpId) {
        return groupPageStores.get(grpId);
    }

    /**
     * Returns {@code true} if a page stores exists for the group.
     *
     * @param grpId Group ID.
     */
    public boolean containsPageStores(Integer grpId) {
        return groupPageStores.containsKey(grpId);
    }

    /**
     * Returns all page stores of all groups.
     */
    public Collection<List<T>> allPageStores() {
        return groupPageStores.values();
    }

    /**
     * Clears all page stores of all groups.
     */
    public void clear() {
        groupPageStores.clear();
    }

    /**
     * Returns the number of groups for which there are page stores.
     */
    public int groupCount() {
        return groupPageStores.size();
    }
}
