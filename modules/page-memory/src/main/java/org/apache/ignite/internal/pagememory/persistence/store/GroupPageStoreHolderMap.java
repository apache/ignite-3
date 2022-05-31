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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Proxy class for map (grpId -> page store holder) that wraps data adding and replacing operations to disallow concurrent execution
 * simultaneously with cleanup of file page storage.
 *
 * <p>Wrapping of data removing operations is not needed.
 *
 * @param <T> Type of {@link PageStore}.
 */
class GroupPageStoreHolderMap<T extends PageStore> extends ConcurrentHashMap<Integer, GroupPageStoreHolder<T>> {
    /** Executor that wraps data adding and replacing operations. */
    private final LongOperationAsyncExecutor longOperationAsyncExecutor;

    /**
     * Constructor.
     *
     * @param longOperationAsyncExecutor Executor that wraps data adding and replacing operations.
     */
    public GroupPageStoreHolderMap(LongOperationAsyncExecutor longOperationAsyncExecutor) {
        this.longOperationAsyncExecutor = longOperationAsyncExecutor;
    }

    /** {@inheritDoc} */
    @Override
    public GroupPageStoreHolder<T> put(Integer grpId, GroupPageStoreHolder<T> val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.put(grpId, val));
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(Map<? extends Integer, ? extends GroupPageStoreHolder<T>> m) {
        longOperationAsyncExecutor.afterAsyncCompletion(() -> {
            super.putAll(m);

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public GroupPageStoreHolder<T> putIfAbsent(Integer grpId, GroupPageStoreHolder<T> val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.putIfAbsent(grpId, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(Integer grpId, GroupPageStoreHolder<T> oldVal, GroupPageStoreHolder<T> newVal) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.replace(grpId, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public GroupPageStoreHolder<T> replace(Integer grpId, GroupPageStoreHolder<T> val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.replace(grpId, val));
    }

    /** {@inheritDoc} */
    @Override
    public GroupPageStoreHolder<T> computeIfAbsent(
            Integer grpId,
            Function<? super Integer, ? extends GroupPageStoreHolder<T>> mappingFunction
    ) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.computeIfAbsent(grpId, mappingFunction));
    }

    /** {@inheritDoc} */
    @Override
    public GroupPageStoreHolder<T> computeIfPresent(
            Integer grpId,
            BiFunction<? super Integer, ? super GroupPageStoreHolder<T>, ? extends GroupPageStoreHolder<T>> remappingFunction
    ) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.computeIfPresent(grpId, remappingFunction));
    }

    /** {@inheritDoc} */
    @Override
    public GroupPageStoreHolder<T> compute(
            Integer grpId,
            BiFunction<? super Integer, ? super GroupPageStoreHolder<T>, ? extends GroupPageStoreHolder<T>> remappingFunction
    ) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.compute(grpId, remappingFunction));
    }

    /** {@inheritDoc} */
    @Override
    public GroupPageStoreHolder<T> merge(
            Integer grpId,
            GroupPageStoreHolder<T> val,
            BiFunction<? super GroupPageStoreHolder<T>, ? super GroupPageStoreHolder<T>, ? extends GroupPageStoreHolder<T>> remappingFun
    ) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.merge(grpId, val, remappingFun));
    }
}
