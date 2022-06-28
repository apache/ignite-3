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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Proxy class for map (grpId -> page store list) that wraps data adding and replacing operations to disallow concurrent execution
 * simultaneously with cleanup of file page storage.
 *
 * <p>Wrapping of data removing operations is not needed.
 *
 * @param <T> Type of {@link PageStore}.
 */
class GroupPageStoreHolderMap<T extends PageStore> extends ConcurrentHashMap<Integer, List<T>> {
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
    public List<T> put(Integer grpId, List<T> val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.put(grpId, val));
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(Map<? extends Integer, ? extends List<T>> m) {
        longOperationAsyncExecutor.afterAsyncCompletion(() -> {
            super.putAll(m);

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public List<T> putIfAbsent(Integer grpId, List<T> val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.putIfAbsent(grpId, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(Integer grpId, List<T> oldVal, List<T> newVal) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.replace(grpId, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public List<T> replace(Integer grpId, List<T> val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.replace(grpId, val));
    }

    /** {@inheritDoc} */
    @Override
    public List<T> computeIfAbsent(
            Integer grpId,
            Function<? super Integer, ? extends List<T>> mappingFunction
    ) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.computeIfAbsent(grpId, mappingFunction));
    }

    /** {@inheritDoc} */
    @Override
    public List<T> computeIfPresent(
            Integer grpId,
            BiFunction<? super Integer, ? super List<T>, ? extends List<T>> remappingFunction
    ) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.computeIfPresent(grpId, remappingFunction));
    }

    /** {@inheritDoc} */
    @Override
    public List<T> compute(
            Integer grpId,
            BiFunction<? super Integer, ? super List<T>, ? extends List<T>> remappingFunction
    ) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.compute(grpId, remappingFunction));
    }

    /** {@inheritDoc} */
    @Override
    public List<T> merge(
            Integer grpId,
            List<T> val,
            BiFunction<? super List<T>, ? super List<T>, ? extends List<T>> remappingFun
    ) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.merge(grpId, val, remappingFun));
    }
}
