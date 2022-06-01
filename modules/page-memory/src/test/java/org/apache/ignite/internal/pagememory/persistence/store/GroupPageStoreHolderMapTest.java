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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link GroupPageStoreHolderMap} testing.
 */
public class GroupPageStoreHolderMapTest {
    private LongOperationAsyncExecutor longOperationAsyncExecutor;

    private GroupPageStoreHolderMap<PageStore> holderMap;

    @BeforeEach
    void setUp() {
        longOperationAsyncExecutor = mock(LongOperationAsyncExecutor.class);

        when(longOperationAsyncExecutor.afterAsyncCompletion(any(Supplier.class)))
                .then(answer -> ((Supplier<?>) answer.getArgument(0)).get());

        holderMap = new GroupPageStoreHolderMap<>(longOperationAsyncExecutor);
    }

    @Test
    void testPut() {
        GroupPageStoreHolder<PageStore> holder0 = createHolder();

        assertNull(holderMap.put(0, holder0));
        checkInvokeAfterAsyncCompletion(1);

        GroupPageStoreHolder<PageStore> holder1 = createHolder();

        assertSame(holder0, holderMap.put(0, holder1));
        checkInvokeAfterAsyncCompletion(2);

        assertEquals(1, holderMap.size());

        assertSame(holder1, holderMap.get(0));
        assertNull(holderMap.get(1));
    }

    @Test
    void testPutAll() {
        Map<Integer, GroupPageStoreHolder<PageStore>> expHolders = Map.of(0, createHolder(), 1, createHolder());

        holderMap.putAll(expHolders);

        checkInvokeAfterAsyncCompletion(1);

        assertEquals(2, expHolders.size());

        assertSame(expHolders.get(0), holderMap.get(0));
        assertSame(expHolders.get(1), holderMap.get(1));

        assertNull(holderMap.get(2));
    }

    @Test
    void testPutIfAbsent() {
        GroupPageStoreHolder<PageStore> holder0 = createHolder();

        assertNull(holderMap.putIfAbsent(0, holder0));

        checkInvokeAfterAsyncCompletion(1);

        GroupPageStoreHolder<PageStore> holder1 = createHolder();

        assertSame(holder0, holderMap.putIfAbsent(0, holder1));

        checkInvokeAfterAsyncCompletion(2);

        assertEquals(1, holderMap.size());

        assertSame(holder0, holderMap.get(0));

        assertNull(holderMap.get(1));
    }

    @Test
    void testReplaceMappedValue() {
        GroupPageStoreHolder<PageStore> holder0 = createHolder();

        holderMap.put(0, holder0);

        checkInvokeAfterAsyncCompletion(1);

        GroupPageStoreHolder<PageStore> holder1 = createHolder();
        GroupPageStoreHolder<PageStore> holder2 = createHolder();

        assertTrue(holderMap.replace(0, holder0, holder1));

        checkInvokeAfterAsyncCompletion(2);

        assertFalse(holderMap.replace(0, holder0, holder1));
        assertFalse(holderMap.replace(0, holder0, holder2));
        assertFalse(holderMap.replace(1, holder0, holder2));

        checkInvokeAfterAsyncCompletion(5);

        assertEquals(1, holderMap.size());

        assertSame(holder1, holderMap.get(0));

        assertNull(holderMap.get(1));
    }

    @Test
    void testReplace() {
        GroupPageStoreHolder<PageStore> holder0 = createHolder();

        holderMap.put(0, holder0);

        checkInvokeAfterAsyncCompletion(1);

        GroupPageStoreHolder<PageStore> holder1 = createHolder();
        GroupPageStoreHolder<PageStore> holder2 = createHolder();

        assertSame(holder0, holderMap.replace(0, holder1));
        assertNull(holderMap.replace(1, holder2));

        checkInvokeAfterAsyncCompletion(3);

        assertEquals(1, holderMap.size());

        assertSame(holder1, holderMap.get(0));

        assertNull(holderMap.get(1));
    }

    @Test
    void testComputeIfAbsent() {
        GroupPageStoreHolder<PageStore> holder0 = createHolder();

        assertSame(holder0, holderMap.computeIfAbsent(0, grpId -> holder0));

        checkInvokeAfterAsyncCompletion(1);

        GroupPageStoreHolder<PageStore> holder1 = createHolder();

        assertSame(holder0, holderMap.computeIfAbsent(0, grpId -> holder1));

        checkInvokeAfterAsyncCompletion(2);

        assertEquals(1, holderMap.size());

        assertSame(holder0, holderMap.get(0));

        assertNull(holderMap.get(1));
    }

    @Test
    void testComputeIfPresent() {
        GroupPageStoreHolder<PageStore> holder0 = createHolder();

        holderMap.put(0, holder0);

        checkInvokeAfterAsyncCompletion(1);

        GroupPageStoreHolder<PageStore> holder1 = createHolder();
        GroupPageStoreHolder<PageStore> holder2 = createHolder();

        assertSame(holder1, holderMap.computeIfPresent(0, (grpId, oldVal) -> holder1));

        assertNull(holderMap.computeIfPresent(1, (grpId, oldVal) -> holder2));

        checkInvokeAfterAsyncCompletion(3);

        assertEquals(1, holderMap.size());

        assertSame(holder1, holderMap.get(0));

        assertNull(holderMap.get(1));
    }

    @Test
    void testCompute() {
        GroupPageStoreHolder<PageStore> holder0 = createHolder();

        assertSame(holder0, holderMap.compute(0, (grpId, oldVal) -> holder0));

        checkInvokeAfterAsyncCompletion(1);

        GroupPageStoreHolder<PageStore> holder1 = createHolder();
        GroupPageStoreHolder<PageStore> holder2 = createHolder();

        assertSame(holder1, holderMap.compute(0, (grpId, oldVal) -> holder1));
        assertSame(holder2, holderMap.compute(1, (grpId, oldVal) -> holder2));

        checkInvokeAfterAsyncCompletion(3);

        assertEquals(2, holderMap.size());

        assertSame(holder1, holderMap.get(0));
        assertSame(holder2, holderMap.get(1));

        assertNull(holderMap.get(2));
    }

    @Test
    void testMerge() {
        GroupPageStoreHolder<PageStore> holder0 = createHolder();

        assertSame(holder0, holderMap.merge(0, holder0, (h0, h1) -> h1));

        checkInvokeAfterAsyncCompletion(1);

        GroupPageStoreHolder<PageStore> holder1 = createHolder();

        assertSame(holder1, holderMap.merge(0, holder1, (h0, h1) -> h1));

        checkInvokeAfterAsyncCompletion(2);

        assertEquals(1, holderMap.size());

        assertSame(holder1, holderMap.get(0));

        assertNull(holderMap.get(1));
    }

    private GroupPageStoreHolder<PageStore> createHolder() {
        return mock(GroupPageStoreHolder.class);
    }

    private void checkInvokeAfterAsyncCompletion(int times) {
        verify(longOperationAsyncExecutor, times(times)).afterAsyncCompletion(any(Supplier.class));
    }
}
