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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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

import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link GroupPageStoresMap} testing.
 */
public class GroupPageStoresMapTest {
    private LongOperationAsyncExecutor longOperationAsyncExecutor;

    private GroupPageStoresMap<PageStore> groupPageStores;

    @BeforeEach
    void setUp() {
        longOperationAsyncExecutor = mock(LongOperationAsyncExecutor.class);

        when(longOperationAsyncExecutor.afterAsyncCompletion(any(Supplier.class)))
                .then(answer -> ((Supplier<?>) answer.getArgument(0)).get());

        groupPageStores = new GroupPageStoresMap<>(longOperationAsyncExecutor);
    }

    @Test
    void testGroupCount() {
        assertEquals(0, groupPageStores.groupCount());

        groupPageStores.put(0, List.of());

        assertEquals(1, groupPageStores.groupCount());

        groupPageStores.put(0, List.of());

        assertEquals(1, groupPageStores.groupCount());

        groupPageStores.put(1, List.of());

        assertEquals(2, groupPageStores.groupCount());

        groupPageStores.clear();

        assertEquals(0, groupPageStores.groupCount());
    }

    @Test
    void testPut() {
        List<PageStore> holder0 = List.of();

        assertNull(groupPageStores.put(0, holder0));
        checkInvokeAfterAsyncCompletion(1);

        List<PageStore> holder1 = List.of();

        assertSame(holder0, groupPageStores.put(0, holder1));
        checkInvokeAfterAsyncCompletion(2);

        assertSame(holder1, groupPageStores.get(0));
        assertNull(groupPageStores.get(1));
    }

    @Test
    void testGet() {
        assertNull(groupPageStores.get(0));
        assertNull(groupPageStores.get(1));

        List<PageStore> pageStores0 = List.of();

        groupPageStores.put(0, pageStores0);

        assertSame(pageStores0, groupPageStores.get(0));
        assertNull(groupPageStores.get(1));

        List<PageStore> pageStores1 = List.of();

        groupPageStores.put(1, pageStores1);

        assertSame(pageStores0, groupPageStores.get(0));
        assertSame(pageStores1, groupPageStores.get(1));

        groupPageStores.clear();

        assertNull(groupPageStores.get(0));
        assertNull(groupPageStores.get(1));
    }

    @Test
    void testContainsPageStores() {
        assertFalse(groupPageStores.containsPageStores(0));
        assertFalse(groupPageStores.containsPageStores(1));

        groupPageStores.put(0, List.of());

        assertTrue(groupPageStores.containsPageStores(0));
        assertFalse(groupPageStores.containsPageStores(1));

        List<PageStore> pageStores1 = List.of();

        groupPageStores.put(1, List.of());

        assertTrue(groupPageStores.containsPageStores(0));
        assertTrue(groupPageStores.containsPageStores(1));

        groupPageStores.clear();

        assertFalse(groupPageStores.containsPageStores(0));
        assertFalse(groupPageStores.containsPageStores(1));
    }

    @Test
    void testAllPageStores() {
        assertThat(groupPageStores.allPageStores(), empty());

        List<PageStore> pageStores0 = List.of();
        List<PageStore> pageStores1 = List.of();

        groupPageStores.put(0, pageStores0);

        assertThat(groupPageStores.allPageStores(), containsInAnyOrder(pageStores0));

        groupPageStores.put(1, pageStores1);

        assertThat(groupPageStores.allPageStores(), containsInAnyOrder(pageStores0, pageStores1));

        groupPageStores.clear();

        assertThat(groupPageStores.allPageStores(), empty());
    }

    @Test
    void testClear() {
        assertDoesNotThrow(groupPageStores::clear);

        groupPageStores.put(0, List.of());

        assertDoesNotThrow(groupPageStores::clear);
        assertEquals(0, groupPageStores.groupCount());
    }

    private void checkInvokeAfterAsyncCompletion(int times) {
        verify(longOperationAsyncExecutor, times(times)).afterAsyncCompletion(any(Supplier.class));
    }
}
