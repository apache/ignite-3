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

import static java.util.stream.Collectors.toList;
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

import java.util.Collection;
import java.util.function.Supplier;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.GroupPageStores;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.PartitionPageStore;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link GroupPageStoresMap} testing.
 */
public class GroupPageStoresMapTest {
    private LongOperationAsyncExecutor longOperationAsyncExecutor;

    private GroupPageStoresMap<PageStore> groupPageStoresMap;

    @BeforeEach
    void setUp() {
        longOperationAsyncExecutor = mock(LongOperationAsyncExecutor.class);

        when(longOperationAsyncExecutor.afterAsyncCompletion(any(Supplier.class)))
                .then(answer -> ((Supplier<?>) answer.getArgument(0)).get());

        groupPageStoresMap = new GroupPageStoresMap<>(longOperationAsyncExecutor);
    }

    @Test
    void testGroupCount() {
        assertEquals(0, groupPageStoresMap.groupCount());

        groupPageStoresMap.put(0, 0, mock(FilePageStore.class));

        assertEquals(1, groupPageStoresMap.groupCount());

        groupPageStoresMap.put(0, 0, mock(FilePageStore.class));

        assertEquals(1, groupPageStoresMap.groupCount());

        groupPageStoresMap.put(1, 0, mock(FilePageStore.class));

        assertEquals(2, groupPageStoresMap.groupCount());

        groupPageStoresMap.clear();

        assertEquals(0, groupPageStoresMap.groupCount());
    }

    @Test
    void testPut() {
        FilePageStore filePageStore0 = mock(FilePageStore.class);

        assertNull(groupPageStoresMap.put(0, 0, filePageStore0));
        checkInvokeAfterAsyncCompletion(1);

        FilePageStore filePageStore1 = mock(FilePageStore.class);

        assertSame(filePageStore0, groupPageStoresMap.put(0, 0, filePageStore1));
        checkInvokeAfterAsyncCompletion(2);

        assertNull(groupPageStoresMap.get(1));

        assertThat(getAll(groupPageStoresMap.get(0)), containsInAnyOrder(new TestPartitionPageStore<>(0, filePageStore1)));
    }

    @Test
    void testGet() {
        assertNull(groupPageStoresMap.get(0));
        assertNull(groupPageStoresMap.get(1));

        FilePageStore filePageStore0 = mock(FilePageStore.class);

        groupPageStoresMap.put(0, 0, filePageStore0);

        GroupPageStores<PageStore> groupPageStores0 = groupPageStoresMap.get(0);

        assertThat(getAll(groupPageStores0), containsInAnyOrder(new TestPartitionPageStore<>(0, filePageStore0)));
        assertNull(groupPageStoresMap.get(1));

        FilePageStore filePageStore1 = mock(FilePageStore.class);

        groupPageStoresMap.put(1, 1, filePageStore1);

        GroupPageStores<PageStore> groupPageStores1 = groupPageStoresMap.get(1);

        assertThat(getAll(groupPageStores0), containsInAnyOrder(new TestPartitionPageStore<>(0, filePageStore0)));
        assertThat(getAll(groupPageStores1), containsInAnyOrder(new TestPartitionPageStore<>(1, filePageStore1)));

        assertEquals(TestPartitionPageStore.of(groupPageStores0.get(0)), new TestPartitionPageStore<>(0, filePageStore0));
        assertEquals(TestPartitionPageStore.of(groupPageStores1.get(1)), new TestPartitionPageStore<>(1, filePageStore1));

        assertNull(groupPageStores0.get(2));
        assertNull(groupPageStores1.get(2));

        groupPageStoresMap.clear();

        assertNull(groupPageStoresMap.get(0));
        assertNull(groupPageStoresMap.get(1));
    }

    @Test
    void testContainsPageStores() {
        assertFalse(groupPageStoresMap.contains(0, 0));
        assertFalse(groupPageStoresMap.contains(1, 0));

        FilePageStore filePageStore0 = mock(FilePageStore.class);

        groupPageStoresMap.put(0, 0, filePageStore0);

        assertTrue(groupPageStoresMap.contains(0, 0));
        assertFalse(groupPageStoresMap.contains(1, 0));
        assertFalse(groupPageStoresMap.contains(0, 1));

        FilePageStore filePageStore1 = mock(FilePageStore.class);

        groupPageStoresMap.put(1, 0, filePageStore1);

        assertTrue(groupPageStoresMap.contains(0, 0));
        assertTrue(groupPageStoresMap.contains(1, 0));

        groupPageStoresMap.clear();

        assertFalse(groupPageStoresMap.contains(0, 0));
        assertFalse(groupPageStoresMap.contains(1, 0));
    }

    @Test
    void testGetAll() {
        assertThat(groupPageStoresMap.getAll(), empty());

        FilePageStore filePageStore0 = mock(FilePageStore.class);
        FilePageStore filePageStore1 = mock(FilePageStore.class);

        groupPageStoresMap.put(0, 0, filePageStore0);

        assertThat(
                getAll(groupPageStoresMap),
                containsInAnyOrder(new TestGroupPartitionPageStore<>(0, 0, filePageStore0))
        );

        groupPageStoresMap.put(1, 1, filePageStore1);

        assertThat(
                getAll(groupPageStoresMap),
                containsInAnyOrder(
                        new TestGroupPartitionPageStore<>(0, 0, filePageStore0),
                        new TestGroupPartitionPageStore<>(1, 1, filePageStore1)
                )
        );

        groupPageStoresMap.clear();

        assertThat(groupPageStoresMap.getAll(), empty());
    }

    @Test
    void testClear() {
        assertDoesNotThrow(groupPageStoresMap::clear);

        groupPageStoresMap.put(0, 0, mock(FilePageStore.class));

        assertDoesNotThrow(groupPageStoresMap::clear);
        assertEquals(0, groupPageStoresMap.groupCount());

        assertNull(groupPageStoresMap.get(0));
        assertFalse(groupPageStoresMap.contains(0, 0));
    }

    @Test
    void testRemove() {
        assertNull(groupPageStoresMap.remove(0, 0));

        FilePageStore filePageStore = mock(FilePageStore.class);

        groupPageStoresMap.put(0, 0, filePageStore);

        GroupPageStores<PageStore> groupPageStores = groupPageStoresMap.get(0);

        assertNull(groupPageStoresMap.remove(0, 1));
        assertNull(groupPageStoresMap.remove(1, 0));

        assertSame(filePageStore, groupPageStoresMap.remove(0, 0));

        assertNull(groupPageStoresMap.get(0));
        assertNull(groupPageStores.get(0));

        assertFalse(groupPageStoresMap.contains(0, 0));
        assertEquals(0, groupPageStoresMap.groupCount());
    }

    private void checkInvokeAfterAsyncCompletion(int times) {
        verify(longOperationAsyncExecutor, times(times)).afterAsyncCompletion(any(Supplier.class));
    }

    private static <T extends PageStore> Collection<TestGroupPartitionPageStore<T>> getAll(GroupPageStoresMap<T> groupPageStoresMap) {
        return groupPageStoresMap.getAll().stream()
                .flatMap(groupPageStores -> groupPageStores.getAll().stream()
                        .map(partitionPageStore -> new TestGroupPartitionPageStore<>(
                                groupPageStores.groupId(),
                                partitionPageStore.partitionId(),
                                partitionPageStore.pageStore()
                        ))
                )
                .collect(toList());
    }

    private static <T extends PageStore> Collection<TestPartitionPageStore<T>> getAll(GroupPageStores<T> groupPageStores) {
        return groupPageStores.getAll().stream()
                .map(partitionPageStore -> new TestPartitionPageStore<>(partitionPageStore.partitionId(), partitionPageStore.pageStore()))
                .collect(toList());
    }

    /**
     * Inner class for tests.
     */
    private static class TestGroupPartitionPageStore<T extends PageStore> extends TestPartitionPageStore<T> {
        @IgniteToStringInclude
        final int groupId;

        private TestGroupPartitionPageStore(int groupId, int partitionId, T filePageStore) {
            super(partitionId, filePageStore);

            this.groupId = groupId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj instanceof TestGroupPartitionPageStore) {
                TestGroupPartitionPageStore that = (TestGroupPartitionPageStore) obj;

                return groupId == that.groupId && partitionId == that.partitionId && filePageStore == that.filePageStore;
            }

            return false;
        }

        @Override
        public String toString() {
            return S.toString(TestGroupPartitionPageStore.class, this, "partitionId", partitionId, "filePageStore", filePageStore);
        }
    }

    /**
     * Inner class for tests.
     */
    private static class TestPartitionPageStore<T extends PageStore> {
        @IgniteToStringInclude
        final int partitionId;

        @IgniteToStringInclude
        final T filePageStore;

        private TestPartitionPageStore(int partitionId, T filePageStore) {
            this.partitionId = partitionId;
            this.filePageStore = filePageStore;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj instanceof TestPartitionPageStore) {
                TestPartitionPageStore that = (TestPartitionPageStore) obj;

                return partitionId == that.partitionId && filePageStore.equals(that.filePageStore);
            }

            return false;
        }

        @Override
        public String toString() {
            return S.toString(TestPartitionPageStore.class, this);
        }

        private static <T extends PageStore> TestPartitionPageStore<T> of(PartitionPageStore<T> partitionPageStore) {
            return new TestPartitionPageStore<>(partitionPageStore.partitionId(), partitionPageStore.pageStore());
        }
    }
}
