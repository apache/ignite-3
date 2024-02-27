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
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.GroupPartitionPageStore;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link GroupPageStoresMap} testing.
 */
public class GroupPageStoresMapTest extends BaseIgniteAbstractTest {
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
    void testPut() {
        FilePageStore filePageStore0 = mock(FilePageStore.class);

        GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);

        assertNull(groupPageStoresMap.put(groupPartitionId00, filePageStore0));
        checkInvokeAfterAsyncCompletion(1);

        FilePageStore filePageStore1 = mock(FilePageStore.class);

        assertSame(filePageStore0, groupPageStoresMap.put(groupPartitionId00, filePageStore1));
        checkInvokeAfterAsyncCompletion(2);

        GroupPartitionId groupPartitionId01 = new GroupPartitionId(0, 1);
        GroupPartitionId groupPartitionId11 = new GroupPartitionId(1, 1);

        assertEquals(filePageStore1, groupPageStoresMap.get(groupPartitionId00));

        assertNull(groupPageStoresMap.get(groupPartitionId01));
        assertNull(groupPageStoresMap.get(groupPartitionId11));

        assertThat(
                getAll(groupPageStoresMap.getAll()),
                containsInAnyOrder(new TestGroupPartitionPageStore<>(groupPartitionId00, filePageStore1))
        );
    }

    @Test
    void testGet() {
        GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);
        GroupPartitionId groupPartitionId10 = new GroupPartitionId(1, 0);

        assertNull(groupPageStoresMap.get(groupPartitionId00));
        assertNull(groupPageStoresMap.get(groupPartitionId10));

        FilePageStore filePageStore0 = mock(FilePageStore.class);

        groupPageStoresMap.put(groupPartitionId00, filePageStore0);

        assertEquals(filePageStore0, groupPageStoresMap.get(groupPartitionId00));

        assertNull(groupPageStoresMap.get(groupPartitionId10));

        assertThat(
                getAll(groupPageStoresMap.getAll()),
                containsInAnyOrder(new TestGroupPartitionPageStore<>(groupPartitionId00, filePageStore0))
        );

        FilePageStore filePageStore1 = mock(FilePageStore.class);

        groupPageStoresMap.put(groupPartitionId10, filePageStore1);

        assertEquals(filePageStore0, groupPageStoresMap.get(groupPartitionId00));
        assertEquals(filePageStore1, groupPageStoresMap.get(groupPartitionId10));

        assertThat(
                getAll(groupPageStoresMap.getAll()),
                containsInAnyOrder(
                        new TestGroupPartitionPageStore<>(groupPartitionId00, filePageStore0),
                        new TestGroupPartitionPageStore<>(groupPartitionId10, filePageStore1)
                )
        );

        groupPageStoresMap.clear();

        assertNull(groupPageStoresMap.get(groupPartitionId00));
        assertNull(groupPageStoresMap.get(groupPartitionId10));

        assertThat(getAll(groupPageStoresMap.getAll()), empty());
    }

    @Test
    void testContainsPageStores() {
        GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);
        GroupPartitionId groupPartitionId10 = new GroupPartitionId(1, 0);

        assertFalse(groupPageStoresMap.contains(groupPartitionId00));
        assertFalse(groupPageStoresMap.contains(groupPartitionId10));

        FilePageStore filePageStore0 = mock(FilePageStore.class);

        groupPageStoresMap.put(groupPartitionId00, filePageStore0);

        GroupPartitionId groupPartitionId01 = new GroupPartitionId(0, 1);

        assertTrue(groupPageStoresMap.contains(groupPartitionId00));
        assertFalse(groupPageStoresMap.contains(groupPartitionId10));
        assertFalse(groupPageStoresMap.contains(groupPartitionId01));

        FilePageStore filePageStore1 = mock(FilePageStore.class);

        groupPageStoresMap.put(groupPartitionId10, filePageStore1);

        assertTrue(groupPageStoresMap.contains(groupPartitionId00));
        assertTrue(groupPageStoresMap.contains(groupPartitionId10));
        assertFalse(groupPageStoresMap.contains(groupPartitionId01));

        groupPageStoresMap.clear();

        assertFalse(groupPageStoresMap.contains(groupPartitionId00));
        assertFalse(groupPageStoresMap.contains(groupPartitionId10));
        assertFalse(groupPageStoresMap.contains(groupPartitionId01));
    }

    @Test
    void testGetAll() {
        assertThat(groupPageStoresMap.getAll().collect(toList()), empty());

        FilePageStore filePageStore0 = mock(FilePageStore.class);
        FilePageStore filePageStore1 = mock(FilePageStore.class);

        GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);

        groupPageStoresMap.put(groupPartitionId00, filePageStore0);

        assertThat(
                getAll(groupPageStoresMap.getAll()),
                containsInAnyOrder(new TestGroupPartitionPageStore<>(groupPartitionId00, filePageStore0))
        );

        GroupPartitionId groupPartitionId11 = new GroupPartitionId(1, 1);

        groupPageStoresMap.put(groupPartitionId11, filePageStore1);

        assertThat(
                getAll(groupPageStoresMap.getAll()),
                containsInAnyOrder(
                        new TestGroupPartitionPageStore<>(groupPartitionId00, filePageStore0),
                        new TestGroupPartitionPageStore<>(groupPartitionId11, filePageStore1)
                )
        );

        groupPageStoresMap.clear();

        assertThat(groupPageStoresMap.getAll().collect(toList()), empty());
    }

    @Test
    void testClear() {
        assertDoesNotThrow(groupPageStoresMap::clear);

        groupPageStoresMap.put(new GroupPartitionId(0, 0), mock(FilePageStore.class));

        assertDoesNotThrow(groupPageStoresMap::clear);

        assertThat(getAll(groupPageStoresMap.getAll()), empty());
    }

    @Test
    void testRemove() {
        GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);

        assertNull(groupPageStoresMap.remove(groupPartitionId00));

        FilePageStore filePageStore = mock(FilePageStore.class);

        groupPageStoresMap.put(groupPartitionId00, filePageStore);

        GroupPartitionId groupPartitionId10 = new GroupPartitionId(1, 0);

        assertNull(groupPageStoresMap.remove(groupPartitionId10));

        assertEquals(filePageStore, groupPageStoresMap.remove(groupPartitionId00));

        assertNull(groupPageStoresMap.get(groupPartitionId00));

        assertFalse(groupPageStoresMap.contains(groupPartitionId00));

        assertThat(getAll(groupPageStoresMap.getAll()), empty());
    }

    private void checkInvokeAfterAsyncCompletion(int times) {
        verify(longOperationAsyncExecutor, times(times)).afterAsyncCompletion(any(Supplier.class));
    }

    private static <T extends PageStore> Collection<TestGroupPartitionPageStore<T>> getAll(
            Stream<GroupPartitionPageStore<T>> groupPartitionPageStores
    ) {
        return groupPartitionPageStores
                .map(TestGroupPartitionPageStore::of)
                .collect(toList());
    }

    /**
     * Inner class for tests.
     */
    private static class TestGroupPartitionPageStore<T extends PageStore> {
        @IgniteToStringInclude
        final GroupPartitionId groupPartitionId;

        @IgniteToStringInclude
        final T filePageStore;

        private TestGroupPartitionPageStore(GroupPartitionId groupPartitionId, T filePageStore) {
            this.groupPartitionId = groupPartitionId;
            this.filePageStore = filePageStore;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj instanceof TestGroupPartitionPageStore) {
                TestGroupPartitionPageStore<?> that = (TestGroupPartitionPageStore<?>) obj;

                return groupPartitionId.equals(that.groupPartitionId) && filePageStore.equals(that.filePageStore);
            }

            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupPartitionId, filePageStore);
        }

        @Override
        public String toString() {
            return S.toString(TestGroupPartitionPageStore.class, this);
        }

        private static <T extends PageStore> TestGroupPartitionPageStore<T> of(GroupPartitionPageStore<T> groupPartitionPageStore) {
            return new TestGroupPartitionPageStore<>(groupPartitionPageStore.groupPartitionId(), groupPartitionPageStore.pageStore());
        }
    }
}
