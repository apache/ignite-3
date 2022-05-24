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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * For {@link GroupPageStoreHolder} testing.
 */
public class GroupPageStoreHolderTest {
    @Test
    void testSize() {
        assertEquals(11, new GroupPageStoreHolder<>(createPageStore(), createPageStores(10)).size());
        assertEquals(21, new GroupPageStoreHolder<>(createPageStore(), createPageStores(20)).size());
        assertEquals(2, new GroupPageStoreHolder<>(createPageStore(), createPageStores(1)).size());
    }

    @Test
    void testGetByIndex() {
        PageStore idxPageStore = createPageStore();

        PageStore[] partitionPageStores = createPageStores(2);

        GroupPageStoreHolder<PageStore> holder = new GroupPageStoreHolder<>(idxPageStore, partitionPageStores);

        assertSame(partitionPageStores[0], holder.get(0));
        assertSame(partitionPageStores[1], holder.get(1));
        assertSame(idxPageStore, holder.get(2));

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> holder.get(4));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> holder.get(-1));
    }

    private PageStore[] createPageStores(int count) {
        return IntStream.range(0, count).mapToObj(i -> createPageStore()).toArray(PageStore[]::new);
    }

    private PageStore createPageStore() {
        return mock(PageStore.class);
    }
}
