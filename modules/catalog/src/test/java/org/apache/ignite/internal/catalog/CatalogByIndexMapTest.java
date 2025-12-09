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

package org.apache.ignite.internal.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CatalogByIndexMap}.
 */
class CatalogByIndexMapTest extends BaseIgniteAbstractTest {
    private CatalogByIndexMap map;
    private final Catalog catalog1 = mock(Catalog.class);
    private final Catalog catalog2 = mock(Catalog.class);

    @BeforeEach
    void setUp() {
        map = new CatalogByIndexMap();
    }

    @Test
    void testPutAndGet() {
        map = map.appendOrUpdate(1, catalog1);
        map = map.appendOrUpdate(2, catalog2);

        assertNull(map.get(0));
        assertSame(catalog1, map.get(1));
        assertSame(catalog2, map.get(2));
        assertNull(map.get(3));
    }

    @Test
    void testFirstLast() {
        map = map.appendOrUpdate(1, catalog1);
        map = map.appendOrUpdate(2, catalog2);

        assertEquals(1, map.firstKey());
        assertEquals(2, map.lastKey());

        assertSame(catalog1, map.firstValue());
        assertSame(catalog2, map.lastValue());
    }

    @Test
    void testFloorValue() {
        map = map.appendOrUpdate(1, catalog1);
        map = map.appendOrUpdate(3, catalog2);

        assertNull(map.floorValue(0));
        assertSame(catalog1, map.floorValue(1));
        assertSame(catalog1, map.floorValue(2));
        assertSame(catalog2, map.floorValue(3));
        assertSame(catalog2, map.floorValue(4));
    }

    @Test
    void testOverwrite() {
        map = map.appendOrUpdate(1, catalog1);
        map = map.appendOrUpdate(1, catalog2);

        assertSame(catalog2, map.get(1));
    }

    @Test
    void testGetNonExistent() {
        assertNull(map.get(42));
    }

    @Test
    void testClearEmptyHead() {
        map = map.appendOrUpdate(1, catalog1);

        map = map.clearHead(1);

        assertSame(catalog1, map.get(1));
    }

    @Test
    void testClearNonEmptyHead() {
        map = map.appendOrUpdate(1, catalog1);
        map = map.appendOrUpdate(2, catalog2);

        map = map.clearHead(2);

        assertNull(map.get(1));
        assertSame(catalog2, map.get(2));
    }
}
