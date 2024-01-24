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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.cache.IgniteCache;
import org.apache.ignite.internal.client.table.ClientTable;
import org.junit.jupiter.api.Test;

/**
 * Client cache tests.
 */
public class ClientCacheTest extends AbstractClientTableTest {
    @Test
    public void testBasic() {
        ClientTable table = (ClientTable) cache();

        try (IgniteCache<Integer, Integer> view = table.cache(null, null, null, null, null)) {
            view.put(1, 1);

            assertEquals(1, view.get(1));

            assertTrue(view.remove(1));

            assertNull(view.get(1));

            view.put(1, 2);

            assertEquals(2, view.get(1));

            assertTrue(view.remove(1));

            assertNull(view.get(1));
        }
    }

    @Test
    public void testBasicTypeUnsafe() {
        ClientTable table = (ClientTable) cache();

        try (IgniteCache<Object, Object> view = table.cache(null, null, null, null, null)) {
            view.put(1, 1);

            assertEquals(1, view.get(1));

            assertTrue(view.remove(1));

            assertNull(view.get(1));

            view.put(2L, "test");

            assertEquals("test", view.get(2L));

            assertTrue(view.remove(2L));

            assertNull(view.get(2L));
        }
    }
}
