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

import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;

/**
 * Client cache tests.
 */
public class ClientCacheTest extends AbstractClientTableTest {
    @Test
    public void testBasic() {
        ClientTable table = (ClientTable) cache();

        KeyValueView<Integer, Integer> view = table.cache();

        view.put(null, 1, 1);

        assertEquals(1, view.get(null, 1));

        assertTrue(view.remove(null, 1));

        assertNull(view.get(null, 1));

        view.put(null, 1, 2);

        assertEquals(2, view.get(null, 1));

        assertTrue(view.remove(null, 1));

        assertNull(view.get(null, 1));
    }

    @Test
    public void testBasicTypeUnsafe() {
        ClientTable table = (ClientTable) cache();

        KeyValueView<Object, Object> view = table.cache();

        view.put(null, 1, 1);

        assertEquals(1, view.get(null, 1));

        assertTrue(view.remove(null, 1));

        assertNull(view.get(null, 1));

        view.put(null, 2L, "test");

        assertEquals("test", view.get(null, 2L));

        assertTrue(view.remove(null, 2L));

        assertNull(view.get(null, 2L));
    }
}
