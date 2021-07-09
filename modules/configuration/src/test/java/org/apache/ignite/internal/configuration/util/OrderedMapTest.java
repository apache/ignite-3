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

package org.apache.ignite.internal.configuration.util;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class OrderedMapTest {

    private OrderedMap<String> map = new OrderedMap<>();

    @Test
    public void putGetRemove() {
        assertNull(map.get("key1"));

        map.put("key", "value");

        assertEquals(1, map.size());
        assertEquals("value", map.get("key"));

        map.remove("key");

        assertNull(map.get("key1"));
    }

    @Test
    public void keysOrder() {
        map.put("key1", "value1");
        map.put("key2", "value2");

        assertEquals(2, map.size());
        assertEquals(Set.of("key1", "key2"), map.keySet());

        map.clear();

        assertEquals(0, map.size());
        assertEquals(Set.of(), map.keySet());

        map.put("key2", "value2");
        map.put("key1", "value1");

        assertEquals(2, map.size());
        assertEquals(Set.of("key2", "key1"), map.keySet());

        map.put("key2", "value3");
        assertEquals(Set.of("key2", "key1"), map.keySet());
    }

    @Test
    public void putByIndex() {
        map.putByIndex(0, "key1", "value1");

        map.putByIndex(0, "key2", "value2");

        assertEquals(Set.of("key2", "key1"), map.keySet());

        map.putByIndex(1, "key3", "value3");

        assertEquals(Set.of("key2", "key3", "key1"), map.keySet());

        map.putByIndex(3, "key4", "value4");

        assertEquals(Set.of("key2", "key3", "key1", "key4"), map.keySet());

        map.putByIndex(5, "key5", "value5");

        assertEquals(Set.of("key2", "key3", "key1", "key4", "key5"), map.keySet());
    }

    @Test
    public void putAfter() {
        map.putAfter("foo", "key1", "value1");

        assertEquals(Set.of("key1"), map.keySet());

        map.putAfter("key1", "key2", "value2");

        assertEquals(Set.of("key1", "key2"), map.keySet());

        map.putAfter("key1", "key3", "value3");

        assertEquals(Set.of("key1", "key3", "key2"), map.keySet());

        map.putAfter("key2", "key4", "value4");

        assertEquals(Set.of("key1", "key3", "key2", "key4"), map.keySet());

        map.putAfter("foo", "key5", "value5");

        assertEquals(Set.of("key1", "key3", "key2", "key4", "key5"), map.keySet());
    }

    @Test
    public void rename() {
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        map.rename("key2", "key4");

        assertEquals("value2", map.get("key4"));

        assertEquals(Set.of("key1", "key4", "key3"), map.keySet());
    }

    @Test
    public void reorderKeys() {
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        assertEquals(Set.of("key1", "key2", "key3"), map.keySet());

        map.reorderKeys(List.of("key2", "key1", "key3"));

        assertEquals(Set.of("key2", "key1", "key3"), map.keySet());

        map.reorderKeys(List.of("key2", "key3", "key1"));

        assertEquals(Set.of("key2", "key3", "key1"), map.keySet());

        map.reorderKeys(List.of("key1", "key3", "key2"));

        assertEquals(Set.of("key1", "key3", "key2"), map.keySet());
    }
}