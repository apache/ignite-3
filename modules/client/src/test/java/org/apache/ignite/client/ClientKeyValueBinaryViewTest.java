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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Binary KeyValueView tests.
 */
public class ClientKeyValueBinaryViewTest extends AbstractClientTableTest {
    @Test
    public void testGetMissingRowReturnsNull() {
        Table table = defaultTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        assertNull(kvView.get(defaultTupleKey(), null));
    }

    @Test
    public void testRecordUpsertKvGet() {
        Table table = defaultTable();
        table.recordView().upsert(tuple(), null);

        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = defaultTupleKey();
        Tuple val = kvView.get(key, null);

        assertEquals(DEFAULT_NAME, val.value("name"));
        assertEquals(DEFAULT_NAME, val.value(0));
        assertEquals(1, val.columnCount());
    }

    @Test
    public void testKvPutRecordGet() {
        Table table = defaultTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", "bar");

        kvView.put(key, val, null);
        Tuple res = table.recordView().get(key, null);

        assertEquals("bar", res.stringValue("name"));
        assertEquals(DEFAULT_ID, res.longValue("id"));
    }

    @Test
    public void testPutGet() {
        Table table = defaultTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", DEFAULT_NAME);

        kvView.put(key, val, null);
        Tuple resVal = kvView.get(key, null);

        assertTupleEquals(val, resVal);
    }

    @Test
    public void testGetUpdatePut() {
        Table table = defaultTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", DEFAULT_NAME);
        kvView.put(key, val, null);

        Tuple resVal = kvView.get(key, null);
        resVal.set("name", "123");
        kvView.put(key, resVal, null);

        Tuple resVal2 = kvView.get(key, null);

        assertTupleEquals(resVal, resVal2);
    }

    @Test
    public void testGetAll() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        kvView.put(tupleKey(1L), tupleVal("1"), null);
        kvView.put(tupleKey(2L), tupleVal("2"), null);
        kvView.put(tupleKey(3L), tupleVal("3"), null);

        Map<Tuple, Tuple> res = kvView.getAll(List.of(tupleKey(1L), tupleKey(3L)), null);

        assertEquals(2, res.size());

        var keys = sortedTuples(res.keySet());

        assertEquals(1L, keys[0].longValue(0));
        assertEquals(3L, keys[1].longValue(0));

        assertEquals("1", res.get(keys[0]).stringValue("name"));
        assertEquals("3", res.get(keys[1]).stringValue(0));
    }

    @Test
    public void testGetAllEmptyKeysReturnsEmptyMap() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        Map<Tuple, Tuple> res = kvView.getAll(List.of(), null);
        assertEquals(0, res.size());
    }

    @Test
    public void testGetAllNonExistentKeysReturnsEmptyMap() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        Map<Tuple, Tuple> res = kvView.getAll(List.of(tupleKey(-1L)), null);
        assertEquals(0, res.size());
    }

    @Test
    public void testContains() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        assertTrue(kvView.contains(tupleKey(1L), null));
        assertFalse(kvView.contains(tupleKey(2L), null));
    }

    @Test
    public void testContainsThrowsOnEmptyKey() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        var ex = assertThrows(CompletionException.class, () -> kvView.contains(Tuple.create(), null));
        assertTrue(ex.getMessage().contains("Missed key column: id"), ex.getMessage());
    }

    @Test
    public void testPutAll() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.putAll(Map.of(tupleKey(1L), tupleVal("1"), tupleKey(2L), tupleVal("2")), null);

        assertEquals("1", kvView.get(tupleKey(1L), null).stringValue("name"));
        assertEquals("2", kvView.get(tupleKey(2L), null).stringValue(0));
    }

    @Test
    public void testPutIfAbsent() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        assertTrue(kvView.putIfAbsent(tupleKey(1L), tupleVal("1"), null));
        assertFalse(kvView.putIfAbsent(tupleKey(1L), tupleVal("1"), null));
        assertFalse(kvView.putIfAbsent(tupleKey(1L), tupleVal("2"), null));
        assertTrue(kvView.putIfAbsent(tupleKey(2L), tupleVal("1"), null));
    }

    @Test
    public void testRemove() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        assertFalse(kvView.remove(tupleKey(2L), null));
        assertTrue(kvView.remove(tupleKey(1L), null));
        assertFalse(kvView.remove(tupleKey(1L), null));
        assertFalse(kvView.contains(tupleKey(1L), null));
    }

    @Test
    public void testRemoveExact() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        assertFalse(kvView.remove(tupleKey(1L), tupleVal("2"), null));
        assertFalse(kvView.remove(tupleKey(2L), tupleVal("1"), null));
        assertTrue(kvView.contains(tupleKey(1L), null));

        assertTrue(kvView.remove(tupleKey(1L), tupleVal("1"), null));
        assertFalse(kvView.contains(tupleKey(1L), null));
    }

    @Test
    public void testRemoveAll() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.putAll(Map.of(tupleKey(1L), tupleVal("1"), tupleKey(2L), tupleVal("2")), null);

        Collection<Tuple> res = kvView.removeAll(List.of(tupleKey(2L), tupleKey(3L)), null);

        assertTrue(kvView.contains(tupleKey(1L), null));
        assertFalse(kvView.contains(tupleKey(2L), null));

        assertEquals(1, res.size());
        assertEquals(3L, res.iterator().next().longValue(0));
    }

    @Test
    public void testReplace() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        assertFalse(kvView.replace(tupleKey(3L), tupleVal("3"), null));
        assertTrue(kvView.replace(tupleKey(1L), tupleVal("2"), null));
        assertEquals("2", kvView.get(tupleKey(1L), null).stringValue(0));
    }

    @Test
    public void testReplaceExact() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        assertFalse(kvView.replace(tupleKey(1L), tupleVal("2"), tupleVal("3"), null));
        assertTrue(kvView.replace(tupleKey(1L), tupleVal("1"), tupleVal("3"), null));
        assertEquals("3", kvView.get(tupleKey(1L), null).stringValue(0));
    }

    @Test
    public void testGetAndReplace() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        assertNull(kvView.getAndReplace(tupleKey(2L), tupleVal("2"), null));

        Tuple res = kvView.getAndReplace(tupleKey(1L), tupleVal("2"), null);
        assertEquals("1", res.stringValue(0));
        assertEquals("2", kvView.get(tupleKey(1L), null).stringValue(0));
    }

    @Test
    public void testGetAndRemove() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        Tuple removed = kvView.getAndRemove(tupleKey(1L), null);

        assertNotNull(removed);
        assertEquals(1, removed.columnCount());
        assertEquals("1", removed.stringValue(0));
        assertEquals("1", removed.stringValue("name"));

        assertFalse(kvView.contains(tupleKey(1L), null));
        assertNull(kvView.getAndRemove(tupleKey(1L), null));
    }

    @Test
    public void testGetAndPut() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(tupleKey(1L), tupleVal("1"), null);

        Tuple res1 = kvView.getAndPut(tupleKey(2L), tupleVal("2"), null);
        Tuple res2 = kvView.getAndPut(tupleKey(1L), tupleVal("3"), null);

        assertNull(res1);
        assertEquals("2", kvView.get(tupleKey(2L), null).stringValue(0));

        assertNotNull(res2);
        assertEquals("1", res2.stringValue(0));
        assertEquals("3", kvView.get(tupleKey(1L), null).stringValue(0));
    }
}
