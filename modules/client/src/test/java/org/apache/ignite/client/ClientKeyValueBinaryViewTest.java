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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Binary KeyValueView tests.
 */
public class ClientKeyValueBinaryViewTest extends AbstractClientTableTest {
    @Test
    public void testGetMissingRowReturnsNull() {
        Table table = defaultTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        assertNull(kvView.get(null, defaultTupleKey()));
    }

    @Test
    public void testRecordUpsertKvGet() {
        Table table = defaultTable();
        table.recordView().upsert(null, tuple());

        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = defaultTupleKey();
        Tuple val = kvView.get(null, key);

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

        kvView.put(null, key, val);
        Tuple res = table.recordView().get(null, key);

        assertEquals("bar", res.stringValue("name"));
        assertEquals(DEFAULT_ID, res.longValue("id"));
    }

    @Test
    public void testPutGet() {
        Table table = defaultTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", DEFAULT_NAME);

        kvView.put(null, key, val);
        Tuple resVal = kvView.get(null, key);

        assertNotNull(resVal);
        assertTupleEquals(val, resVal);

        // Key columns should not be available in the value.
        assertThrows(IllegalArgumentException.class, () -> resVal.longValue("id"));
    }

    @Test
    public void testPutNullAsKeyIsForbidden() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        NullPointerException ex = assertThrows(NullPointerException.class, () -> kvView.put(null, null, Tuple.create()));
        assertThat(ex.getMessage(), containsString("key"));
    }

    @Test
    public void testPutNullAsValueIsForbidden() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        Tuple key = defaultTupleKey();

        NullPointerException ex = assertThrows(NullPointerException.class, () -> kvView.put(null, key, null));
        assertThat(ex.getMessage(), equalTo("val"));
    }

    @Test
    public void testPutEmptyTuple() {
        Table table = defaultTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = defaultTupleKey();

        kvView.put(null, key, Tuple.create());
        Tuple resVal = kvView.get(null, key);

        assertNull(resVal.stringValue("name"));
    }

    @Test
    public void testGetUpdatePut() {
        Table table = defaultTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", DEFAULT_NAME);
        kvView.put(null, key, val);

        Tuple resVal = kvView.get(null, key);
        resVal.set("name", "123");
        kvView.put(null, key, resVal);

        Tuple resVal2 = kvView.get(null, key);

        assertTupleEquals(resVal, resVal2);
    }

    @Test
    public void testGetAll() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        kvView.put(null, tupleKey(1L), tupleVal("1"));
        kvView.put(null, tupleKey(2L), tupleVal("2"));
        kvView.put(null, tupleKey(3L), tupleVal("3"));

        Map<Tuple, Tuple> res = kvView.getAll(null, List.of(tupleKey(1L), tupleKey(3L)));

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
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        Map<Tuple, Tuple> res = kvView.getAll(null, List.of());
        assertEquals(0, res.size());
    }

    @Test
    public void testGetAllNonExistentKeysReturnsEmptyMap() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        Map<Tuple, Tuple> res = kvView.getAll(null, List.of(tupleKey(-1L)));
        assertEquals(0, res.size());
    }

    @Test
    public void testContains() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        assertTrue(kvView.contains(null, tupleKey(1L)));
        assertFalse(kvView.contains(null, tupleKey(2L)));
    }

    @Test
    public void testContainsThrowsOnEmptyKey() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        var ex = assertThrows(IgniteException.class, () -> kvView.contains(null, Tuple.create()));
        assertTrue(ex.getMessage().contains("Missed key column: ID"), ex.getMessage());
        assertThat(Arrays.asList(ex.getStackTrace()), anyOf(hasToString(containsString("ClientKeyValueBinaryView"))));
    }

    @Test
    public void testContainsAll() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        Tuple firstKeyTuple = tupleKey(101L);
        Tuple secondKeyTuple = tupleKey(102L);
        Tuple thirdKeyTuple = tupleKey(103L);

        Map<Tuple, Tuple> kvs = Map.of(
                firstKeyTuple, tupleVal("201"),
                secondKeyTuple, tupleVal("202"),
                thirdKeyTuple, tupleVal("203")
        );

        kvView.putAll(null, kvs);

        assertThrows(NullPointerException.class, () -> kvView.containsAll(null, null));
        assertThrows(NullPointerException.class, () -> kvView.containsAll(null, List.of(firstKeyTuple, null, thirdKeyTuple)));

        assertTrue(kvView.containsAll(null, List.of()));
        assertTrue(kvView.containsAll(null, List.of(firstKeyTuple)));
        assertTrue(kvView.containsAll(null, List.of(firstKeyTuple, secondKeyTuple, thirdKeyTuple)));

        Tuple zeroKeyTuple = tupleKey(0L);
        assertFalse(kvView.containsAll(null, List.of(zeroKeyTuple)));
        assertFalse(kvView.containsAll(null, List.of(firstKeyTuple, secondKeyTuple, zeroKeyTuple)));
    }

    @Test
    public void testPutAll() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.putAll(null, Map.of(tupleKey(1L), tupleVal("1"), tupleKey(2L), tupleVal("2")));

        assertEquals("1", kvView.get(null, tupleKey(1L)).stringValue("name"));
        assertEquals("2", kvView.get(null, tupleKey(2L)).stringValue(0));
    }

    @Test
    public void testPutIfAbsent() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        assertTrue(kvView.putIfAbsent(null, tupleKey(1L), tupleVal("1")));
        assertFalse(kvView.putIfAbsent(null, tupleKey(1L), tupleVal("1")));
        assertFalse(kvView.putIfAbsent(null, tupleKey(1L), tupleVal("2")));
        assertTrue(kvView.putIfAbsent(null, tupleKey(2L), tupleVal("1")));
    }

    @Test
    public void testRemove() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        assertFalse(kvView.remove(null, tupleKey(2L)));
        assertTrue(kvView.remove(null, tupleKey(1L)));
        assertFalse(kvView.remove(null, tupleKey(1L)));
        assertFalse(kvView.contains(null, tupleKey(1L)));
    }

    @Test
    public void testRemoveExact() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        assertFalse(kvView.remove(null, tupleKey(1L), tupleVal("2")));
        assertFalse(kvView.remove(null, tupleKey(2L), tupleVal("1")));
        assertTrue(kvView.contains(null, tupleKey(1L)));

        assertTrue(kvView.remove(null, tupleKey(1L), tupleVal("1")));
        assertFalse(kvView.contains(null, tupleKey(1L)));
    }

    @Test
    public void testRemoveAll() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.putAll(null, Map.of(tupleKey(1L), tupleVal("1"), tupleKey(2L), tupleVal("2")));

        Collection<Tuple> res = kvView.removeAll(null, List.of(tupleKey(2L), tupleKey(3L)));

        assertTrue(kvView.contains(null, tupleKey(1L)));
        assertFalse(kvView.contains(null, tupleKey(2L)));

        assertEquals(1, res.size());
        assertEquals(3L, res.iterator().next().longValue(0));
    }

    @Test
    public void testReplace() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        assertFalse(kvView.replace((Transaction) null, tupleKey(3L), tupleVal("3")));
        assertTrue(kvView.replace((Transaction) null, tupleKey(1L), tupleVal("2")));
        assertEquals("2", kvView.get(null, tupleKey(1L)).stringValue(0));
    }

    @Test
    public void testReplaceExact() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        assertFalse(kvView.replace(null, tupleKey(1L), tupleVal("2"), tupleVal("3")));
        assertTrue(kvView.replace(null, tupleKey(1L), tupleVal("1"), tupleVal("3")));
        assertEquals("3", kvView.get(null, tupleKey(1L)).stringValue(0));
    }

    @Test
    public void testGetAndReplace() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        assertNull(kvView.getAndReplace(null, tupleKey(2L), tupleVal("2")));

        Tuple res = kvView.getAndReplace(null, tupleKey(1L), tupleVal("2"));
        assertEquals("1", res.stringValue(0));
        assertEquals("2", kvView.get(null, tupleKey(1L)).stringValue(0));
    }

    @Test
    public void testGetAndRemove() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        Tuple removed = kvView.getAndRemove(null, tupleKey(1L));

        assertNotNull(removed);
        assertEquals(1, removed.columnCount());
        assertEquals("1", removed.stringValue(0));
        assertEquals("1", removed.stringValue("name"));

        assertFalse(kvView.contains(null, tupleKey(1L)));
        assertNull(kvView.getAndRemove(null, tupleKey(1L)));
    }

    @Test
    public void testGetAndPut() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();
        kvView.put(null, tupleKey(1L), tupleVal("1"));

        Tuple res1 = kvView.getAndPut(null, tupleKey(2L), tupleVal("2"));
        Tuple res2 = kvView.getAndPut(null, tupleKey(1L), tupleVal("3"));

        assertNull(res1);
        assertEquals("2", kvView.get(null, tupleKey(2L)).stringValue(0));

        assertNotNull(res2);
        assertEquals("1", res2.stringValue(0));
        assertEquals("3", kvView.get(null, tupleKey(1L)).stringValue(0));
    }

    @Test
    public void testGetNullable() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        Tuple existingKey = tupleKey(DEFAULT_ID);
        Tuple nonExistingKey = tupleKey(-1L);

        kvView.put(null, existingKey, Tuple.create());
        kvView.remove((Transaction) null, nonExistingKey);

        NullableValue<Tuple> emptyTuple = kvView.getNullable(null, existingKey);
        NullableValue<Tuple> missingVal = kvView.getNullable(null, nonExistingKey);

        assertNull(missingVal);

        assertNotNull(emptyTuple);
        assertNotNull(emptyTuple.get());
        assertEquals(1, emptyTuple.get().columnCount());
        assertNull(emptyTuple.get().value(0));
        assertNull(emptyTuple.get().stringValue("name"));
    }

    @Test
    public void testGetNullableAndPut() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        Tuple existingKey = tupleKey(DEFAULT_ID);
        Tuple nonExistingKey = tupleKey(-1L);

        kvView.put(null, existingKey, Tuple.create());
        kvView.remove((Transaction) null, nonExistingKey);

        NullableValue<Tuple> emptyTuple = kvView.getNullableAndPut(null, existingKey, tupleVal(DEFAULT_NAME));
        NullableValue<Tuple> missingVal = kvView.getNullableAndPut(null, nonExistingKey, tupleVal(DEFAULT_NAME));

        assertNull(missingVal);

        assertNotNull(emptyTuple);
        assertNotNull(emptyTuple.get());
        assertEquals(1, emptyTuple.get().columnCount());
        assertNull(emptyTuple.get().value(0));
        assertNull(emptyTuple.get().stringValue("name"));

        Tuple val = kvView.get(null, existingKey);
        assertEquals(DEFAULT_NAME, val.stringValue("name"));
    }

    @Test
    public void testGetNullableAndRemove() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        Tuple existingKey = tupleKey(DEFAULT_ID);
        Tuple nonExistingKey = tupleKey(-1L);

        kvView.put(null, existingKey, Tuple.create());
        kvView.remove((Transaction) null, nonExistingKey);

        NullableValue<Tuple> emptyTuple = kvView.getNullableAndRemove(null, existingKey);
        NullableValue<Tuple> missingVal = kvView.getNullableAndRemove(null, nonExistingKey);

        assertNull(missingVal);

        assertNotNull(emptyTuple);
        assertNotNull(emptyTuple.get());
        assertEquals(1, emptyTuple.get().columnCount());
        assertNull(emptyTuple.get().value(0));
        assertNull(emptyTuple.get().stringValue("name"));

        assertNull(kvView.get(null, existingKey));
    }

    @Test
    public void testGetNullableAndReplace() {
        KeyValueView<Tuple, Tuple> kvView = defaultTable().keyValueView();

        Tuple existingKey = tupleKey(DEFAULT_ID);
        Tuple nonExistingKey = tupleKey(-1L);

        kvView.put(null, existingKey, Tuple.create());
        kvView.remove((Transaction) null, nonExistingKey);

        NullableValue<Tuple> emptyTuple = kvView.getNullableAndReplace(null, existingKey, tupleVal(DEFAULT_NAME));
        NullableValue<Tuple> missingVal = kvView.getNullableAndReplace(null, nonExistingKey, tupleVal(DEFAULT_NAME));

        assertNull(missingVal);

        assertNotNull(emptyTuple);
        assertNotNull(emptyTuple.get());
        assertEquals(1, emptyTuple.get().columnCount());
        assertNull(emptyTuple.get().value(0));
        assertNull(emptyTuple.get().stringValue("name"));

        Tuple val = kvView.get(null, existingKey);
        assertEquals(DEFAULT_NAME, val.stringValue("name"));
    }
}
