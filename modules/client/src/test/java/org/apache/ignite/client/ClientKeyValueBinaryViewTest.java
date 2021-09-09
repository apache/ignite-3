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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * KeyValueBinaryView tests.
 */
public class ClientKeyValueBinaryViewTest extends AbstractClientTableTest {
    @Test
    public void testGetMissingRowReturnsNull() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        assertNull(kvView.get(defaultTupleKey()));
    }

    @Test
    public void testTableUpsertKvGet() {
        Table table = defaultTable();
        table.upsert(tuple());

        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey();
        Tuple val = kvView.get(key);

        assertEquals(DEFAULT_NAME, val.value("name"));
        assertEquals(DEFAULT_NAME, val.value(0));
        assertEquals(1, val.columnCount());
    }

    @Test
    public void testKvPutTableGet() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", "bar");

        kvView.put(key, val);
        Tuple res = table.get(key);

        assertEquals("bar", res.stringValue("name"));
        assertEquals(DEFAULT_ID, res.longValue("id"));
    }

    @Test
    public void testPutGet() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", DEFAULT_NAME);

        kvView.put(key, val);
        Tuple resVal = kvView.get(key);

        assertTupleEquals(val, resVal);
    }

    @Test
    public void testGetUpdatePut() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey();
        Tuple val = Tuple.create().set("name", DEFAULT_NAME);
        kvView.put(key, val);

        Tuple resVal = kvView.get(key);
        resVal.set("name", "123");
        kvView.put(key, resVal);

        Tuple resVal2 = kvView.get(key);

        assertTupleEquals(resVal, resVal2);
    }

    @Test
    public void testGetAll() {
        KeyValueBinaryView kvView = defaultTable().kvView();

        kvView.put(tupleKey(1L), tupleVal("1"));
        kvView.put(tupleKey(2L), tupleVal("2"));
        kvView.put(tupleKey(3L), tupleVal("3"));

        Map<Tuple, Tuple> res = kvView.getAll(List.of(tupleKey(1L), tupleKey(3L)));

        assertEquals(2, res.size());

        var keys = sortedTuples(res.keySet());

        assertEquals(1L, keys[0].longValue(0));
        assertEquals(3L, keys[1].longValue(0));

        assertEquals("1", res.get(keys[0]).stringValue("name"));
        assertEquals("3", res.get(keys[1]).stringValue("name"));
    }
}
