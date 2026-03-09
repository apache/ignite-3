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

package org.apache.ignite.internal.runner.app.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests custom key column order table operations with thin client.
 */
@SuppressWarnings("resource")
public class ItCustomKeyColumnOrderClientTest extends ItAbstractThinClientTest {
    private static final String TABLE_NAME2 = "TBL2";

    @BeforeAll
    void createTable() {
        String query = "CREATE TABLE " + TABLE_NAME2
                + " (val1 VARCHAR, key1 INT, val2 BIGINT, key2 VARCHAR, PRIMARY KEY (key1, key2)) "
                + "colocate by (key2, key1)";

        client().sql().execute(query);
    }

    @BeforeEach
    void clearTable() {
        client().sql().execute("DELETE FROM " + TABLE_NAME2);
    }

    protected Ignite ignite() {
        return client();
    }

    private Table table() {
        return ignite().tables().table(TABLE_NAME2);
    }

    @Test
    void testRecordBinaryView() {
        var recView = table().recordView();

        Tuple key = Tuple.create().set("key1", 1).set("key2", "key2");
        Tuple key2 = Tuple.create().set("key1", 2).set("key2", "key3");

        Tuple val = Tuple.create().set("key1", 1).set("key2", "key2").set("val1", "val1").set("val2", 2L);
        Tuple val2 = Tuple.create().set("key1", 1).set("key2", "key2").set("val1", "val2").set("val2", 3L);

        // put/get.
        recView.insert(null, val);

        Tuple res = recView.get(null, key);
        assertEquals(val, res);

        // getAndPut.
        Tuple putRes = recView.getAndUpsert(null, val2);
        assertEquals(val, putRes);

        // contains.
        assertTrue(recView.contains(null, key));

        // getAndRemove.
        Tuple removeRes = recView.getAndDelete(null, key);
        assertEquals(val2, removeRes);

        // putIfAbsent.
        assertTrue(recView.insert(null, val));

        // remove exact.
        assertTrue(recView.deleteExact(null, val));

        // putAll/getAll.
        recView.insertAll(null, List.of(val));

        List<Tuple> resMap = recView.getAll(null, Collections.singletonList(key));
        assertEquals(1, resMap.size());
        Tuple resEntry = resMap.get(0);
        assertEquals(val, resEntry);

        // Query mapper.
        try (Cursor<Tuple> cursor = recView.query(null, null)) {
            Tuple curEntry = cursor.next();
            assertEquals(val, curEntry);
        }

        // removeAll.
        Collection<Tuple> removeAllRes = recView.deleteAll(null, List.of(key, key2));
        assertEquals(1, removeAllRes.size());
        assertEquals(key2, removeAllRes.iterator().next());

        // replace.
        recView.insert(null, val);
        assertTrue(recView.replace(null, val2));

        // getAndReplace.
        Tuple getAndReplaceRes = recView.getAndReplace(null, val);
        assertNotNull(getAndReplaceRes);
        assertEquals(val2, getAndReplaceRes);
    }

    @Test
    void testRecordView() {
        var recView = table().recordView(Pojo.class);

        Pojo key = new Pojo(1, "key2");
        Pojo key2 = new Pojo(2, "key3");

        Pojo val = new Pojo(1, "key2", "val1", 2L);
        Pojo val2 = new Pojo(1, "key2", "val2", 3L);

        // put/get.
        recView.insert(null, val);

        Pojo res = recView.get(null, key);
        assertEquals(val.key1, res.key1);
        assertEquals(val.key2, res.key2);
        assertEquals(val.val1, res.val1);
        assertEquals(val.val2, res.val2);

        // getAndPut.
        Pojo putRes = recView.getAndUpsert(null, val2);
        assertEquals(val.key1, putRes.key1);

        // contains.
        assertTrue(recView.contains(null, key));

        // getAndRemove.
        Pojo removeRes = recView.getAndDelete(null, key);
        assertEquals(val2.key1, removeRes.key1);
        assertEquals(val2.key2, removeRes.key2);
        assertEquals(val2.val1, removeRes.val1);
        assertEquals(val2.val2, removeRes.val2);

        // putIfAbsent.
        assertTrue(recView.insert(null, val));

        // remove exact.
        assertTrue(recView.deleteExact(null, val));

        // putAll/getAll.
        recView.insertAll(null, List.of(val));

        List<Pojo> resMap = recView.getAll(null, Collections.singletonList(key));
        assertEquals(1, resMap.size());
        Pojo resEntry = resMap.get(0);
        assertEquals(val.key1, resEntry.key1);
        assertEquals(val.key2, resEntry.key2);
        assertEquals(val.val1, resEntry.val1);
        assertEquals(val.val2, resEntry.val2);

        // Query mapper.
        try (Cursor<Pojo> cursor = recView.query(null, null)) {
            Pojo curEntry = cursor.next();
            assertEquals(val.key1, curEntry.key1);
            assertEquals(val.key2, curEntry.key2);
            assertEquals(val.val1, curEntry.val1);
            assertEquals(val.val2, curEntry.val2);
        }

        // removeAll.
        Collection<Pojo> removeAllRes = recView.deleteAll(null, List.of(key, key2));
        assertEquals(1, removeAllRes.size());
        assertEquals(key2.key1, removeAllRes.iterator().next().key1);

        // replace.
        recView.insert(null, val);
        assertTrue(recView.replace(null, val2));

        // getAndReplace.
        Pojo getAndReplaceRes = recView.getAndReplace(null, val);
        assertNotNull(getAndReplaceRes);
        assertEquals(val.key1, getAndReplaceRes.key1);
        assertEquals(val.key2, getAndReplaceRes.key2);
    }

    @Test
    void testKeyValueBinaryView() {
        var kvView = table().keyValueView();

        // put/get.
        Tuple key = Tuple.create().set("key1", 1).set("key2", "key2");
        Tuple key2 = Tuple.create().set("key1", 2).set("key2", "key3");

        Tuple val = Tuple.create().set("val1", "val1").set("val2", 2L);
        Tuple val2 = Tuple.create().set("val1", "val2").set("val2", 3L);

        kvView.put(null, key, val);

        Tuple res = kvView.get(null, key);
        assertEquals(val, res);

        // getAndPut.
        Tuple putRes = kvView.getAndPut(null, key, val2);
        assertEquals(val, putRes);

        // contains.
        assertTrue(kvView.contains(null, key));

        // getAndRemove.
        Tuple removeRes = kvView.getAndRemove(null, key);
        assertEquals(val2, removeRes);

        // putIfAbsent.
        assertTrue(kvView.putIfAbsent(null, key, val));

        // remove exact.
        assertTrue(kvView.remove(null, key, val));

        // putAll/getAll.
        kvView.putAll(null, Map.of(key, val));

        Map<Tuple, Tuple> resMap = kvView.getAll(null, Collections.singletonList(key));
        assertEquals(1, resMap.size());
        Entry<Tuple, Tuple> resEntry = resMap.entrySet().iterator().next();
        assertEquals(key, resEntry.getKey());
        assertEquals(val, resEntry.getValue());

        // Query mapper.
        try (Cursor<Entry<Tuple, Tuple>> cursor = kvView.query(null, null)) {
            Entry<Tuple, Tuple> curEntry = cursor.next();
            assertEquals(key, curEntry.getKey());
            assertEquals(val, curEntry.getValue());
        }

        // removeAll.
        Collection<Tuple> removeAllRes = kvView.removeAll(null, List.of(key, key2));
        assertEquals(1, removeAllRes.size());
        assertEquals(key2, removeAllRes.iterator().next());

        // replace.
        kvView.put(null, key, val);
        assertTrue(kvView.replace(null, key, val2));
        assertTrue(kvView.replace(null, key, val2, val));

        // getAndReplace.
        Tuple getAndReplaceRes = kvView.getAndReplace(null, key, val2);
        assertNotNull(getAndReplaceRes);
        assertEquals(val, getAndReplaceRes);
    }

    @Test
    void testKeyValueView() {
        var kvView = table().keyValueView(PojoKey.class, PojoVal.class);

        // put/get.
        PojoKey key = new PojoKey(1, "key2");
        PojoKey key2 = new PojoKey(2, "key3");

        PojoVal val = new PojoVal("val1", 2L);
        PojoVal val2 = new PojoVal("val2", 3L);
        kvView.put(null, key, val);

        PojoVal res = kvView.get(null, key);
        assertEquals(val.val1, res.val1);
        assertEquals(val.val2, res.val2);

        // getAndPut.
        PojoVal putRes = kvView.getAndPut(null, key, val2);
        assertEquals(val.val1, putRes.val1);
        assertEquals(val.val2, putRes.val2);

        // contains.
        assertTrue(kvView.contains(null, key));

        // getAndRemove.
        PojoVal removeRes = kvView.getAndRemove(null, key);
        assertEquals(val2.val1, removeRes.val1);
        assertEquals(val2.val2, removeRes.val2);

        // putIfAbsent.
        assertTrue(kvView.putIfAbsent(null, key, val));

        // remove exact.
        assertTrue(kvView.remove(null, key, val));

        // putAll/getAll.
        kvView.putAll(null, Map.of(key, val));

        Map<PojoKey, PojoVal> resMap = kvView.getAll(null, Collections.singletonList(key));
        assertEquals(1, resMap.size());
        Entry<PojoKey, PojoVal> resEntry = resMap.entrySet().iterator().next();
        assertEquals(key.key1, resEntry.getKey().key1);
        assertEquals(val.val1, resEntry.getValue().val1);
        assertEquals(val.val2, resEntry.getValue().val2);

        // Query mapper.
        try (Cursor<Entry<PojoKey, PojoVal>> cursor = kvView.query(null, null)) {
            Entry<PojoKey, PojoVal> curEntry = cursor.next();
            assertEquals(key.key1, curEntry.getKey().key1);
            assertEquals(val.val1, curEntry.getValue().val1);
            assertEquals(val.val2, curEntry.getValue().val2);
        }

        // removeAll.
        Collection<PojoKey> removeAllRes = kvView.removeAll(null, List.of(key, key2));
        assertEquals(1, removeAllRes.size());
        assertEquals(key2.key1, removeAllRes.iterator().next().key1);

        // replace.
        kvView.put(null, key, val);
        assertTrue(kvView.replace(null, key, val2));
        assertTrue(kvView.replace(null, key, val2, val));

        // getAndReplace.
        PojoVal getAndReplaceRes = kvView.getAndReplace(null, key, val2);
        assertNotNull(getAndReplaceRes);
        assertEquals(val.val1, getAndReplaceRes.val1);
        assertEquals(val.val2, getAndReplaceRes.val2);
    }

    @SuppressWarnings("FieldMayBeFinal")
    private static class Pojo {
        private int key1;
        @Nullable
        private String key2;
        @Nullable
        private String val1;
        private long val2;

        @SuppressWarnings("unused") // Required by serializer.
        Pojo() {
            this(0, null, null, 0L);
        }

        Pojo(int key1, String key2) {
            this(key1, key2, null, 0L);
        }

        Pojo(int key1, @Nullable String key2, @Nullable String val1, long val2) {
            this.key1 = key1;
            this.key2 = key2;
            this.val1 = val1;
            this.val2 = val2;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    private static class PojoKey {
        private int key1;
        @Nullable
        private String key2;

        @SuppressWarnings("unused") // Required by serializer.
        PojoKey() {
            this(0, null);
        }

        PojoKey(int key1, @Nullable String key2) {
            this.key1 = key1;
            this.key2 = key2;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    private static class PojoVal {
        @Nullable
        private String val1;
        private long val2;

        @SuppressWarnings("unused") // Required by serializer.
        PojoVal() {
            this(null, 0L);
        }

        PojoVal(@Nullable String val1, long val2) {
            this.val1 = val1;
            this.val2 = val2;
        }
    }
}
