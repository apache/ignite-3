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

import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests custom key column order table operations with thin client.
 */
@SuppressWarnings("resource")
public class ItThinClientCustomKeyColumnOrderTest extends ItAbstractThinClientTest {
    private static final String TABLE_NAME2 = "TBL2";

    @BeforeAll
    void createTable() {
        String query = "CREATE TABLE " + TABLE_NAME2
                + " (val1 VARCHAR, key1 INT, val2 BIGINT, key2 VARCHAR, PRIMARY KEY (key1, key2)) "
                + "colocate by (key2, key1)";

        client().sql().createSession().execute(null, query);
    }

    @Test
    void testRecordBinaryView() {
        var table = client().tables().table(TABLE_NAME2);

        var recView = table.recordView();

        Tuple key = Tuple.create().set("key1", 1).set("key2", "key2");
        Tuple val = Tuple.create().set("key1", 1).set("key2", "key2").set("val1", "val1").set("val2", 2L);
        recView.insert(null, val);

        Tuple res = recView.get(null, key);
        assertEquals(val, res);
    }

    @Test
    void testRecordView() {
        var table = client().tables().table(TABLE_NAME2);

        var recView = table.recordView(Pojo.class);

        Pojo key = new Pojo(1, "key2");
        Pojo val = new Pojo(1, "key2", "val1", 2L);
        recView.insert(null, val);

        Pojo res = recView.get(null, key);
        assertEquals(val.key1, res.key1);
        assertEquals(val.key2, res.key2);
        assertEquals(val.val1, res.val1);
        assertEquals(val.val2, res.val2);
    }

    @Test
    void testKeyValueBinaryView() {
        var table = client().tables().table(TABLE_NAME2);

        var kvView = table.keyValueView();

        Tuple key = Tuple.create().set("key1", 1).set("key2", "key2");
        Tuple val = Tuple.create().set("val1", "val1").set("val2", 2L);
        kvView.put(null, key, val);

        Tuple res = kvView.get(null, key);
        assertEquals(val, res);
    }

    @Test
    void testKeyValueView() {
        var table = client().tables().table(TABLE_NAME2);

        var kvView = table.keyValueView(PojoKey.class, PojoVal.class);

        PojoKey key = new PojoKey(1, "key2");
        PojoVal val = new PojoVal("val1", 2L);
        kvView.put(null, key, val);

        PojoVal res = kvView.get(null, key);
        assertEquals(val.val1, res.val1);
        assertEquals(val.val2, res.val2);
    }

    @SuppressWarnings("FieldMayBeFinal")
    private static class Pojo {
        private int key1;
        @Nullable
        private String key2;
        @Nullable
        private String val1;
        private long val2;

        @SuppressWarnings("unused")
            // Required by serializer.
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