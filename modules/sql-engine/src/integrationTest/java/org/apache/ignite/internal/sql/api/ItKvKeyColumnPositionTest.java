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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for KV view with different key column placement.
 */
public class ItKvKeyColumnPositionTest extends BaseSqlIntegrationTest {

    private static final AtomicInteger ID_NUM = new AtomicInteger();

    // server

    private KeyValueView<IntString, BoolInt> serverValKey;

    private KeyValueView<IntString, BoolInt> serverKeyVal;

    private KeyValueView<IntString, BoolInt> serverValKeyFlipped;

    private KeyValueView<IntString, BoolInt> serverKeyValFlipped;

    private KeyValueView<String, IntBoolDate> serverSimpleKeyVal;

    private KeyValueView<String, IntBoolDate> serverSimpleValKey;

    // client
    private KeyValueView<IntString, BoolInt> clientValKey;

    private KeyValueView<IntString, BoolInt> clientKeyVal;

    private KeyValueView<IntString, BoolInt> clientValKeyFlipped;

    private KeyValueView<IntString, BoolInt> clientKeyValFlipped;

    private KeyValueView<String, IntBoolDate> clientSimpleKeyVal;

    private KeyValueView<String, IntBoolDate> clientSimpleValKey;

    private IgniteClient client;

    @BeforeAll
    public void setup() {
        sql("CREATE TABLE key_val (intCol INT, boolCol BOOLEAN, dateCol DATE, strCol VARCHAR, PRIMARY KEY (intCol, strCol))");
        sql("CREATE TABLE key_val_flip (intCol INT, boolCol BOOLEAN, dateCol DATE, strCol VARCHAR, PRIMARY KEY (strCol, intCol))");

        sql("CREATE TABLE val_key (boolCol BOOLEAN, intCol INT, dateCol DATE, strCol VARCHAR, PRIMARY KEY (intCol, strCol))");
        sql("CREATE TABLE val_key_flip (boolCol BOOLEAN, intCol INT, dateCol DATE, strCol VARCHAR, PRIMARY KEY (strCol, intCol))");

        sql("CREATE TABLE simple_key_val (strCol VARCHAR, boolCol BOOLEAN, dateCol DATE, intCol INT, PRIMARY KEY (strCol))");
        sql("CREATE TABLE simple_val_key (boolCol BOOLEAN, intCol INT, dateCol DATE, strCol VARCHAR, PRIMARY KEY (strCol))");

        // SERVER
        Ignite ignite = CLUSTER.aliveNode();

        IgniteTables serverTables = ignite.tables();
        serverKeyVal = serverTables.table("key_val").keyValueView(IntString.class, BoolInt.class);
        serverKeyValFlipped = serverTables.table("key_val_flip").keyValueView(IntString.class, BoolInt.class);
        serverValKey = serverTables.table("val_key").keyValueView(IntString.class, BoolInt.class);
        serverValKeyFlipped = serverTables.table("val_key_flip").keyValueView(IntString.class, BoolInt.class);

        serverSimpleKeyVal = serverTables.table("simple_key_val").keyValueView(Mapper.of(String.class), Mapper.of(IntBoolDate.class));
        serverSimpleValKey = serverTables.table("simple_val_key").keyValueView(Mapper.of(String.class), Mapper.of(IntBoolDate.class));

        // CLIENT
        String addressString = "127.0.0.1:" + unwrapIgniteImpl(ignite).clientAddress().port();

        client = IgniteClient.builder().addresses(addressString).build();

        IgniteTables clientTables = client.tables();
        clientKeyValFlipped = clientTables.table("key_val_flip").keyValueView(IntString.class, BoolInt.class);
        clientKeyVal = clientTables.table("key_val").keyValueView(IntString.class, BoolInt.class);
        clientValKey = clientTables.table("val_key").keyValueView(IntString.class, BoolInt.class);
        clientValKeyFlipped = clientTables.table("val_key_flip").keyValueView(IntString.class, BoolInt.class);

        clientSimpleKeyVal = clientTables.table("simple_key_val").keyValueView(Mapper.of(String.class), Mapper.of(IntBoolDate.class));
        clientSimpleValKey = clientTables.table("simple_val_key").keyValueView(Mapper.of(String.class), Mapper.of(IntBoolDate.class));
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterAll
    public void closeClient() {
        client.close();
    }

    private List<Arguments> complexKeyKvs() {
        return List.of(
                Arguments.of(Named.named("server key_val_key", serverKeyVal)),
                Arguments.of(Named.named("server key_val_key_flipped", serverKeyValFlipped)),
                Arguments.of(Named.named("server val_key_val_key", serverValKey)),
                Arguments.of(Named.named("server val_key_val_key_flipped", serverValKeyFlipped)),
                Arguments.of(Named.named("client key_val_key", clientKeyVal)),
                Arguments.of(Named.named("client key_val_key_flipped", clientKeyValFlipped)),
                Arguments.of(Named.named("client val_key_val_key", clientValKey)),
                Arguments.of(Named.named("client val_key_val_key", clientValKeyFlipped))
        );
    }

    private List<Arguments> simpleKeyKvs() {
        return List.of(
                Arguments.of(Named.named("server simple key val", serverSimpleKeyVal)),
                Arguments.of(Named.named("server simple val key", serverSimpleValKey)),
                Arguments.of(Named.named("client simple key val", clientSimpleKeyVal)),
                Arguments.of(Named.named("client simple val key", clientSimpleValKey))
        );
    }

    @ParameterizedTest
    @MethodSource("complexKeyKvs")
    public void testPutGet(KeyValueView<IntString, BoolInt> kvView) {
        {
            IntString key = newKey();

            BoolInt val = new BoolInt();
            val.boolCol = true;
            val.dateCol = LocalDate.now();

            kvView.put(null, key, val);

            BoolInt retrieved = kvView.get(null, key);
            assertEquals(val, retrieved);
        }

        {
            IntString key = newKey();

            BoolInt val = new BoolInt();
            val.boolCol = true;

            kvView.put(null, key, val);

            BoolInt retrieved = kvView.get(null, key);
            assertEquals(val, retrieved);
        }
    }

    @ParameterizedTest
    @MethodSource("simpleKeyKvs")
    public void testSimplePutGet(KeyValueView<String, IntBoolDate> kvView) {
        IntBoolDate val = new IntBoolDate();
        val.boolCol = true;
        val.intCol = (int) System.nanoTime();
        val.dateCol = LocalDate.ofEpochDay(ThreadLocalRandom.current().nextLong(1000));

        kvView.put(null, "1", val);

        IntBoolDate retrieved = kvView.get(null, "1");
        assertEquals(val, retrieved);
    }

    /**
     * Checks remove all since this API calls {@link KvMarshaller#unmarshalKeyOnly(Row)}.
     */
    @ParameterizedTest
    @MethodSource("complexKeyKvs")
    public void testRemoveAll(KeyValueView<IntString, BoolInt> kvView) {
        IntString k1 = newKey();
        IntString k2 = newKey();

        kvView.put(null, k1, new BoolInt());
        kvView.put(null, k2, new BoolInt());

        kvView.removeAll(null, List.of(k1, k2));

        assertNull(kvView.get(null, k1));
        assertNull(kvView.get(null, k2));
    }

    /**
     * Checks remove all since this API calls {@link KvMarshaller#marshal(Object, Object)}}.
     */
    @ParameterizedTest
    @MethodSource("complexKeyKvs")
    public void testPutAll(KeyValueView<IntString, BoolInt> kvView) {
        IntString key1 = newKey();
        BoolInt val1 = new BoolInt();
        val1.dateCol = LocalDate.now().plusDays(ThreadLocalRandom.current().nextInt(100));

        IntString key2 = newKey();
        BoolInt val2 = new BoolInt();
        val2.dateCol = LocalDate.now().plusDays(ThreadLocalRandom.current().nextInt(100));

        kvView.putAll(null, Map.of(key1, val1, key2, val2));

        BoolInt retrieved1 = kvView.get(null, key1);
        assertEquals(val1, retrieved1);

        BoolInt retrieved2 = kvView.get(null, key2);
        assertEquals(val2, retrieved2);
    }

    @ParameterizedTest
    @MethodSource("complexKeyKvs")
    public void testGetAll(KeyValueView<IntString, BoolInt> kvView) {
        IntString key1 = newKey();
        BoolInt val1 = new BoolInt();
        val1.dateCol = LocalDate.now().plusDays(ThreadLocalRandom.current().nextInt(100));
        kvView.put(null, key1, val1);

        IntString key2 = newKey();
        BoolInt val2 = new BoolInt();
        val2.dateCol = LocalDate.now().plusDays(ThreadLocalRandom.current().nextInt(100));

        kvView.put(null, key2, val2);

        Map<IntString, BoolInt> all = kvView.getAll(null, List.of(key1, key2));
        assertEquals(Map.of(key1, val1, key2, val2), all);
    }

    private static IntString newKey() {
        int val = ID_NUM.incrementAndGet();

        IntString key = new IntString();
        key.intCol = val;
        key.strCol = Integer.toString(val);
        return key;
    }

    static class IntString {
        @IgniteToStringInclude
        int intCol;
        @IgniteToStringInclude
        String strCol;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IntString intString = (IntString) o;
            return intCol == intString.intCol && Objects.equals(strCol, intString.strCol);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intCol, strCol);
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    static class BoolInt {
        @IgniteToStringInclude
        boolean boolCol;
        @IgniteToStringInclude
        LocalDate dateCol;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BoolInt boolInt = (BoolInt) o;
            return boolCol == boolInt.boolCol && Objects.equals(dateCol, boolInt.dateCol);
        }

        @Override
        public int hashCode() {
            return Objects.hash(boolCol, dateCol);
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    static class IntBoolDate {
        @IgniteToStringInclude
        int intCol;
        @IgniteToStringInclude
        boolean boolCol;
        @IgniteToStringInclude
        LocalDate dateCol;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IntBoolDate that = (IntBoolDate) o;
            return intCol == that.intCol && boolCol == that.boolCol && Objects.equals(dateCol, that.dateCol);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intCol, boolCol, dateCol);
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }
}
