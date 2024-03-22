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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
    // TODO https://issues.apache.org/jira/browse/IGNITE-21768
    @SuppressWarnings("unused")
    private KeyValueView<IntString, BoolInt> clientValKey;

    @SuppressWarnings("unused")
    private KeyValueView<IntString, BoolInt> clientKeyVal;

    @SuppressWarnings("unused")
    private KeyValueView<IntString, BoolInt> clientValKeyFlipped;

    @SuppressWarnings("unused")
    private KeyValueView<IntString, BoolInt> clientKeyValFlipped;

    private KeyValueView<String, IntBoolDate> clientSimpleKeyVal;

    @SuppressWarnings("unused")
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
        IgniteImpl igniteImpl = CLUSTER.aliveNode();

        IgniteTables serverTables = igniteImpl.tables();
        serverKeyVal = serverTables.table("key_val").keyValueView(IntString.class, BoolInt.class);
        serverKeyValFlipped = serverTables.table("key_val_flip").keyValueView(IntString.class, BoolInt.class);
        serverValKey = serverTables.table("val_key").keyValueView(IntString.class, BoolInt.class);
        serverValKeyFlipped = serverTables.table("val_key_flip").keyValueView(IntString.class, BoolInt.class);

        serverSimpleKeyVal = serverTables.table("simple_key_val").keyValueView(Mapper.of(String.class), Mapper.of(IntBoolDate.class));
        serverSimpleValKey = serverTables.table("simple_val_key").keyValueView(Mapper.of(String.class), Mapper.of(IntBoolDate.class));

        // CLIENT

        String addressString = "127.0.0.1:" + igniteImpl.clientAddress().port();

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
    public void closeClient() throws Exception {
        client.close();
    }

    private List<Arguments> complexKeyKvs() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-21768
        // Arguments.of(Named.named("client key_val_key", clientKeyVal)),
        // Arguments.of(Named.named("client key_val_key_flipped", clientKeyValFlipped)),
        // Arguments.of(Named.named("client val_key_val_key", clientValKey)),
        // Arguments.of(Named.named("client val_key_val_key", clientValKeyFlipped))

        return List.of(
                Arguments.of(Named.named("server key_val_key", serverKeyVal)),
                Arguments.of(Named.named("server key_val_key_flipped", serverKeyValFlipped)),
                Arguments.of(Named.named("server val_key_val_key", serverValKey)),
                Arguments.of(Named.named("server val_key_val_key_flipped", serverValKeyFlipped))
        );
    }

    private List<Arguments> simpleKeyKvs() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-21768
        // Arguments.of(Named.named("client simple val key", clientSimpleValKey))

        return List.of(
                Arguments.of(Named.named("server simple key val", serverSimpleKeyVal)),
                Arguments.of(Named.named("server simple val key", serverSimpleValKey)),
                Arguments.of(Named.named("client simple key val", clientSimpleKeyVal))
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

            assertNotNull(retrieved);
            assertEquals(val.boolCol, retrieved.boolCol);
            assertEquals(val.dateCol, retrieved.dateCol);
        }

        {
            IntString key = newKey();

            BoolInt val = new BoolInt();
            val.boolCol = true;

            kvView.put(null, key, val);

            BoolInt retrieved = kvView.get(null, key);
            assertNotNull(retrieved);
            assertEquals(val.boolCol, retrieved.boolCol);
            assertEquals(val.dateCol, retrieved.dateCol);
        }
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21836")
    @ParameterizedTest
    @MethodSource("nullableKvsSimple")
    public void testNotNullableGetSimple(KeyValueView<String, IntBoolDate> kvView) {
        String key = String.valueOf(ID_NUM.incrementAndGet());
        kvView.put(null, key, null);

        NullableValue<IntBoolDate> nullable = kvView.getNullable(null, key);
        assertNull(nullable.get());
    }

    private List<Arguments> nullableKvsSimple() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-21768
        // Arguments.of(Named.named("client", clientSimpleKeyVal))

        return List.of(
                Arguments.of(Named.named("server", serverSimpleValKey))
        );
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21836")
    @ParameterizedTest
    @MethodSource("complexKeyKvs")
    public void testNotNullableGetComplex(KeyValueView<IntString, BoolInt> kvView) {
        IntString key = newKey();
        kvView.put(null, key, null);

        NullableValue<BoolInt> nullable = kvView.getNullable(null, key);
        assertNull(nullable.get());
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

        assertNotNull(retrieved);
        assertEquals(val.boolCol, retrieved.boolCol);
        assertEquals(val.intCol, retrieved.intCol);
        assertEquals(val.dateCol, retrieved.dateCol);
    }

    /**
     * Checks remove all since this API calls {@link KvMarshaller#unmarshalKey(Row)}.
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

    private static IntString newKey() {
        int val = ID_NUM.incrementAndGet();

        IntString key = new IntString();
        key.intCol = val;
        key.strCol = Integer.toString(val);
        return key;
    }

    static class IntString {
        int intCol;
        String strCol;
    }

    static class BoolInt {
        boolean boolCol;
        LocalDate dateCol;
    }

    static class IntBoolDate {
        int intCol;
        boolean boolCol;
        LocalDate dateCol;
    }
}
