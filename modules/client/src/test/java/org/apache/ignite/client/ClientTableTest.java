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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.ErrorGroups.Client.TABLE_ID_NOT_FOUND_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.client.fakes.FakeSchemaRegistry;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Table tests.
 */
@SuppressWarnings("ZeroLengthArrayAllocation")
public class ClientTableTest extends AbstractClientTableTest {
    @Test
    public void testGetWithMissedKeyColumnThrowsException() {
        var table = defaultTable().recordView();

        var key = Tuple.create().set("name", "123");

        var ex = assertThrows(IgniteException.class, () -> table.get(null, key));

        assertTrue(ex.getMessage().contains("Missed key column: ID"),
                ex.getMessage());
    }

    @Test
    public void testUpsertGet() {
        var table = defaultTable().recordView();
        var tuple = tuple();

        table.upsert(null, tuple);

        Tuple key = tuple(DEFAULT_ID);
        var resTuple = table.get(null, key);

        assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
        assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        assertEquals("foo", resTuple.valueOrDefault("bar", "foo"));

        assertEquals(DEFAULT_NAME, resTuple.value(1));
        assertEquals(DEFAULT_ID, resTuple.value(0));

        assertEquals(2, resTuple.columnCount());
        assertEquals("ID", resTuple.columnName(0));
        assertEquals("NAME", resTuple.columnName(1));

        var iter = tuple.iterator();

        assertTrue(iter.hasNext());
        assertEquals(DEFAULT_ID, iter.next());

        assertTrue(iter.hasNext());
        assertEquals(DEFAULT_NAME, iter.next());

        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, iter::next);

        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testUpsertGetAsync() {
        var table = defaultTable().recordView();

        var tuple = tuple(42L, "Jack");
        var key = Tuple.create().set("id", 42L);

        var resTuple = table.upsertAsync(null, tuple).thenCompose(t -> table.getAsync(null, key)).join();

        assertEquals("Jack", resTuple.stringValue("name"));
        assertEquals(42L, resTuple.longValue("id"));
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testGetReturningTupleWithUnknownSchemaRequestsNewSchema() {
        var embeddedView = defaultTable().recordView();

        try (var client2 = startClient()) {
            // Upsert from client, which will cache schema version 1.
            FakeSchemaRegistry.setLastVer(1);
            RecordView<Tuple> clientView = client2.tables().table(DEFAULT_TABLE).recordView();
            clientView.upsert(null, tuple(-1L));

            // Upsert from server with schema version 2.
            FakeSchemaRegistry.setLastVer(2);
            embeddedView.upsert(null, tuple());

            // Get from client, which should force schema update and retry.
            var resTuple = clientView.get(null, defaultTupleKey());

            assertEquals(3, resTuple.columnCount());
            assertEquals(2, resTuple.columnIndex("XYZ"));
            assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
            assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        }
    }

    @Test
    public void testOperationWithoutTupleResultRequestsNewSchema() throws Exception {
        AtomicLong idGen = new AtomicLong(1000L);

        checkSchemaUpdate(recordView -> recordView.get(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.getAll(null, List.of(tuple(idGen.incrementAndGet()))));
        checkSchemaUpdate(recordView -> recordView.upsert(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.upsertAll(null, List.of(tuple(idGen.incrementAndGet()))));
        checkSchemaUpdate(recordView -> recordView.getAndUpsert(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.insert(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.insertAll(null, List.of(tuple(idGen.incrementAndGet()))));
        checkSchemaUpdate(recordView -> recordView.replace(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.replace(null, tuple(idGen.incrementAndGet()), tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.getAndReplace(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.delete(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.deleteExact(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.getAndDelete(null, tuple(idGen.incrementAndGet())));
        checkSchemaUpdate(recordView -> recordView.deleteAll(null, List.of(tuple(idGen.incrementAndGet()))));
        checkSchemaUpdate(recordView -> recordView.deleteAllExact(null, List.of(tuple(idGen.incrementAndGet()))));
    }

    @Test
    public void testInsert() {
        var table = defaultTable().recordView();

        var tuple = tuple();
        var tuple2 = tuple(DEFAULT_ID, "abc");

        assertTrue(table.insert(null, tuple));
        assertFalse(table.insert(null, tuple));
        assertFalse(table.insert(null, tuple2));

        var resTuple = table.get(null, defaultTupleKey());
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testInsertCustomTuple() {
        var table = defaultTable().recordView();
        var tuple = new CustomTuple(25L, "Foo");

        assertTrue(table.insert(null, tuple));
        assertFalse(table.insert(null, tuple));

        var resTuple = table.get(null, new CustomTuple(25L));

        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testGetAll() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));
        table.insert(null, tuple(2L, "2"));
        table.insert(null, tuple(3L, "3"));

        List<Tuple> keys = Arrays.asList(tuple(1L), tuple(3L));
        Tuple[] res = sortedTuples(table.getAll(null, keys));

        assertEquals(2, res.length);

        assertEquals(1L, res[0].longValue("id"));
        assertEquals("1", res[0].stringValue("name"));

        assertEquals(3L, res[1].longValue("id"));
        assertEquals("3", res[1].stringValue("name"));
    }

    @Test
    public void testContains() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        long key = 101L;
        Tuple keyTuple = tuple(key);
        Tuple valTuple = tuple(key, "201");

        recordView.insert(null, valTuple);

        assertThrows(NullPointerException.class, () -> recordView.contains(null, null));

        assertTrue(recordView.contains(null, keyTuple));

        Tuple missedKeyTuple = tuple(0L);

        assertFalse(recordView.contains(null, missedKeyTuple));
    }

    @Test
    public void testContainsAll() {
        RecordView<Tuple> recordView = defaultTable().recordView();

        long firstKey = 101L;
        Tuple firstKeyTuple = tuple(firstKey);
        Tuple firstValTuple = tuple(firstKey, "201");

        long secondKey = 102L;
        Tuple secondKeyTuple = tuple(secondKey);
        Tuple secondValTuple = tuple(secondKey, "202");

        long thirdKey = 103L;
        Tuple thirdKeyTuple = tuple(thirdKey);
        Tuple thirdValTuple = tuple(thirdKey, "203");

        List<Tuple> recs = List.of(firstValTuple, secondValTuple, thirdValTuple);

        recordView.insertAll(null, recs);

        assertThrows(NullPointerException.class, () -> recordView.containsAll(null, null));
        assertThrows(NullPointerException.class, () -> recordView.containsAll(null, List.of(firstKeyTuple, null, thirdKeyTuple)));

        assertTrue(recordView.containsAll(null, List.of()));
        assertTrue(recordView.containsAll(null, List.of(firstKeyTuple)));
        assertTrue(recordView.containsAll(null, List.of(firstKeyTuple, secondKeyTuple, thirdKeyTuple)));

        long missedKey = 0L;
        Tuple missedKeyTuple = tuple(missedKey);

        assertFalse(recordView.containsAll(null, List.of(missedKeyTuple)));
        assertFalse(recordView.containsAll(null, List.of(firstKeyTuple, secondKeyTuple, missedKeyTuple)));
    }

    @Test
    public void testUpsertAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.upsertAll(null, data);

        assertEquals("1", table.get(null, tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(null, tuple(2L)).stringValue("name"));

        List<Tuple> data2 = Arrays.asList(tuple(1L, "10"), tuple(3L, "30"));
        table.upsertAll(null, data2);

        assertEquals("10", table.get(null, tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(null, tuple(2L)).stringValue("name"));
        assertEquals("30", table.get(null, tuple(3L)).stringValue("name"));
    }

    @Test
    public void testInsertAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        var skippedTuples = table.insertAll(null, data);

        assertEquals(0, skippedTuples.size());
        assertEquals("1", table.get(null, tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(null, tuple(2L)).stringValue("name"));

        List<Tuple> data2 = Arrays.asList(tuple(1L, "10"), tuple(3L, "30"));
        var skippedTuples2 = table.insertAll(null, data2).toArray(new Tuple[0]);

        assertEquals(1, skippedTuples2.length);
        assertEquals(1L, skippedTuples2[0].longValue("id"));
        assertEquals("1", table.get(null, tuple(1L)).stringValue("name"));
        assertEquals("2", table.get(null, tuple(2L)).stringValue("name"));
        assertEquals("30", table.get(null, tuple(3L)).stringValue("name"));
    }

    @Test
    public void testReplace() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));

        assertFalse(table.replace(null, tuple(3L, "3")));
        assertNull(table.get(null, tuple(3L)));

        assertTrue(table.replace(null, tuple(1L, "2")));
        assertEquals("2", table.get(null, tuple(1L)).value("name"));
    }

    @Test
    public void testReplaceExact() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));

        assertFalse(table.replace(null, tuple(3L, "3"), tuple(3L, "4")));
        assertNull(table.get(null, tuple(3L)));

        assertFalse(table.replace(null, tuple(1L, "2"), tuple(1L, "3")));
        assertTrue(table.replace(null, tuple(1L, "1"), tuple(1L, "3")));
        assertEquals("3", table.get(null, tuple(1L)).value("name"));
    }

    @Test
    public void testGetAndReplace() {
        var table = defaultTable().recordView();
        var tuple = tuple(1L, "1");
        table.insert(null, tuple);

        assertNull(table.getAndReplace(null, tuple(3L, "3")));
        assertNull(table.get(null, tuple(3L)));

        var replaceRes = table.getAndReplace(null, tuple(1L, "2"));
        assertTupleEquals(tuple, replaceRes);
        assertEquals("2", table.get(null, tuple(1L)).value("name"));
    }

    @Test
    public void testDelete() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));

        assertFalse(table.delete(null, tuple(2L)));
        assertTrue(table.delete(null, tuple(1L)));
        assertNull(table.get(null, tuple(1L)));
    }

    @Test
    public void testDeleteExact() {
        var table = defaultTable().recordView();
        table.insert(null, tuple(1L, "1"));
        table.insert(null, tuple(2L, "2"));

        assertFalse(table.deleteExact(null, tuple(1L)));
        assertFalse(table.deleteExact(null, tuple(1L, "x")));
        assertTrue(table.deleteExact(null, tuple(1L, "1")));
        assertFalse(table.deleteExact(null, tuple(2L)));
        assertFalse(table.deleteExact(null, tuple(3L)));

        assertNull(table.get(null, tuple(1L)));
        assertNotNull(table.get(null, tuple(2L)));
    }

    @Test
    public void testGetAndDelete() {
        var table = defaultTable().recordView();
        var tuple = tuple(1L, "1");
        table.insert(null, tuple);

        var deleted = table.getAndDelete(null, tuple(1L));

        assertNull(table.getAndDelete(null, tuple(1L)));
        assertNull(table.getAndDelete(null, tuple(2L)));
        assertTupleEquals(tuple, deleted);
    }

    @Test
    public void testDeleteAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.insertAll(null, data);

        List<Tuple> toDelete = Arrays.asList(tuple(1L, "x"), tuple(3L, "y"), tuple(4L, "z"));
        var skippedTuples = sortedTuples(table.deleteAll(null, toDelete));

        assertEquals(2, skippedTuples.length);
        assertNull(table.get(null, tuple(1L)));
        assertNotNull(table.get(null, tuple(2L)));

        assertEquals(3L, skippedTuples[0].longValue("id"));
        assertEquals(-1, skippedTuples[0].columnIndex("name"));

        assertEquals(4L, skippedTuples[1].longValue("id"));
        assertEquals(-1, skippedTuples[1].columnIndex("name"));
    }

    @Test
    public void testDeleteAllExact() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.insertAll(null, data);

        List<Tuple> toDelete = Arrays.asList(tuple(1L, "1"), tuple(2L, "y"), tuple(3L, "z"));
        var skippedTuples = sortedTuples(table.deleteAllExact(null, toDelete));

        assertEquals(2, skippedTuples.length);
        assertNull(table.get(null, tuple(1L)));
        assertNotNull(table.get(null, tuple(2L)));

        assertEquals(2L, skippedTuples[0].longValue("id"));
        assertEquals("y", skippedTuples[0].stringValue("name"));

        assertEquals(3L, skippedTuples[1].longValue("id"));
        assertEquals("z", skippedTuples[1].stringValue("name"));
    }

    @Test
    public void testColumnWithDefaultValueNotSetReturnsDefault() {
        RecordView<Tuple> table = tableWithDefaultValues().recordView();

        var tuple = Tuple.create()
                .set("id", 1);

        table.upsert(null, tuple);

        var res = table.get(null, tuple);

        assertEquals("def_str", res.stringValue("str"));
        assertEquals("def_str2", res.stringValue("strNonNull"));
    }

    @Test
    public void testNullableColumnWithDefaultValueSetNullReturnsNull() {
        RecordView<Tuple> table = tableWithDefaultValues().recordView();

        var tuple = Tuple.create()
                .set("id", 1)
                .set("str", null);

        table.upsert(null, tuple);

        var res = table.get(null, tuple);

        assertNull(res.stringValue("str"));
    }

    @Test
    public void testNonNullableColumnWithDefaultValueSetNullThrowsException() {
        RecordView<Tuple> table = tableWithDefaultValues().recordView();

        var tuple = Tuple.create()
                .set("id", 1)
                .set("strNonNull", null);

        var ex = assertThrows(IgniteException.class, () -> table.upsert(null, tuple));

        assertTrue(ex.getMessage().contains("Column 'STRNONNULL' does not allow NULLs"), ex.getMessage());
    }

    @Test
    public void testColumnTypeMismatchThrowsException() {
        var tuple = Tuple.create().set("id", "str");

        var ex = assertThrows(IgniteException.class, () -> defaultTable().recordView().upsert(null, tuple));
        assertEquals("Value type does not match [column='ID', expected=INT64, actual=STRING]", ex.getMessage());
    }

    @Test
    public void testGetFromDroppedTableThrowsException() {
        ((FakeIgniteTables) server.tables()).createTable("drop-me");
        Table clientTable = client.tables().table("drop-me");
        ((FakeIgniteTables) server.tables()).dropTable("drop-me");

        Tuple tuple = Tuple.create().set("id", 1);
        var ex = assertThrows(IgniteException.class, () -> clientTable.recordView().get(null, tuple));

        assertThat(ex.getMessage(), containsString("Table does not exist: "));
        assertEquals(TABLE_ID_NOT_FOUND_ERR, ex.code());
    }

    private void checkSchemaUpdate(Consumer<RecordView<Tuple>> consumer) throws Exception {
        try (var client2 = startClient()) {
            var table = client2.tables().table(defaultTable().name());
            Map<Integer, Object> schemas = IgniteTestUtils.getFieldValue(table, "schemas");
            var recView = table.recordView();

            assertEquals(0, schemas.size());

            FakeSchemaRegistry.setLastVer(1);
            consumer.accept(recView);
            assertNull(schemas.get(2));

            FakeSchemaRegistry.setLastVer(2);
            consumer.accept(recView);

            assertTrue(waitForCondition(() -> schemas.get(2) != null, 1000));
        }
    }
}
