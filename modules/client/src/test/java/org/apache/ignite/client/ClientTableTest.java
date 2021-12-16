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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.ignite.client.fakes.FakeSchemaRegistry;
import org.apache.ignite.internal.client.table.ClientTuple;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Table tests.
 */
public class ClientTableTest extends AbstractClientTableTest {
    @Test
    public void testGetWithMissedKeyColumnThrowsException() {
        var table = defaultTable().recordView();

        var key = Tuple.create().set("name", "123");

        var ex = assertThrows(CompletionException.class, () -> table.get(key, null));

        assertTrue(ex.getMessage().contains("Missed key column: id"),
                ex.getMessage());
    }

    @Test
    public void testUpsertGet() {
        var table = defaultTable().recordView();
        var tuple = tuple();

        table.upsert(tuple, null);

        Tuple key = tuple(123L);
        var resTuple = table.get(key, null);

        assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
        assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        assertEquals("foo", resTuple.valueOrDefault("bar", "foo"));

        assertEquals(DEFAULT_NAME, resTuple.value(1));
        assertEquals(DEFAULT_ID, resTuple.value(0));

        assertEquals(2, resTuple.columnCount());
        assertEquals("id", resTuple.columnName(0));
        assertEquals("name", resTuple.columnName(1));

        var iter = tuple.iterator();

        assertTrue(iter.hasNext());
        assertEquals(DEFAULT_ID, iter.next());

        assertTrue(iter.hasNext());
        assertEquals(DEFAULT_NAME, iter.next());

        assertFalse(iter.hasNext());
        assertNull(iter.next());

        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testUpsertGetAsync() {
        var table = defaultTable().recordView();

        var tuple = tuple(42L, "Jack");
        var key = Tuple.create().set("id", 42L);

        var resTuple = table.upsertAsync(tuple, null).thenCompose(t -> table.getAsync(key, null)).join();

        assertEquals("Jack", resTuple.stringValue("name"));
        assertEquals(42L, resTuple.longValue("id"));
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15194")
    public void testGetReturningTupleWithUnknownSchemaRequestsNewSchema() throws Exception {
        FakeSchemaRegistry.setLastVer(2);

        var table = defaultTable();
        var recView = table.recordView();
        Tuple tuple = tuple();
        recView.upsert(tuple, null);

        FakeSchemaRegistry.setLastVer(1);

        try (var client2 = startClient()) {
            RecordView<Tuple> table2 = client2.tables().table(table.name()).recordView();
            var tuple2 = tuple();
            var resTuple = table2.get(tuple2, null);

            assertEquals(1, ((ClientTuple) tuple2).schema().version());
            assertEquals(2, ((ClientTuple) resTuple).schema().version());

            assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
            assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        }
    }

    @Test
    public void testInsert() {
        var table = defaultTable().recordView();

        var tuple = tuple();
        var tuple2 = tuple(DEFAULT_ID, "abc");

        assertTrue(table.insert(tuple, null));
        assertFalse(table.insert(tuple, null));
        assertFalse(table.insert(tuple2, null));

        var resTuple = table.get(defaultTupleKey(), null);
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testInsertCustomTuple() {
        var table = defaultTable().recordView();
        var tuple = new CustomTuple(25L, "Foo");

        assertTrue(table.insert(tuple, null));
        assertFalse(table.insert(tuple, null));

        var resTuple = table.get(new CustomTuple(25L), null);

        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testGetAll() {
        var table = defaultTable().recordView();
        table.insert(tuple(1L, "1"), null);
        table.insert(tuple(2L, "2"), null);
        table.insert(tuple(3L, "3"), null);

        List<Tuple> keys = Arrays.asList(tuple(1L), tuple(3L));
        Tuple[] res = sortedTuples(table.getAll(keys, null));

        assertEquals(2, res.length);

        assertEquals(1L, res[0].longValue("id"));
        assertEquals("1", res[0].stringValue("name"));

        assertEquals(3L, res[1].longValue("id"));
        assertEquals("3", res[1].stringValue("name"));
    }

    @Test
    public void testUpsertAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.upsertAll(data, null);

        assertEquals("1", table.get(tuple(1L), null).stringValue("name"));
        assertEquals("2", table.get(tuple(2L), null).stringValue("name"));

        List<Tuple> data2 = Arrays.asList(tuple(1L, "10"), tuple(3L, "30"));
        table.upsertAll(data2, null);

        assertEquals("10", table.get(tuple(1L), null).stringValue("name"));
        assertEquals("2", table.get(tuple(2L), null).stringValue("name"));
        assertEquals("30", table.get(tuple(3L), null).stringValue("name"));
    }

    @Test
    public void testInsertAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        var skippedTuples = table.insertAll(data, null);

        assertEquals(0, skippedTuples.size());
        assertEquals("1", table.get(tuple(1L), null).stringValue("name"));
        assertEquals("2", table.get(tuple(2L), null).stringValue("name"));

        List<Tuple> data2 = Arrays.asList(tuple(1L, "10"), tuple(3L, "30"));
        var skippedTuples2 = table.insertAll(data2, null).toArray(new Tuple[0]);

        assertEquals(1, skippedTuples2.length);
        assertEquals(1L, skippedTuples2[0].longValue("id"));
        assertEquals("1", table.get(tuple(1L), null).stringValue("name"));
        assertEquals("2", table.get(tuple(2L), null).stringValue("name"));
        assertEquals("30", table.get(tuple(3L), null).stringValue("name"));
    }

    @Test
    public void testReplace() {
        var table = defaultTable().recordView();
        table.insert(tuple(1L, "1"), null);

        assertFalse(table.replace(tuple(3L, "3"), null));
        assertNull(table.get(tuple(3L), null));

        assertTrue(table.replace(tuple(1L, "2"), null));
        assertEquals("2", table.get(tuple(1L), null).value("name"));
    }

    @Test
    public void testReplaceExact() {
        var table = defaultTable().recordView();
        table.insert(tuple(1L, "1"), null);

        assertFalse(table.replace(tuple(3L, "3"), tuple(3L, "4"), null));
        assertNull(table.get(tuple(3L), null));

        assertFalse(table.replace(tuple(1L, "2"), tuple(1L, "3"), null));
        assertTrue(table.replace(tuple(1L, "1"), tuple(1L, "3"), null));
        assertEquals("3", table.get(tuple(1L), null).value("name"));
    }

    @Test
    public void testGetAndReplace() {
        var table = defaultTable().recordView();
        var tuple = tuple(1L, "1");
        table.insert(tuple, null);

        assertNull(table.getAndReplace(tuple(3L, "3"), null));
        assertNull(table.get(tuple(3L), null));

        var replaceRes = table.getAndReplace(tuple(1L, "2"), null);
        assertTupleEquals(tuple, replaceRes);
        assertEquals("2", table.get(tuple(1L), null).value("name"));
    }

    @Test
    public void testDelete() {
        var table = defaultTable().recordView();
        table.insert(tuple(1L, "1"), null);

        assertFalse(table.delete(tuple(2L), null));
        assertTrue(table.delete(tuple(1L), null));
        assertNull(table.get(tuple(1L), null));
    }

    @Test
    public void testDeleteExact() {
        var table = defaultTable().recordView();
        table.insert(tuple(1L, "1"), null);
        table.insert(tuple(2L, "2"), null);

        assertFalse(table.deleteExact(tuple(1L), null));
        assertFalse(table.deleteExact(tuple(1L, "x"), null));
        assertTrue(table.deleteExact(tuple(1L, "1"), null));
        assertFalse(table.deleteExact(tuple(2L), null));
        assertFalse(table.deleteExact(tuple(3L), null));

        assertNull(table.get(tuple(1L), null));
        assertNotNull(table.get(tuple(2L), null));
    }

    @Test
    public void testGetAndDelete() {
        var table = defaultTable().recordView();
        var tuple = tuple(1L, "1");
        table.insert(tuple, null);

        var deleted = table.getAndDelete(tuple(1L), null);

        assertNull(table.getAndDelete(tuple(1L), null));
        assertNull(table.getAndDelete(tuple(2L), null));
        assertTupleEquals(tuple, deleted);
    }

    @Test
    public void testDeleteAll() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.insertAll(data, null);

        List<Tuple> toDelete = Arrays.asList(tuple(1L, "x"), tuple(3L, "y"), tuple(4L, "z"));
        var skippedTuples = sortedTuples(table.deleteAll(toDelete, null));

        assertEquals(2, skippedTuples.length);
        assertNull(table.get(tuple(1L), null));
        assertNotNull(table.get(tuple(2L), null));

        assertEquals(3L, skippedTuples[0].longValue("id"));
        assertNull(skippedTuples[0].stringValue("name"));

        assertEquals(4L, skippedTuples[1].longValue("id"));
        assertNull(skippedTuples[1].stringValue("name"));
    }

    @Test
    public void testDeleteAllExact() {
        var table = defaultTable().recordView();

        List<Tuple> data = Arrays.asList(tuple(1L, "1"), tuple(2L, "2"));
        table.insertAll(data, null);

        List<Tuple> toDelete = Arrays.asList(tuple(1L, "1"), tuple(2L, "y"), tuple(3L, "z"));
        var skippedTuples = sortedTuples(table.deleteAllExact(toDelete, null));

        assertEquals(2, skippedTuples.length);
        assertNull(table.get(tuple(1L), null));
        assertNotNull(table.get(tuple(2L), null));

        assertEquals(2L, skippedTuples[0].longValue("id"));
        assertEquals("y", skippedTuples[0].stringValue("name"));

        assertEquals(3L, skippedTuples[1].longValue("id"));
        assertEquals("z", skippedTuples[1].stringValue("name"));
    }
}
