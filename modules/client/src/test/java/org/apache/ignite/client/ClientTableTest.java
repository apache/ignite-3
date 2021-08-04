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

import java.util.concurrent.CompletionException;

import org.apache.ignite.client.fakes.FakeSchemaRegistry;
import org.apache.ignite.internal.client.table.ClientTupleBuilder;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Table tests.
 */
public class ClientTableTest extends AbstractClientTest {
    private static final String DEFAULT_NAME = "John";

    private static final Long DEFAULT_ID = 123L;

    @Test
    public void testGetWithNullInNotNullableKeyColumnThrowsException() {
        Table table = getDefaultTable();

        var key = table.tupleBuilder().set("name", "123").build();

        var ex = assertThrows(CompletionException.class, () -> table.get(key));

        assertTrue(ex.getMessage().contains("Failed to set column (null was passed, but column is not nullable)"),
                ex.getMessage());
    }

    @Test
    public void testUpsertGet() {
        Table table = getDefaultTable();
        var tuple = getDefaultTuple(table);

        table.upsert(tuple);

        Tuple key = table.tupleBuilder().set("id", 123).build();
        var resTuple = table.get(key);

        assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
        assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        assertEquals("foo", resTuple.valueOrDefault("bar", "foo"));

        assertEquals(DEFAULT_NAME, resTuple.value(1));
        assertEquals(DEFAULT_ID, (Long) resTuple.value(0));

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
        Table table = getDefaultTable();

        var tuple = table.tupleBuilder()
                .set("id", 42L)
                .set("name", "Jack")
                .build();

        Tuple key = table.tupleBuilder().set("id", 42).build();

        var resTuple = table.upsertAsync(tuple).thenCompose(t -> table.getAsync(key)).join();

        assertEquals("Jack", resTuple.stringValue("name"));
        assertEquals(42L, resTuple.longValue("id"));
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testGetReturningTupleWithUnknownSchemaRequestsNewSchema() throws Exception {
        FakeSchemaRegistry.lastVer = 2;

        var table = getDefaultTable();
        Tuple tuple = getDefaultTuple(table);
        table.upsert(tuple);

        assertEquals(2, ((ClientTupleBuilder)tuple).schema().version());

        FakeSchemaRegistry.lastVer = 1;

        try (var client2 = startClient()) {
            Table table2 = client2.tables().table(table.tableName());
            var tuple2 = getDefaultTuple(table2);
            var resTuple = table2.get(tuple2);

            assertEquals(1, ((ClientTupleBuilder)tuple2).schema().version());
            assertEquals(2, ((ClientTupleBuilder)resTuple).schema().version());

            assertEquals(DEFAULT_NAME, resTuple.stringValue("name"));
            assertEquals(DEFAULT_ID, resTuple.longValue("id"));
        }
    }

    @Test
    public void testInsert() {
        Table table = getDefaultTable();

        var tuple = getDefaultTuple(table);
        var tuple2 = table.tupleBuilder()
                .set("id", DEFAULT_ID)
                .set("name", "abc")
                .build();

        assertTrue(table.insert(tuple));
        assertFalse(table.insert(tuple));
        assertFalse(table.insert(tuple2));

        var resTuple = table.get(getDefaultTupleKey(table));
        assertTupleEquals(tuple, resTuple);
    }

    @Test
    public void testInsertCustomTuple() {
        Table table = getDefaultTable();
        var tuple = new CustomTuple(25L, "Foo");

        assertTrue(table.insert(tuple));
        assertFalse(table.insert(tuple));

        var resTuple = table.get(new CustomTuple(25L, null));

        assertTupleEquals(tuple, resTuple);
    }

    private Tuple getDefaultTuple(Table table) {
        return table.tupleBuilder()
                .set("id", DEFAULT_ID)
                .set("name", DEFAULT_NAME)
                .build();
    }

    private Tuple getDefaultTupleKey(Table table) {
        return table.tupleBuilder()
                .set("id", DEFAULT_ID)
                .build();
    }

    private Table getDefaultTable() {
        server.tables().getOrCreateTable(DEFAULT_TABLE, tbl -> tbl.changeReplicas(1));

        return client.tables().table(DEFAULT_TABLE);
    }
}
