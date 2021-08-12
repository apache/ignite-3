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

package org.apache.ignite.internal.runner.app;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.ColumnNotFoundException;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live schema tests for KV View.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14581")
class LiveSchemaChangeKVViewTest extends AbstractSchemaChangeTest {
    /**
     * Check exception for unknown column when STRICT_SCHEMA is enabled.
     */
    @Test
    public void testStrictSchemaInsertRowOfNewSchema() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        KeyValueBinaryView view = grid.get(1).tables().table(TABLE).kvView();

        assertThrows(ColumnNotFoundException.class, () -> view.tupleBuilder().set("key", 1L).set("unknownColumn", 10).build());
    }

    /**
     * Check live schema kvBinaryView add columns.
     */
    @Test
    public void testLiveSchemaAddColumns() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        KeyValueBinaryView kvBinaryView = tbl.kvView();

        Tuple key = kvBinaryView.tupleBuilder().set("key", 1L).build();
        Tuple val = kvBinaryView.tupleBuilder().set("valStrNew", "111").set("valIntNew", 333).build();

        kvBinaryView.put(key, val);

        Tuple res = kvBinaryView.get(key);
        assertEquals("111", res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));
    }

    /**
     * Check strict schema works correctly after live schema
     */
    @Test
    public void testLiveSchemaAddColumnsSwitchToStrict() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        KeyValueBinaryView kvBinaryView = tbl.kvView();

        Tuple key = kvBinaryView.tupleBuilder().set("key", 1L).build();
        Tuple val = kvBinaryView.tupleBuilder().set("valStrNew", "111").set("valIntNew", 333).build();

        kvBinaryView.put(key, val);

        Tuple res = kvBinaryView.get(key);
        assertEquals("111", res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));


        ((TableImpl)tbl).schemaType(SchemaMode.STRICT_SCHEMA);

        Tuple anotherKey = kvBinaryView.tupleBuilder().set("key", 2L).build();
        Tuple anotherVal = kvBinaryView.tupleBuilder().set("valStrNew", "111").set("valIntNew", 333).build();

        kvBinaryView.put(anotherKey, anotherVal);

        Tuple newRes = kvBinaryView.get(anotherKey);

        assertEquals("111", newRes.value("valStrNew"));
        assertEquals(Integer.valueOf(333), newRes.value("valIntNew"));

        assertThrows(ColumnNotFoundException.class, () -> kvBinaryView.tupleBuilder().set("key", 1L).set("unknownColumn", 10).build());
    }

    /**
     * Check upsert row of old schema with row of new schema.
     */
    @Test
    public void testLiveSchemaUpsertOldSchemaRow() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        KeyValueBinaryView view = tbl.kvView();

        Tuple oldSchemaKey = view.tupleBuilder().set("key", 32L).build();
        Tuple oldSchemaVal = view.tupleBuilder().set("valInt", 111).set("valStr", "str").build();

        view.put(oldSchemaKey, oldSchemaVal);

        Tuple upsertOldSchemaVal = view.tupleBuilder().set("valStrNew", "111").set("valIntNew", 333).build();

        view.put(oldSchemaKey, upsertOldSchemaVal);

        Tuple oldSchemaRes = view.get(oldSchemaKey);

        assertEquals("111", oldSchemaRes.value("valStrNew"));
        assertEquals(Integer.valueOf(333), oldSchemaRes.value("valIntNew"));
    }

    /**
     * Check upsert row of old schema with row of new schema.
     */
    @Test
    public void testLiveSchemaUpsertSchemaTwice() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        KeyValueBinaryView view = tbl.kvView();

        Tuple oldSchemaKey = view.tupleBuilder().set("key", 32L).build();

        Tuple oldSchemaVal = view.tupleBuilder().set("valInt", 111).set("valStr", "str").build();
        Tuple upsertOldSchemaVal = view.tupleBuilder().set("valStrNew", "111").set("valIntNew", 333).build();
        Tuple secondUpsertOldSchemaVal = view.tupleBuilder().set("valStrNew", "111").set("valIntNew", 333).set("anotherNewVal", 48L).build();

        view.put(oldSchemaKey, oldSchemaVal);
        view.put(oldSchemaKey, upsertOldSchemaVal);
        view.put(oldSchemaKey, secondUpsertOldSchemaVal);

        Tuple oldSchemaRes = view.get(oldSchemaKey);

        assertEquals("111", oldSchemaRes.value("valStrNew"));
        assertEquals(Integer.valueOf(333), oldSchemaRes.value("valIntNew"));
        assertEquals(Long.valueOf(48L), oldSchemaRes.value("anotherNewVal"));
    }

    /**
     * Check inserting row of old schema will not lead to column removal.
     */
    @Test
    public void testLiveSchemaInsertOldSchemaRow() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaMode.LIVE_SCHEMA);

        KeyValueBinaryView view = tbl.kvView();

        Tuple oldSchemaKey = view.tupleBuilder().set("key", 32L).build();
        Tuple oldSchemaVal = view.tupleBuilder().set("valInt", 111).set("valStr", "str").build();

        Tuple newSchemaKey = view.tupleBuilder().set("key", 1L).build();
        Tuple newSchemaVal = view.tupleBuilder().set("valStrNew", "111").set("valIntNew", 333).build();

        view.put(newSchemaKey, newSchemaVal);
        view.put(oldSchemaKey, oldSchemaVal);

        Tuple res = view.get(newSchemaKey);

        assertEquals("111", res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));

        TupleBuilder newVerBuilder = tbl.tupleBuilder();

        SchemaDescriptor schema = ((SchemaAware)newVerBuilder).schema();

        assertTrue(schema.columnNames().contains("valStrNew"));
        assertTrue(schema.columnNames().contains("valIntNew"));
    }
}
