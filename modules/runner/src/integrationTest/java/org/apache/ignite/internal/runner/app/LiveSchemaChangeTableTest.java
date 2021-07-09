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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.internal.schema.SchemaType;
import org.apache.ignite.internal.table.ColumnNotFoundException;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Live schema tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14581")
class LiveSchemaChangeTableTest extends AbstractSchemaChangeTest {
    /**
     * Check live schema add columns
     */
    @Test
    public void testLiveSchemaAddColumns() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaType.LIVE_SCHEMA);

        Tuple row = tbl.tupleBuilder().set("key", 1L).set("valStrNew", "111").set("valIntNew", 333).build();

        tbl.insert(row);

        Tuple res = tbl.get(row);

        assertEquals("111", res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));

        Tuple secondRow = tbl.tupleBuilder().set("key", 2L).set("valStrNew", "222").set("valIntNew", 42).build();

        tbl.insert(secondRow);

        Tuple res2 = tbl.get(secondRow);

        assertEquals("222", res2.value("valStrNew"));
        assertEquals(Integer.valueOf(42), res2.value("valIntNew"));
    }

    /**
     * Check live schema kvBinaryView add columns
     */
    @Test
    public void testLiveSchemaAddColumnsKVBinaryView() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaType.LIVE_SCHEMA);

        KeyValueBinaryView kvBinaryView = tbl.kvView();

        Tuple key = kvBinaryView.tupleBuilder().set("key", 1L).build();
        Tuple val = kvBinaryView.tupleBuilder().set("valStrNew", "111").set("valIntNew", 333).build();

        kvBinaryView.put(key, val);

        Tuple key2 = kvBinaryView.tupleBuilder().set("key", 2L).build();
        Tuple val2 = kvBinaryView.tupleBuilder().set("valStrNew", "222").set("valIntNew", 42).build();

        kvBinaryView.put(key2, val2);

        Tuple res = kvBinaryView.get(key);
        assertEquals("111", res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));


        Tuple res2 = kvBinaryView.get(key2);

        assertEquals("222", res2.value("valStrNew"));
        assertEquals(Integer.valueOf(42), res2.value("valIntNew"));
    }

    /**
     * Check live schema tuple can handle different value types
     */
    @Test
    public void testLiveSchemaDifferentColumnTypes() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaType.LIVE_SCHEMA);

        UUID uuid = UUID.randomUUID();

        Tuple row = tbl.tupleBuilder()
            .set("key", 1L)
            .set("valByteNew", (byte)10)
            .set("valShortNew", (short)48)
            .set("valIntNew", 333)
            .set("valLongNew", 55L)
            .set("valFloatNew", 32.23f)
            .set("valDoubleNew", 100.101d)
            .set("valStrNew", "111")
            .set("valUUIDNew", uuid)
            .build();

        tbl.insert(row);

        Tuple res = tbl.get(row);

        assertEquals(Byte.valueOf((byte)10), res.value("valByteNew"));
        assertEquals(Short.valueOf((short)48), res.value("valShortNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));
        assertEquals(Long.valueOf(55L), res.value("valLongNew"));
        assertEquals(32.23f, res.value("valFloatNew"), 0.001f);
        assertEquals(100.101d, res.value("valDoubleNew"), 0.001f);

        assertEquals("111", res.value("valStrNew"));
        assertEquals(uuid, res.value("valUUIDNew"));
    }


    /**
     * Check live schema tuple can handle unsupported values and null`s correctly
     */
    @Test
    public void testLiveSchemaNullAndUnsupportedTypes() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(1).tables().table(TABLE);

        ((TableImpl)tbl).schemaType(SchemaType.LIVE_SCHEMA);

        assertDoesNotThrow(() -> tbl.tupleBuilder().set("newBrokenColumn", new Object()));
        assertThrows(UnsupportedOperationException.class, () -> tbl.tupleBuilder().set("newBrokenColumn", new Object()).build());

        Tuple row = tbl.tupleBuilder().set("key", 1L).set("valStrNew", null).set("valIntNew", 333).build();

        tbl.insert(row);

        Tuple res = tbl.get(tbl.tupleBuilder().set("key", 1L).build());

        assertThrows(ColumnNotFoundException.class, () -> res.value("valStrNew"));
        assertEquals(Integer.valueOf(333), res.value("valIntNew"));
    }
}
