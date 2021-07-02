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
import org.apache.ignite.app.Ignite;
import org.apache.ignite.internal.table.ColumnNotFoundException;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Ignition interface tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14581")
class SchemaChangeTableViewTest extends AbstractSchemaChangeTest {
    /**
     * Check add a new column to table schema.
     */
    @Test
    public void testDropColumn() {
        List<Ignite> grid = startGrid();

        SchemaTable schema = createTable(grid);

        {
            final Table tbl = grid.get(1).tables().table(TABLE);

            tbl.insert(tbl.tupleBuilder().set("key", 1L).set("valInt", 111).set("colForDrop", "str").build());
        }

        dropColumn(grid, schema, "colForDrop");

        {
            Table tbl = grid.get(2).tables().table(TABLE);

            // Check old row conversion.
            final Tuple keyTuple = tbl.tupleBuilder().set("key", 1L).build();

            assertEquals(1, (Long) tbl.get(keyTuple).value("key"));
            assertEquals(111, (Integer) tbl.get(keyTuple).value("valInt"));
            assertThrows(ColumnNotFoundException.class, () -> tbl.get(keyTuple).value("val2"));

            // Check tuple of outdated schema.
            assertThrows(ColumnNotFoundException.class,
                    () -> tbl.insert(tbl.tupleBuilder().set("key", 2L).set("valInt", -222).set("val2", "str").build())
            );

            // Check tuple of correct schema.
            tbl.insert(tbl.tupleBuilder().set("key", 2L).set("valInt", 222).build());

            final Tuple keyTuple2 = tbl.tupleBuilder().set("key", 2L).build();

            assertEquals(2, (Long) tbl.get(keyTuple2).value("key"));
            assertEquals(222, (Integer) tbl.get(keyTuple2).value("valInt"));
            assertThrows(ColumnNotFoundException.class, () -> tbl.get(keyTuple2).value("val2"));
        }
    }

    /**
     * Check drop column from table schema.
     */
    @Test
    public void testAddNewColumn() {
        List<Ignite> grid = startGrid();

        SchemaTable schTbl1 = createTable(grid);

        {
            Table tbl = grid.get(1).tables().table(TABLE);

            tbl.insert(tbl.tupleBuilder().set("key", 1L).set("valInt", 111).build());

            assertThrows(ColumnNotFoundException.class,
                    () -> tbl.insert(tbl.tupleBuilder().set("key", 1L).set("valInt", -111).set("val2", "str").build())
            );
        }

        addColumn(
                grid,
                schTbl1,
                SchemaBuilders.column("val2", ColumnType.string()).asNullable().withDefaultValue("default").build()
        );

        Table tbl = grid.get(2).tables().table(TABLE);

        // Check old row conversion.
        Tuple keyTuple1 = tbl.tupleBuilder().set("key", 1L).build();

        assertEquals(1, (Long) tbl.get(keyTuple1).value("key"));
        assertEquals(111, (Integer) tbl.get(keyTuple1).value("valInt"));
        assertEquals("default", tbl.get(keyTuple1).value("val2"));

        // Check tuple of new schema.
        tbl.insert(tbl.tupleBuilder().set("key", 2L).set("valInt", 222).set("val2", "str").build());

        Tuple keyTuple2 = tbl.tupleBuilder().set("key", 2L).build();

        assertEquals(2, (Long) tbl.get(keyTuple2).value("key"));
        assertEquals(222, (Integer) tbl.get(keyTuple2).value("valInt"));
        assertEquals("str", tbl.get(keyTuple2).value("val2"));
    }

    /**
     * Check rename column from table schema.
     */
    @Test
    void testRenameColumn() {
        List<Ignite> grid = startGrid();

        // Create table on node 0.
        SchemaTable schTbl1 = createTable(grid);

        {
            Table tbl = grid.get(1).tables().table(TABLE);

            tbl.insert(tbl.tupleBuilder().set("key", 1L).set("valInt", 111).build());

            assertThrows(ColumnNotFoundException.class,
                    () -> tbl.insert(tbl.tupleBuilder().set("key", 2L).set("val2", -222).build())
            );
        }

        renameColumn(grid, schTbl1, "valInt", "val2");

        {
            Table tbl = grid.get(2).tables().table(TABLE);

            // Check old row conversion.
            Tuple keyTuple1 = tbl.tupleBuilder().set("key", 1L).build();

            assertEquals(1, (Long) tbl.get(keyTuple1).value("key"));
            assertEquals(111, (Integer) tbl.get(keyTuple1).value("val2"));
            assertThrows(ColumnNotFoundException.class, () -> tbl.get(keyTuple1).value("valInt"));

            // Check tuple of outdated schema.
            assertThrows(ColumnNotFoundException.class,
                    () -> tbl.insert(tbl.tupleBuilder().set("key", 2L).set("valInt", -222).build())
            );

            // Check tuple of correct schema.
            tbl.insert(tbl.tupleBuilder().set("key", 2L).set("val2", 222).build());

            Tuple keyTuple2 = tbl.tupleBuilder().set("key", 2L).build();

            assertEquals(2, (Long) tbl.get(keyTuple2).value("key"));
            assertEquals(222, (Integer) tbl.get(keyTuple2).value("val2"));
            assertThrows(ColumnNotFoundException.class, () -> tbl.get(keyTuple2).value("valInt"));
        }
    }

    /**
     * Check merge table schema changes.
     */
    @Test
    public void testMergeChangesAddDropAdd() {
        List<Ignite> grid = startGrid();

        SchemaTable schema = createTable(grid);

        final Column column = SchemaBuilders.column("val", ColumnType.string()).asNullable().withDefaultValue("default").build();

        Table tbl = grid.get(2).tables().table(TABLE);

        {
            tbl.insert(tbl.tupleBuilder().set("key", 1L).set("valInt", 111).build());

            assertThrows(ColumnNotFoundException.class, () -> tbl.insert(
                    tbl.tupleBuilder().set("key", 2L).set("val", "I'not exists").build())
            );
        }

        addColumn(grid, schema, column);

        {
            assertNull(tbl.get(tbl.tupleBuilder().set("key", 2L).build()));

            tbl.insert(tbl.tupleBuilder().set("key", 2L).set("valInt", 222).set("val", "string").build());

            tbl.insert(tbl.tupleBuilder().set("key", 3L).set("valInt", 3).build());
        }

        dropColumn(grid, schema, column.name());

        {
            tbl.insert(tbl.tupleBuilder().set("key", 4L).set("valInt", 4).build());

            assertThrows(ColumnNotFoundException.class, () -> tbl.insert(
                    tbl.tupleBuilder().set("key", 4L).set("val", "I'm not exist").build())
            );
        }

        addColumn(grid, schema, SchemaBuilders.column("val", ColumnType.string()).asNullable().withDefaultValue("default").build());

        {
            tbl.insert(tbl.tupleBuilder().set("key", 5L).set("valInt", 555).build());

            // Check old row conversion.
            Tuple keyTuple1 = tbl.tupleBuilder().set("key", 1L).build();

            assertEquals(111, (Integer) tbl.get(keyTuple1).value("valInt"));
            assertEquals("default", tbl.get(keyTuple1).value("val"));

            Tuple keyTuple2 = tbl.tupleBuilder().set("key", 2L).build();

            assertEquals(222, (Integer) tbl.get(keyTuple2).value("valInt"));
            assertEquals("default", tbl.get(keyTuple2).value("val"));

            Tuple keyTuple3 = tbl.tupleBuilder().set("key", 3L).build();

            assertEquals(333, (Integer) tbl.get(keyTuple3).value("valInt"));
            assertEquals("default", tbl.get(keyTuple3).value("val"));

            Tuple keyTuple4 = tbl.tupleBuilder().set("key", 4L).build();

            assertEquals(444, (Integer) tbl.get(keyTuple4).value("valInt"));
            assertEquals("default", tbl.get(keyTuple4).value("val"));

            Tuple keyTuple5 = tbl.tupleBuilder().set("key", 5L).build();

            assertEquals(555, (Integer) tbl.get(keyTuple5).value("valInt"));
            assertEquals("default", tbl.get(keyTuple5).value("val"));
        }
    }
}
