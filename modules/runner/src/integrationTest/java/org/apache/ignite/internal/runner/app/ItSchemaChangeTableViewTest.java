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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Ignition interface tests.
 */
class ItSchemaChangeTableViewTest extends AbstractSchemaChangeTest {
    /**
     * Check add a new column to table schema.
     */
    @Test
    public void testDropColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111).set("valStr", "str"), null);

        dropColumn(grid, "valStr");

        // Check old row conversion.
        final Tuple keyTuple = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl.get(keyTuple, null).value("key"));
        assertEquals(111, (Integer) tbl.get(keyTuple, null).value("valInt"));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple, null).value("valStr"));

        // Check tuple of outdated schema.
        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(Tuple.create().set("key", 2L).set("valInt", -222).set("valStr", "str"), null)
        );

        // Check tuple of correct schema.
        tbl.insert(Tuple.create().set("key", 2L).set("valInt", 222), null);

        final Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long) tbl.get(keyTuple2, null).value("key"));
        assertEquals(222, (Integer) tbl.get(keyTuple2, null).value("valInt"));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple2, null).value("valStr"));
    }

    /**
     * Check drop column from table schema.
     */
    @Test
    public void testAddNewColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111), null);

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(Tuple.create().set("key", 1L).set("valInt", -111).set("valStrNew", "str"), null)
        );

        addColumn(grid, SchemaBuilders.column("valStrNew", ColumnType.string())
                .asNullable(true).withDefaultValueExpression("default").build());

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl.get(keyTuple1, null).value("key"));
        assertEquals(111, (Integer) tbl.get(keyTuple1, null).value("valInt"));
        assertEquals("default", tbl.get(keyTuple1, null).value("valStrNew"));

        // Check tuple of new schema.
        tbl.insert(Tuple.create().set("key", 2L).set("valInt", 222).set("valStrNew", "str"), null);

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long) tbl.get(keyTuple2, null).value("key"));
        assertEquals(222, (Integer) tbl.get(keyTuple2, null).value("valInt"));
        assertEquals("str", tbl.get(keyTuple2, null).value("valStrNew"));
    }

    /**
     * Check column renaming.
     */
    @Test
    void testRenameColumn() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111), null);

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(Tuple.create().set("key", 2L).set("valRenamed", -222), null)
        );

        renameColumn(grid, "valInt", "valRenamed");

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl.get(keyTuple1, null).value("key"));
        assertEquals(111, (Integer) tbl.get(keyTuple1, null).value("valRenamed"));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple1, null).value("valInt"));

        // Check tuple of outdated schema.
        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(Tuple.create().set("key", 2L).set("valInt", -222), null)
        );

        // Check tuple of correct schema.
        tbl.insert(Tuple.create().set("key", 2L).set("valRenamed", 222), null);

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long) tbl.get(keyTuple2, null).value("key"));
        assertEquals(222, (Integer) tbl.get(keyTuple2, null).value("valRenamed"));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(keyTuple2, null).value("valInt"));
    }

    /**
     * Rename column then add a new column with same name.
     */
    @Test
    void testRenameThenAddColumnWithSameName() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111), null);

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(Tuple.create().set("key", 2L).set("val2", -222), null)
        );

        renameColumn(grid, "valInt", "val2");
        addColumn(grid, SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true)
                .withDefaultValueExpression(-1).build());

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl.get(keyTuple1, null).value("key"));
        assertEquals(111, (Integer) tbl.get(keyTuple1, null).value("val2"));
        assertEquals(-1, (Integer) tbl.get(keyTuple1, null).value("valInt"));

        // Check tuple of outdated schema.
        assertNull(tbl.get(Tuple.create().set("key", 2L), null));

        // Check tuple of correct schema.
        tbl.insert(Tuple.create().set("key", 2L).set("val2", 222), null);

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long) tbl.get(keyTuple2, null).value("key"));
        assertEquals(222, (Integer) tbl.get(keyTuple2, null).value("val2"));
        assertEquals(-1, (Integer) tbl.get(keyTuple2, null).value("valInt"));
    }

    /**
     * Check merge table schema changes.
     */
    @Test
    public void testMergeChangesAddDropAdd() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        final ColumnDefinition column = SchemaBuilders.column("val", ColumnType.string()).asNullable(true)
                .withDefaultValueExpression("default")
                .build();

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111), null);

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(
                        Tuple.create().set("key", 2L).set("val", "I'not exists"),
                        null
                )
        );

        addColumn(grid, column);

        assertNull(tbl.get(Tuple.create().set("key", 2L), null));

        tbl.insert(Tuple.create().set("key", 2L).set("valInt", 222).set("val", "string"), null);

        tbl.insert(Tuple.create().set("key", 3L).set("valInt", 333), null);

        dropColumn(grid, column.name());

        tbl.insert(Tuple.create().set("key", 4L).set("valInt", 444), null);

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(
                        Tuple.create().set("key", 4L).set("val", "I'm not exist"),
                        null
                )
        );

        addColumn(grid, SchemaBuilders.column("val", ColumnType.string()).withDefaultValueExpression("default").build());

        tbl.insert(Tuple.create().set("key", 5L).set("valInt", 555), null);

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(111, (Integer) tbl.get(keyTuple1, null).value("valInt"));
        assertEquals("default", tbl.get(keyTuple1, null).value("val"));

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(222, (Integer) tbl.get(keyTuple2, null).value("valInt"));
        assertEquals("default", tbl.get(keyTuple2, null).value("val"));

        Tuple keyTuple3 = Tuple.create().set("key", 3L);

        assertEquals(333, (Integer) tbl.get(keyTuple3, null).value("valInt"));
        assertEquals("default", tbl.get(keyTuple3, null).value("val"));

        Tuple keyTuple4 = Tuple.create().set("key", 4L);

        assertEquals(444, (Integer) tbl.get(keyTuple4, null).value("valInt"));
        assertEquals("default", tbl.get(keyTuple4, null).value("val"));

        Tuple keyTuple5 = Tuple.create().set("key", 5L);

        assertEquals(555, (Integer) tbl.get(keyTuple5, null).value("valInt"));
        assertEquals("default", tbl.get(keyTuple5, null).value("val"));
    }

    /**
     * Check merge column default value changes.
     */
    @Test
    public void testMergeChangesColumnDefault() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        final String colName = "valStr";

        tbl.insert(Tuple.create().set("key", 1L).set("valInt", 111), null);

        changeDefault(grid, colName, (Supplier<Object> & Serializable) () -> "newDefault");
        addColumn(grid, SchemaBuilders.column("val", ColumnType.string()).withDefaultValueExpression("newDefault").build());

        tbl.insert(Tuple.create().set("key", 2L).set("valInt", 222), null);

        changeDefault(grid, colName, (Supplier<Object> & Serializable) () -> "brandNewDefault");
        changeDefault(grid, "val", (Supplier<Object> & Serializable) () -> "brandNewDefault");

        tbl.insert(Tuple.create().set("key", 3L).set("valInt", 333), null);

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(111, (Integer) tbl.get(keyTuple1, null).value("valInt"));
        assertEquals("default", tbl.get(keyTuple1, null).value("valStr"));
        assertEquals("newDefault", tbl.get(keyTuple1, null).value("val"));

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(222, (Integer) tbl.get(keyTuple2, null).value("valInt"));
        assertEquals("newDefault", tbl.get(keyTuple2, null).value("valStr"));
        assertEquals("newDefault", tbl.get(keyTuple2, null).value("val"));

        Tuple keyTuple3 = Tuple.create().set("key", 3L);

        assertEquals(333, (Integer) tbl.get(keyTuple3, null).value("valInt"));
        assertEquals("brandNewDefault", tbl.get(keyTuple3, null).value("valStr"));
        assertEquals("brandNewDefault", tbl.get(keyTuple3, null).value("val"));
    }

    /**
     * Check operation failed if unknown column found.
     */
    @Test
    public void testStrictSchemaInsertRowOfNewSchema() {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        Tuple tuple = Tuple.create().set("key", 1L).set("unknownColumn", 10);

        assertThrowsWithCause(SchemaMismatchException.class, () -> tbl.recordView().insert(tuple, null));
    }
}
