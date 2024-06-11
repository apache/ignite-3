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

package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Checks if data compliant with the schema, otherwise the correct exception is thrown.
 */
public class SchemaValidationTest extends TableKvOperationsTestBase {

    @Test
    public void columnNotExist() {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("ID", NativeTypes.INT64, false)},
                new Column[]{new Column("VAL", NativeTypes.INT64, true)}
        );

        RecordView<Tuple> recView = createTable(schema).recordView();

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> recView.insert(null, Tuple.create().set("id", 0L).set("invalidCol", 0)));
    }

    @Test
    public void schemaMismatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{
                        new Column("ID", NativeTypes.INT64, false),
                        new Column("AFFID", NativeTypes.INT64, false)
                },
                new Column[]{new Column("VAL", NativeTypes.INT64, true)}
        );

        Table tbl = createTable(schema);

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.recordView().get(null, Tuple.create().set("id", 0L).set("affId", 1L).set("val", 0L)));
        assertThrowsWithCause(SchemaMismatchException.class, () -> tbl.recordView().get(null, Tuple.create().set("id", 0L)));

        assertThrowsWithCause(SchemaMismatchException.class, () -> tbl.keyValueView().get(null, Tuple.create().set("id", 0L)));
        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.keyValueView().get(null, Tuple.create().set("id", 0L).set("affId", 1L).set("val", 0L)));

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.keyValueView().put(null, Tuple.create().set("id", 0L), Tuple.create()));

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.keyValueView().put(null, Tuple.create().set("id", 0L).set("affId", 1L).set("val", 0L), Tuple.create()));

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.keyValueView().put(
                        null,
                        Tuple.create().set("id", 0L).set("affId", 1L),
                        Tuple.create().set("id", 0L).set("val", 0L))
        );
    }

    @Test
    public void typeMismatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("ID", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("VALSTRING", NativeTypes.stringOf(3), true),
                        new Column("VALBYTES", NativeTypes.blobOf(3), true)
                }
        );

        RecordView<Tuple> tbl = createTable(schema).recordView();

        // Check not-nullable column.
        assertThrowsWithCause(SchemaMismatchException.class, () -> tbl.insert(null, Tuple.create().set("id", null)));

        // Check length of the string column
        assertThrowsWithCause(InvalidTypeException.class,
                () -> tbl.insert(null, Tuple.create().set("id", 0L).set("valString", "qweqwe")));

        // Check length of the string column
        assertThrowsWithCause(InvalidTypeException.class,
                () -> tbl.insert(null, Tuple.create().set("id", 0L).set("valBytes", new byte[]{0, 1, 2, 3})));
    }

    @Test
    public void stringTypeMatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("ID", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("VALSTRING", NativeTypes.stringOf(3), true)
                }
        );

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple tuple = Tuple.create().set("id", 1L);

        tbl.insert(null, tuple.set("valString", "qwe"));
        tbl.insert(null, tuple.set("valString", "qw"));
        tbl.insert(null, tuple.set("valString", "q"));
        tbl.insert(null, tuple.set("valString", ""));
        tbl.insert(null, tuple.set("valString", null));

        // Check string 3 char length and 9 bytes.
        tbl.insert(null, tuple.set("valString", "我是谁"));
    }

    @Test
    public void bytesTypeMatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("ID", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("VALUNLIMITED", NativeTypes.BYTES, true),
                        new Column("VALLIMITED", NativeTypes.blobOf(2), true)
                });

        RecordView<Tuple> tbl = createTable(schema).recordView();

        Tuple tuple = Tuple.create().set("id", 1L);

        tbl.insert(null, tuple.set("valUnlimited", null));
        tbl.insert(null, tuple.set("valLimited", null));
        tbl.insert(null, tuple.set("valUnlimited", new byte[2]));
        tbl.insert(null, tuple.set("valLimited", new byte[2]));
        tbl.insert(null, tuple.set("valUnlimited", new byte[3]));

        assertThrowsWithCause(InvalidTypeException.class, () -> tbl.insert(null, tuple.set("valLimited", new byte[3])));

    }

    private <T extends Throwable> void assertThrowsWithCause(Class<T> expectedType, Executable executable) {
        Throwable ex = assertThrows(IgniteException.class, executable);

        while (ex.getCause() != null) {
            if (expectedType.isInstance(ex.getCause())) {
                return;
            }

            ex = ex.getCause();
        }

        fail("Expected cause wasn't found.");
    }
}
