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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.table.ColumnNotFoundException;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Check data by strict schema.
 * <p>
 */
public class StrictSchemaOperationsTest {
    /**
     *
     */
    @Test
    public void testColumnNotExist() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {new Column("val", NativeType.LONG, false)}
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        assertThrows(ColumnNotFoundException.class, () -> tbl.tupleBuilder().set("invalidCol", 0));
    }

    /**
     *
     */
    @Test
    public void testTypeMismatch() {
        SchemaDescriptor schema = new SchemaDescriptor(
            1,
            new Column[] {new Column("id", NativeType.LONG, false)},
            new Column[] {
                new Column("valString", NativeType.fromColumnType(ColumnType.stringOf(3)), true),
                new Column("valBytes", NativeType.fromColumnType(ColumnType.blobOf(3)), true)
            }
        );

        Table tbl = new TableImpl(new DummyInternalTableImpl(), new DummySchemaManagerImpl(schema));

        // Check not-nullable column.
        assertThrows(IllegalArgumentException.class, () -> tbl.tupleBuilder().set("id", null));

        // Check length of the string column
        assertThrows(InvalidTypeException.class, () -> tbl.tupleBuilder().set("valString", "qweqwe"));

        // Check length of the string column
        assertThrows(InvalidTypeException.class, () -> tbl.tupleBuilder().set("valBytes", new byte[] {0, 1, 2, 3}));
    }
}
