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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/** Tests for the {@link BinaryTupleSchema} class. */
public class BinaryTupleSchemaTest {

    @Test
    public void rowSchema() {
        SchemaDescriptor schema = new SchemaDescriptor(1, Arrays.asList(
                new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C4".toUpperCase(Locale.ROOT), NativeTypes.INT32, false)
        ), List.of("C2", "C4"), null);

        BinaryTupleSchema rowSchema = BinaryTupleSchema.createRowSchema(schema);
        assertEquals(0, rowSchema.columnIndex(0));
        assertEquals(1, rowSchema.columnIndex(1));
        assertTrue(rowSchema.convertible());
    }

    @Test
    public void keySchema() {
        {
            SchemaDescriptor schema = new SchemaDescriptor(1, Arrays.asList(
                    new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT8, false),
                    new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT16, false),
                    new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                    new Column("C4".toUpperCase(Locale.ROOT), NativeTypes.INT64, false)
            ), List.of("C2", "C4"), null);

            BinaryTupleSchema keySchema = BinaryTupleSchema.createKeySchema(schema);

            assertEquals(0, keySchema.columnIndex(0));
            assertEquals(1, keySchema.columnIndex(1));
            assertTrue(keySchema.convertible());

            expectColumnMatch(schema, keySchema, 1, 0);
            expectColumnMatch(schema, keySchema, 3, 1);
        }

        {
            SchemaDescriptor schema = new SchemaDescriptor(1, Arrays.asList(
                    new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT8, false),
                    new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT16, false),
                    new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                    new Column("C4".toUpperCase(Locale.ROOT), NativeTypes.INT64, false)
            ), List.of("C4", "C2"), null);

            BinaryTupleSchema keySchema = BinaryTupleSchema.createKeySchema(schema);

            assertEquals(0, keySchema.columnIndex(0));
            assertEquals(1, keySchema.columnIndex(1));
            assertTrue(keySchema.convertible());

            expectColumnMatch(schema, keySchema, 3, 0);
            expectColumnMatch(schema, keySchema, 1, 1);
        }
    }

    @Test
    public void valueSchema() {
        SchemaDescriptor schema = new SchemaDescriptor(1, Arrays.asList(
                new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT8, false),
                new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT16, false)
        ), List.of("C1"), null);

        BinaryTupleSchema valueSchema = BinaryTupleSchema.createValueSchema(schema);

        assertEquals(1, valueSchema.columnIndex(0));
        assertFalse(valueSchema.convertible());
    }

    @Test
    public void destinationKeySchema() {
        SchemaDescriptor schema = new SchemaDescriptor(1, Arrays.asList(
                new Column("C1".toUpperCase(Locale.ROOT), NativeTypes.INT8, false),
                new Column("C2".toUpperCase(Locale.ROOT), NativeTypes.INT16, false),
                new Column("C3".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("C4".toUpperCase(Locale.ROOT), NativeTypes.INT64, false)
        ), List.of("C2", "C4"), null);

        BinaryTupleSchema dstSchema = BinaryTupleSchema.createDestinationKeySchema(schema);

        assertEquals(1, dstSchema.columnIndex(0));
        assertEquals(3, dstSchema.columnIndex(1));
        assertFalse(dstSchema.convertible());
    }

    private static void expectColumnMatch(SchemaDescriptor schema, BinaryTupleSchema tupleSchema, int schemaIdx, int tupleIdx) {
        Column column = schema.column(schemaIdx);
        Element elem = tupleSchema.element(tupleIdx);

        String message = format("schema field: {}, tuple field: {}", schemaIdx, tupleIdx);
        assertSame(column.type().spec(), elem.typeSpec, message);
    }
}
