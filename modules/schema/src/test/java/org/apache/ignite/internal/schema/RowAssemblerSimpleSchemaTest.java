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

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.junit.jupiter.api.Test;

/**
 * Validate row layout for different schema configurations.
 */
public class RowAssemblerSimpleSchemaTest {
    /** Uuid test value. */
    public final UUID uuidVal = new UUID(-5204230847775358097L, 4916207022290092939L);

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len nullable value.
     */
    @Test
    public void fixedKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("keyIntCol", INT32, false)},
                new Column[]{new Column("valIntCol", INT32, true)});

        assertRowBytesEquals(
                new byte[]{42, 0, -4, -111, -102, -5, 9, 0, 0, 0, 0, 33, 0, 0, 0, 10, 0, 0, 0, 0, 0, -71, -1, -1, -1},
                new RowAssembler(schema, 0, 0)
                        .appendInt(33)
                        .appendInt(-71)
                        .toBytes());

        // Null value.
        assertRowBytesEquals(
                new byte[]{42, 0, -4, -111, -102, -5, 9, 0, 0, 0, 0, 33, 0, 0, 0, 6, 0, 0, 0, 0, 1},
                new RowAssembler(schema, 0, 0)
                        .appendInt(33)
                        .appendNull()
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{0, 0, -4, -111, -102, -5, 9, 0, 0, 0, 0, 33, 0, 0, 0},
                new RowAssembler(schema, 0, 0)
                        .appendInt(33)
                        .toBytes());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len non-null value.
     */
    @Test
    public void fixedKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("keyShortCol", INT16, false)},
                new Column[]{new Column("valShortCol", INT16, false)});

        // With value.
        assertRowBytesEquals(
                new byte[]{42, 0, -45, 61, -70, -41, 7, 0, 0, 0, 0, 33, 0, 7, 0, 0, 0, 0, -71, -1},
                new RowAssembler(schema, 0, 0)
                        .appendShort((short) 33)
                        .appendShort((short) -71)
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{0, 0, -45, 61, -70, -41, 7, 0, 0, 0, 0, 33, 0},
                new RowAssembler(schema, 0, 0)
                        .appendShort((short) 33)
                        .toBytes());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len nullable value.
     */
    @Test
    public void fixedKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("keyShortCol", INT16, false)},
                new Column[]{new Column("valStrCol", STRING, true)});

        assertRowBytesEquals(
                new byte[]{42, 0, -23, -36, 114, 80, 7, 0, 0, 0, 0, -33, -1, 9, 0, 0, 0, 0, 0, 118, 97, 108},
                new RowAssembler(schema, 0, 1)
                        .appendShort((short) -33)
                        .appendString("val")
                        .toBytes());

        // Null value.
        assertRowBytesEquals(
                new byte[]{42, 0, -45, 61, -70, -41, 7, 0, 0, 0, 0, 33, 0, 6, 0, 0, 0, 0, 1},
                new RowAssembler(schema, 0, 0)
                        .appendShort((short) 33)
                        .appendNull()
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{0, 0, -45, 61, -70, -41, 7, 0, 0, 0, 0, 33, 0},
                new RowAssembler(schema, 0, 0)
                        .appendShort((short) 33)
                        .toBytes());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len non-null value.
     */
    @Test
    public void fixedKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("keyShortCol", INT16, false)},
                new Column[]{new Column("valStrCol", STRING, false)});

        assertRowBytesEquals(
                new byte[]{42, 0, -23, -36, 114, 80, 7, 0, 0, 0, 0, -33, -1, 8, 0, 0, 0, 0, 118, 97, 108},
                new RowAssembler(schema, 0, 1)
                        .appendShort((short) -33)
                        .appendString("val")
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{0, 0, -45, 61, -70, -41, 7, 0, 0, 0, 0, 33, 0},
                new RowAssembler(schema, 0, 0)
                        .appendShort((short) 33)
                        .toBytes());
    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len nullable value.
     */
    @Test
    public void varlenKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("keyStrCol", STRING, false)},
                new Column[]{new Column("valUuidCol", UUID, true)});

        assertRowBytesEquals(
                new byte[]{
                        42, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121,
                        22, 0, 0, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73},
                new RowAssembler(schema, 1, 0)
                        .appendString("key")
                        .appendUuid(uuidVal)
                        .toBytes());

        // Null value.
        assertRowBytesEquals(
                new byte[]{42, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121,
                        6, 0, 0, 0, 0, 1},
                new RowAssembler(schema, 1, 0)
                        .appendString("key")
                        .appendNull()
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{0, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121},
                new RowAssembler(schema, 1, 0)
                        .appendString("key")
                        .toBytes());
    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len non-null value.
     */
    @Test
    public void varlenKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("keyStrCol", STRING, false)},
                new Column[]{new Column("valUuidCol", UUID, false)});

        assertRowBytesEquals(
                new byte[]{
                        42, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121,
                        21, 0, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73},
                new RowAssembler(schema, 1, 0)
                        .appendString("key")
                        .appendUuid(uuidVal)
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{0, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121},
                new RowAssembler(schema, 1, 0)
                        .appendString("key")
                        .toBytes());
    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len nullable value.
     */
    @Test
    public void varlenKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("keyStrCol", STRING, false)},
                new Column[]{new Column("valBytesCol", BYTES, true)});

        System.out.println(Integer.toHexString(((byte) -116) & 0xFF));
        System.out.println(Integer.toHexString(((byte) -99) & 0xFF));

        assertRowBytesEquals(
                new byte[]{42, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121, 10, 0, 0, 0, 0, 0, -1, 1, 0, 120},
                new RowAssembler(schema, 1, 1)
                        .appendString("key")
                        .appendBytes(new byte[]{-1, 1, 0, 120})
                        .toBytes());

        // Null value.
        assertRowBytesEquals(
                new byte[]{42, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121, 6, 0, 0, 0, 0, 1},
                new RowAssembler(schema, 1, 0)
                        .appendString("key")
                        .appendNull()
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{0, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121},
                new RowAssembler(schema, 1, 0)
                        .appendString("key")
                        .toBytes());
    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len non-null value.
     */
    @Test
    public void varlenKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{new Column("keyStrCol", STRING, false)},
                new Column[]{new Column("valBytesCol", BYTES, false)});

        assertRowBytesEquals(
                new byte[]{42, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121, 9, 0, 0, 0, 0, -1, 1, 0, 120},
                new RowAssembler(schema, 1, 1)
                        .appendString("key")
                        .appendBytes(new byte[]{-1, 1, 0, 120})
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{0, 0, 64, -119, 105, 64, 8, 0, 0, 0, 0, 107, 101, 121},
                new RowAssembler(schema, 1, 0)
                        .appendString("key")
                        .toBytes());
    }

    /**
     * Validate row layout for key\value columns of different types.
     */
    @Test
    public void mixedTypes() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{
                        new Column("keyShortCol", INT16, false),
                        new Column("keyStrCol", STRING, false)
                },
                new Column[]{
                        new Column("valIntCol", INT32, true),
                        new Column("valStrCol", STRING, true)
                });

        assertRowBytesEquals(
                new byte[]{
                        42, 0, -93, 60, -78, -36,
                        13, 0, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114,
                        16, 0, 0, 0, 0, 0, 73, 0, 0, 0, 118, 97, 108, 115, 116, 114},
                new RowAssembler(schema, 1, 1)
                        .appendShort((short) 33)
                        .appendString("keystr")
                        .appendInt(73)
                        .appendString("valstr")
                        .toBytes());

        // Null value.
        assertRowBytesEquals(
                new byte[]{
                        42, 0, -87, -110, 95, -78,
                        14, 0, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114, 50,
                        6, 0, 0, 0, 0, 3},
                new RowAssembler(schema, 1, 0)
                        .appendShort((short) 33)
                        .appendString("keystr2")
                        .appendNull()
                        .appendNull()
                        .toBytes());

        // No value.
        assertRowBytesEquals(
                new byte[]{
                        0, 0, -93, 60, -78, -36,
                        13, 0, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114},
                new RowAssembler(schema, 1, 0)
                        .appendShort((short) 33)
                        .appendString("keystr")
                        .toBytes());
    }

    /**
     * Assert row bytes equals.
     *
     * @param expected Expected row bytes.
     * @param actual   Actual row bytes.
     */
    private void assertRowBytesEquals(byte[] expected, byte[] actual) {
        assertArrayEquals(expected, actual, Arrays.toString(actual));
    }
}
