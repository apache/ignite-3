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
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.junit.jupiter.api.Test;

/**
 * Validate row layout for different schema configurations.
 */
public class RowAssemblerAdvancedSchemaTest {
    /**
     * Validate row layout for schema of fix-len nullable key and fix-len nullable value.
     */
    @Test
    public void fixedNullableColumns() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{
                        new Column("keyCol", INT8, false),
                },
                new Column[]{
                        new Column("valCol2", INT8, true),
                        new Column("valCol1", INT32, true),
                        new Column("valCol3", INT16, true)
                });

        // Last col null
        assertRowBytesEquals(
                new byte[]{42, 0, 125, 64, 31, -9, 6, 0, 0, 0, 0, 22, 9, 0, 0, 0, 0, 4, -44, -66, -1},
                new RowAssembler(schema, 0, 0)
                        .appendByte((byte) 22)
                        .appendByte((byte) -44)
                        .appendShort((short) -66)
                        .appendNull()
                        .toBytes());

        // First col null.
        assertRowBytesEquals(
                new byte[]{42, 0, 125, 64, 31, -9, 6, 0, 0, 0, 0, 22, 12, 0, 0, 0, 0, 1, -55, -1, -66, -1, -1, -1},
                new RowAssembler(schema, 0, 0)
                        .appendByte((byte) 22)
                        .appendNull()
                        .appendShort((short) -55)
                        .appendInt(-66)
                        .toBytes());

        // Middle col null.
        assertRowBytesEquals(
                new byte[]{42, 0, 125, 64, 31, -9, 6, 0, 0, 0, 0, 22, 11, 0, 0, 0, 0, 2, -44, -66, -1, -1, -1},
                new RowAssembler(schema, 0, 0)
                        .appendByte((byte) 22)
                        .appendByte((byte) -44)
                        .appendNull()
                        .appendInt(-66)
                        .toBytes());

        // All null.
        assertRowBytesEquals(new byte[]{42, 0, 125, 64, 31, -9, 6, 0, 0, 0, 0, 22, 6, 0, 0, 0, 0, 7},
                new RowAssembler(schema, 0, 0)
                        .appendByte((byte) 22)
                        .appendNull()
                        .appendNull()
                        .appendNull()
                        .toBytes());

        // No value.
        assertRowBytesEquals(new byte[]{0, 0, 125, 64, 31, -9, 6, 0, 0, 0, 0, 22},
                new RowAssembler(schema, 0, 0)
                        .appendByte((byte) 22)
                        .toBytes());
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len nullable value.
     */
    @Test
    public void varlenNullableColumns() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{
                        new Column("keyCol", INT8, false),
                },
                new Column[]{
                        new Column("valCol1", STRING, true),
                        new Column("valCol2", BYTES, true),
                        new Column("valCol3", STRING, true)
                });

        // Last col null
        assertRowBytesEquals(
                new byte[]{
                        42, 0, 125, 64, 31, -9,
                        6, 0, 0, 0, 0, 22,
                        20, 0, 0, 0, 1, 4, 1, 2, 33, -77, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97},
                new RowAssembler(schema, 0, 2)
                        .appendByte((byte) 22)
                        .appendBytes(new byte[]{33, -77})
                        .appendString("我愛Java")
                        .appendNull()
                        .toBytes());

        // Fist col null.
        assertRowBytesEquals(
                new byte[]{
                        42, 0, 125, 64, 31, -9,
                        6, 0, 0, 0, 0, 22,
                        23, 0, 0, 0, 1, 1, 1, 10, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97, 97, 115, 99, 105, 105},
                new RowAssembler(schema, 0, 2)
                        .appendByte((byte) 22)
                        .appendNull()
                        .appendString("我愛Java")
                        .appendString("ascii")
                        .toBytes());

        // Middle col null.
        assertRowBytesEquals(
                new byte[]{
                        42, 0, 125, 64, 31, -9,
                        6, 0, 0, 0, 0, 22,
                        15, 0, 0, 0, 1, 2, 1, 2, 33, -77, 97, 115, 99, 105, 105},
                new RowAssembler(schema, 0, 2)
                        .appendByte((byte) 22)
                        .appendBytes(new byte[]{33, -77})
                        .appendNull()
                        .appendString("ascii")
                        .toBytes());

        // All null.
        assertRowBytesEquals(
                new byte[]{42, 0, 125, 64, 31, -9, 6, 0, 0, 0, 0, 22, 6, 0, 0, 0, 0, 7},
                new RowAssembler(schema, 0, 0)
                        .appendByte((byte) 22)
                        .appendNull()
                        .appendNull()
                        .appendNull()
                        .toBytes());

        // No value.
        assertRowBytesEquals(new byte[]{0, 0, 125, 64, 31, -9, 6, 0, 0, 0, 0, 22},
                new RowAssembler(schema, 0, 0)
                        .appendByte((byte) 22)
                        .toBytes());
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len nullable value.
     */
    @Test
    public void mixedTypes() {
        SchemaDescriptor schema = new SchemaDescriptor(42,
                new Column[]{
                        new Column("keyCol1", INT8, false),
                        new Column("keyCol1", INT16, false),
                        new Column("keyCol2", BYTES, false),
                        new Column("keyCol3", STRING, false)
                },
                new Column[]{
                        new Column("valCol1", INT8, true),
                        new Column("valCol1", INT16, true),
                        new Column("valCol2", BYTES, true),
                        new Column("valCol3", STRING, true)
                });

        // Check null/non-null all fixlen/varlen.
        assertRowBytesEquals(
                new byte[]{
                        42, 0, -108, -41, -56, 26,
                        10, 0, 0, 0, 1, 1, 3, 11, 22, 0,
                        15, 0, 0, 0, 1, 3, 1, 2, 77, -88, 97, 115, 99, 105, 105},
                new RowAssembler(schema, 2, 2)
                        .appendByte((byte) 11)
                        .appendShort((short) 22)
                        .appendBytes(new byte[]{})
                        .appendString("")
                        .appendNull()
                        .appendNull()
                        .appendBytes(new byte[]{77, -88})
                        .appendString("ascii")
                        .toBytes());

        // Check null/non-null fixlen.
        assertRowBytesEquals(
                new byte[]{
                        42, 0, 121, 23, 75, 100,
                        22, 0, 0, 0, 1, 1, 5, 11, 22, 0, 33, -44, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97,
                        16, 0, 0, 0, 1, 2, 1, 3, 55, 77, -88, 97, 115, 99, 105, 105},
                new RowAssembler(schema, 2, 2)
                        .appendByte((byte) 11)
                        .appendShort((short) 22)
                        .appendBytes(new byte[]{33, -44})
                        .appendString("我愛Java")
                        .appendByte((byte) 55)
                        .appendNull()
                        .appendBytes(new byte[]{77, -88})
                        .appendString("ascii")
                        .toBytes());

        // Check null/non-null varlen.
        assertRowBytesEquals(
                new byte[]{
                        42, 0, 121, 23, 75, 100,
                        22, 0, 0, 0, 1, 1, 5, 11, 22, 0, 33, -44, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97,
                        11, 0, 0, 0, 0, 8, 55, 22, 0, 77, -88},
                new RowAssembler(schema, 2, 1)
                        .appendByte((byte) 11)
                        .appendShort((short) 22)
                        .appendBytes(new byte[]{33, -44})
                        .appendString("我愛Java")
                        .appendByte((byte) 55)
                        .appendShort((short) 22)
                        .appendBytes(new byte[]{77, -88})
                        .appendNull()
                        .toBytes());

        // Check null/non-null mixed.
        assertRowBytesEquals(
                new byte[]{
                        42, 0, 121, 23, 75, 100,
                        22, 0, 0, 0, 1, 1, 5, 11, 22, 0, 33, -44, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97,
                        10, 0, 0, 0, 0, 9, 22, 0, 77, -88},
                new RowAssembler(schema, 2, 1)
                        .appendByte((byte) 11)
                        .appendShort((short) 22)
                        .appendBytes(new byte[]{33, -44})
                        .appendString("我愛Java")
                        .appendNull()
                        .appendShort((short) 22)
                        .appendBytes(new byte[]{77, -88})
                        .appendNull()
                        .toBytes());

        // Check all null/non-null.
        assertRowBytesEquals(
                new byte[]{
                        42, 0, 121, 23, 75, 100,
                        22, 0, 0, 0, 1, 1, 5, 11, 22, 0, 33, -44, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97,
                        6, 0, 0, 0, 0, 15},
                new RowAssembler(schema, 2, 0)
                        .appendByte((byte) 11)
                        .appendShort((short) 22)
                        .appendBytes(new byte[]{33, -44})
                        .appendString("我愛Java")
                        .appendNull()
                        .appendNull()
                        .appendNull()
                        .appendNull()
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
