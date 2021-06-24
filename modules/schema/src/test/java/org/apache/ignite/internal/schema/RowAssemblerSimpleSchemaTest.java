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

package org.apache.ignite.internal.schema;

import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeTypes.BYTE;
import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.INTEGER;
import static org.apache.ignite.internal.schema.NativeTypes.SHORT;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Validate row layout for different schema configurations.
 */
public class RowAssemblerSimpleSchemaTest {
    /** Uuid test value. */
    public final UUID uuidVal = new UUID(-5204230847775358097L, 4916207022290092939L);

    /** Table ID test value. */
    public final UUID tableId = java.util.UUID.randomUUID();

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len nullable value.
     */
    @Test
    public void fixedKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyIntCol", INTEGER, false)},
            new Column[] {new Column("valIntCol", INTEGER, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -116, 33, 0, 0, 0, 8, 0, 0, 0, 33, 0, 0, 0, 9, 0, 0, 0, 0, -71, -1, -1, -1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendInt(33)
                .appendInt(-71)
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -116, 33, 0, 0, 0, 8, 0, 0, 0, 33, 0, 0, 0, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendInt(33)
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 12, 33, 0, 0, 0, 8, 0, 0, 0, 33, 0, 0, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendInt(33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len non-null value.
     */
    @Test
    public void fixedKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", SHORT, false)},
            new Column[] {new Column("valShortCol", SHORT, false)});

        // With value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -52, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0, 6, 0, 0, 0, -71, -1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .appendShort((short)-71)
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 12, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len nullable value.
     */
    @Test
    public void fixedKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", SHORT, false)},
            new Column[] {new Column("valStrCol", STRING, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -116, -33, -1, -1, -1, 6, 0, 0, 0, -33, -1, 8, 0, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 128, 0, 128, 1)
                .appendShort((short)-33)
                .appendString("val")
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -116, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 12, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len non-null value.
     */
    @Test
    public void fixedKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", SHORT, false)},
            new Column[] {new Column("valStrCol", STRING, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -52, -33, -1, -1, -1, 6, 0, 0, 0, -33, -1, 7, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 128, 0, 128, 1)
                .appendShort((short)-33)
                .appendString("val")
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 12, 33, 0, 0, 0, 6, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len non-null value.
     */
    @Test
    public void fixedNullableKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", SHORT, true)},
            new Column[] {new Column("valByteCol", BYTE, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -56, -33, -1, -1, -1, 7, 0, 0, 0, 0, -33, -1, 5, 0, 0, 0, 71},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)-33)
                .appendByte((byte)71)
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -56, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, -71},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendNull()
                .appendByte((byte)-71)
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 8, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len nullable value.
     */
    @Test
    public void fixedNullableKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyShortCol", SHORT, true)},
            new Column[] {new Column("valShortCol", SHORT, true)});

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 0, 0, 0, 0, 5, 0, 0, 0, 1, 7, 0, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendNull()
                .appendShort((short)33)
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .appendNull()
                .build());

        // Null both.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 8, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .build());

    }

    /**
     * Validate row layout for schema of fix-len nullable key and var-len nullable value.
     */
    @Test
    public void fixedNullableKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyIntCol", SHORT, true)},
            new Column[] {new Column("valStrCol", STRING, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0, 8, 0, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 128, 0, 128, 1)
                .appendShort((short)33)
                .appendString("val")
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 0, 0, 0, 0, 5, 0, 0, 0, 1, 8, 0, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 128, 0, 128, 1)
                .appendNull()
                .appendString("val")
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .appendNull()
                .build());

        // Null both.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 8, 33, 0, 0, 0, 7, 0, 0, 0, 0, 33, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendShort((short)33)
                .build());
    }

    /**
     * Validate row layout for schema of fix-len nullable key and var-len non-null value.
     */
    @Test
    public void fixedNullableKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyByteCol", BYTE, true)},
            new Column[] {new Column("valStrCol", STRING, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -56, 33, 0, 0, 0, 6, 0, 0, 0, 0, 33, 7, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 128, 0, 128, 1)
                .appendByte((byte)33)
                .appendString("val").build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -56, 0, 0, 0, 0, 5, 0, 0, 0, 1, 7, 0, 0, 0, 118, 97, 108},
            new RowAssembler(schema, 128, 0, 128, 1)
                .appendNull()
                .appendString("val").build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 8, 33, 0, 0, 0, 6, 0, 0, 0, 0, 33},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendByte((byte)33)
                .build());
    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len nullable value.
     */
    @Test
    public void varlenKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, false)},
            new Column[] {new Column("valUuidCol", UUID, true)});

        assertRowBytesEquals(
            new byte[] {
                42, 0, 0, -116, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121,
                21, 0, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .appendUuid(uuidVal)
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -116, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 12, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .build());

    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len non-null value.
     */
    @Test
    public void varlenKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, false)},
            new Column[] {new Column("valUuidCol", UUID, false)});

        assertRowBytesEquals(
            new byte[] {
                42, 0, 0, -52, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121,
                20, 0, 0, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .appendUuid(uuidVal)
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 12, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .build());

    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len nullable value.
     */
    @Test
    public void varlenKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, false)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        System.out.println(Integer.toHexString(((byte)-116) & 0xFF));
        System.out.println(Integer.toHexString(((byte)-99) & 0xFF));

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -116, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121, 9, 0, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 128, 1, 128, 1)
                .appendString("key")
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -116, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 12, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .build());

    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len non-null value.
     */
    @Test
    public void varlenKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, false)},
            new Column[] {new Column("valBytesCol", BYTES, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -52, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121, 8, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 128, 1, 128, 1)
                .appendString("key")
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 12, 95, -98, 1, 0, 7, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .build());
    }

    /**
     * Validate row layout for schema of var-len nullable key and fix-len nullable value.
     */
    @Test
    public void varlenNullableKeyFixedNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, true)},
            new Column[] {new Column("valShortCol", SHORT, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 7, 0, 0, 0, 0, -71, -1},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .appendShort((short)-71)
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 0, 0, 0, 0, 5, 0, 0, 0, 1, 7, 0, 0, 0, 0, 71, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendNull()
                .appendShort((short)71)
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .appendNull()
                .build());

        // Null both.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 8, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .build());

    }

    /**
     * Validate row layout for schema of var-len nullable key and fix-len non-null value.
     */
    @Test
    public void varlenNullableKeyFixedValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, true)},
            new Column[] {new Column("valShortCol", SHORT, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -56, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 6, 0, 0, 0, -71, -1},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .appendShort((short)-71L)
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -56, 0, 0, 0, 0, 5, 0, 0, 0, 1, 6, 0, 0, 0, 71, 0},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendNull()
                .appendShort((short)71)
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 8, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .build());
    }

    /**
     * Validate row layout for schema of var-len nullable key and var-len nullable value.
     */
    @Test
    public void varlenNullableKeyVarlenNullableValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, true)},
            new Column[] {new Column("valBytesCol", BYTES, true)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 9, 0, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 128, 1, 128, 1)
                .appendString("key")
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 0, 0, 0, 0, 5, 0, 0, 0, 1, 9, 0, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 128, 0, 128, 1)
                .appendNull()
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // Null value.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .appendNull()
                .build());

        // Null both.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -120, 0, 0, 0, 0, 5, 0, 0, 0, 1, 5, 0, 0, 0, 1},
            new RowAssembler(schema, 128, 0, 128, 0)
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 8, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .build());
    }

    /**
     * Validate row layout for schema of var-len nullable key and var-len non-null value.
     */
    @Test
    public void varlenNullableKeyVarlenValue() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {new Column("keyStrCol", STRING, true)},
            new Column[] {new Column("valBytesCol", BYTES, false)});

        assertRowBytesEquals(
            new byte[] {42, 0, 0, -56, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121, 8, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 128, 1, 128, 1)
                .appendString("key")
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // Null key.
        assertRowBytesEquals(
            new byte[] {42, 0, 0, -56, 0, 0, 0, 0, 5, 0, 0, 0, 1, 8, 0, 0, 0, -1, 1, 0, 120},
            new RowAssembler(schema, 128, 0, 128, 1)
                .appendNull()
                .appendBytes(new byte[] {-1, 1, 0, 120})
                .build());

        // No value.
        assertRowBytesEquals(
            new byte[] {42, 0, 1, 8, 95, -98, 1, 0, 8, 0, 0, 0, 0, 107, 101, 121},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendString("key")
                .build());
    }

    /**
     * Validate row layout for key\value columns of different types.
     */
    @Test
    public void mixedTypes() {
        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42,
            new Column[] {
                new Column("keyShortCol", SHORT, false),
                new Column("keyStrCol", STRING, false)
            },
            new Column[] {
                new Column("valIntCol", INTEGER, true),
                new Column("valStrCol", STRING, true)
            });

        assertRowBytesEquals(new byte[] {
                42, 0, 0, -116, 113, -109, 94, -68,
                12, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114,
                15, 0, 0, 0, 0, 73, 0, 0, 0, 118, 97, 108, 115, 116, 114},
            new RowAssembler(schema, 128, 1, 128, 1)
                .appendShort((short)33)
                .appendString("keystr")
                .appendInt(73)
                .appendString("valstr")
                .build());

        // Null value.
        assertRowBytesEquals(new byte[] {
                42, 0, 0, -116, -1, 98, 115, -49,
                13, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114, 50,
                5, 0, 0, 0, 3},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendShort((short)33)
                .appendString("keystr2")
                .appendNull()
                .appendNull()
                .build());

        // No value.
        assertRowBytesEquals(new byte[] {
                42, 0, 1, 12, 113, -109, 94, -68,
                12, 0, 0, 0, 33, 0, 107, 101, 121, 115, 116, 114},
            new RowAssembler(schema, 128, 1, 128, 0)
                .appendShort((short)33)
                .appendString("keystr")
                .build());
    }

    /**
     * @param expected Expected row bytes.
     * @param actual Actual row bytes.
     */
    private void assertRowBytesEquals(byte[] expected, byte[] actual) {
        assertArrayEquals(expected, actual, Arrays.toString(actual));
    }
}
