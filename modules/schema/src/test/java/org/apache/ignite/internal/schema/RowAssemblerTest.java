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
public class RowAssemblerTest {
    /** Uuid test value. */
    public final java.util.UUID uuidVal = new UUID(-5204230847775358097L, 4916207022290092939L);

    /** Table ID test value. */
    public final java.util.UUID tableId = java.util.UUID.randomUUID();

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len nullable value.
     */
    @Test
    public void fixedKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyIntCol", INTEGER, false)};
        Column[] valCols = new Column[] {new Column("valIntCol", INTEGER, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendInt(33);
            asm.appendInt(-71);

            assertRowBytesEquals(new byte[] {42, 0, 122, 0, -10, 54, 5, 7, 5, 33, 0, 0, 0, 6, 0, -71, -1, -1, -1}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendInt(-33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 122, 0, 61, 0, -79, 50, 5, -33, -1, -1, -1, 2, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendInt(-33);

            assertRowBytesEquals(new byte[] {42, 0, 123, 0, 61, 0, -79, 50, 5, -33, -1, -1, -1}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len non-null key and fix-len non-null value.
     */
    @Test
    public void fixedKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        { // With value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)33);
            asm.appendShort((short)71L);

            assertRowBytesEquals(new byte[] {42, 0, 126, 0, 1, -101, 57, -89, 3, 33, 0, 3, 71, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)-33);

            assertRowBytesEquals(new byte[] {42, 0, 127, 0, 83, 62, 65, -105, 3, -33, -1}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len nullable value.
     */
    @Test
    public void fixedKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 1);

            asm.appendShort((short)33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 106, 0, 1, -101, 57, -89, 3, 33, 0, 7, 0, 1, 0, 118, 97, 108}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 122, 0, 1, -101, 57, -89, 3, 33, 0, 2, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 123, 0, 1, -101, 57, -89, 3, 33, 0}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len non-null key and var-len non-null value.
     */
    @Test
    public void fixedKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, false)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 1);

            asm.appendShort((short)33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 110, 0, 1, -101, 57, -89, 3, 33, 0, 6, 1, 0, 118, 97, 108}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 127, 0, 1, -101, 57, -89, 3, 33, 0}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len non-null value.
     */
    @Test
    public void fixedNullableKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, true)};
        Column[] valCols = new Column[] {new Column("valByteCol", BYTE, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)-33);
            asm.appendByte((byte)71);

            assertRowBytesEquals(new byte[] {42, 0, 124, 0, 83, 62, 65, -105, 4, 0, -33, -1, 2, 71}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendNull();
            asm.appendByte((byte)-71);

            assertRowBytesEquals(new byte[] {42, 0, 124, 0, 5, 93, -52, 2, 2, 1, 2, -71}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)33);

            assertRowBytesEquals(new byte[] {42, 0, 125, 0, 1, -101, 57, -89, 4, 0, 33, 0}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len nullable key and fix-len nullable value.
     */
    @Test
    public void fixedNullableKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyShortCol", SHORT, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)1133);
            asm.appendShort((short)1071);

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, -100, -86, -70, -80, 4, 0, 109, 4, 4, 0, 47, 4}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendNull();
            asm.appendShort((short)1171);

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, 5, 93, -52, 2, 2, 1, 4, 0, -109, 4}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)1133);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, -100, -86, -70, -80, 4, 0, 109, 4, 2, 1}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, 5, 93, -52, 2, 2, 1, 2, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendShort((short)1133);

            assertRowBytesEquals(new byte[] {42, 0, 121, 0, -100, -86, -70, -80, 4, 0, 109, 4}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len nullable key and var-len nullable value.
     */
    @Test
    public void fixedNullableKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyIntCol", INTEGER, true)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 1);

            asm.appendInt(-33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 104, 0, 61, 0, -79, 50, 6, 0, -33, -1, -1, -1, 7, 0, 1, 0, 118, 97, 108}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 1);

            asm.appendNull();
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 104, 0, 5, 93, -52, 2, 2, 1, 7, 0, 1, 0, 118, 97, 108}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendInt(33);
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, -10, 54, 5, 7, 6, 0, 33, 0, 0, 0, 2, 1}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, 5, 93, -52, 2, 2, 1, 2, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendInt(33);

            assertRowBytesEquals(new byte[] {42, 0, 121, 0, -10, 54, 5, 7, 6, 0, 33, 0, 0, 0}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of fix-len nullable key and var-len non-null value.
     */
    @Test
    public void fixedNullableKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyByteCol", BYTE, true)};
        Column[] valCols = new Column[] {new Column("valStrCol", STRING, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 1);

            asm.appendByte((byte)33);
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 108, 0, 116, -97, 78, -100, 3, 0, 33, 6, 1, 0, 118, 97, 108}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 1);

            asm.appendNull();
            asm.appendString("val");

            assertRowBytesEquals(new byte[] {42, 0, 108, 0, 5, 93, -52, 2, 2, 1, 6, 1, 0, 118, 97, 108}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendByte((byte)33);

            assertRowBytesEquals(new byte[] {42, 0, 125, 0, 116, -97, 78, -100, 3, 0, 33}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len nullable value.
     */
    @Test
    public void varlenKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valUuidCol", UUID, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");
            asm.appendUuid(uuidVal);

            assertRowBytesEquals(new byte[] {
                42, 0, 114, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121,
                18, 0, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 114, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121, 2, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 115, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len non-null key and fix-len non-null value.
     */
    @Test
    public void varlenKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valUuidCol", UUID, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");
            asm.appendUuid(uuidVal);

            assertRowBytesEquals(new byte[] {
                42, 0, 118, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121,
                17, -117, -61, -31, 85, 61, -32, 57, 68, 111, 67, 56, -3, -99, -37, -58, -73}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 119, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len nullable value.
     */
    @Test
    public void varlenKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 98, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121, 8, 0, 1, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 114, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121, 2, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 115, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len non-null key and var-len non-null value.
     */
    @Test
    public void varlenKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, false)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 102, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121, 7, 1, 0, -1, 1, 0, 120}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 119, 0, -32, 58, -39, -77, 6, 1, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len nullable key and fix-len nullable value.
     */
    @Test
    public void varlenNullableKeyFixedNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");
            asm.appendShort((short)-71);

            assertRowBytesEquals(new byte[] {42, 0, 112, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121, 4, 0, -71, -1}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendNull();
            asm.appendShort((short)71);

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, 5, 93, -52, 2, 2, 1, 4, 0, 71, 0}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 112, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121, 2, 1}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, 5, 93, -52, 2, 2, 1, 2, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 113, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len nullable key and fix-len non-null value.
     */
    @Test
    public void varlenNullableKeyFixedValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valShortCol", SHORT, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");
            asm.appendShort((short)-71L);

            assertRowBytesEquals(new byte[] {42, 0, 116, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121, 3, -71, -1}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendNull();
            asm.appendShort((short)71);

            assertRowBytesEquals(new byte[] {42, 0, 124, 0, 5, 93, -52, 2, 2, 1, 3, 71, 0}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 117, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len nullable key and var-len nullable value.
     */
    @Test
    public void varlenNullableKeyVarlenNullableValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, true)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 96, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121, 8, 0, 1, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 1);

            asm.appendNull();
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 104, 0, 5, 93, -52, 2, 2, 1, 8, 0, 1, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 112, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121, 2, 1}, asm.build());
        }

        { // Null both.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 0);

            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {42, 0, 120, 0, 5, 93, -52, 2, 2, 1, 2, 1}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 113, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for schema of var-len nullable key and var-len non-null value.
     */
    @Test
    public void varlenNullableKeyVarlenValue() {
        Column[] keyCols = new Column[] {new Column("keyStrCol", STRING, true)};
        Column[] valCols = new Column[] {new Column("valBytesCol", BYTES, false)};

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 1);

            asm.appendString("key");
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 100, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121, 7, 1, 0, -1, 1, 0, 120}, asm.build());
        }

        { // Null key.
            RowAssembler asm = new RowAssembler(schema, 128, 0, 128, 1);

            asm.appendNull();
            asm.appendBytes(new byte[] {-1, 1, 0, 120});

            assertRowBytesEquals(new byte[] {42, 0, 108, 0, 5, 93, -52, 2, 2, 1, 7, 1, 0, -1, 1, 0, 120}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendString("key");

            assertRowBytesEquals(new byte[] {42, 0, 117, 0, -32, 58, -39, -77, 7, 0, 1, 0, 107, 101, 121}, asm.build());
        }
    }

    /**
     * Validate row layout for key\value columns of different types.
     */
    @Test
    public void mixedTypes() {
        Column[] keyCols = new Column[] {
            new Column("keyShortCol", SHORT, false),
            new Column("keyStrCol", STRING, false)
        };
        Column[] valCols = new Column[] {
            new Column("valIntCol", INTEGER, true),
            new Column("valStrCol", STRING, true)
        };

        SchemaDescriptor schema = new SchemaDescriptor(tableId, 42, keyCols, valCols);

        {
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 1);

            asm.appendShort((short)33);
            asm.appendString("keystr");
            asm.appendInt(73);
            asm.appendString("valstr");

            assertRowBytesEquals(new byte[] {
                42, 0, 98, 0, 5, -39, 55, 59,
                11, 1, 2, 33, 0, 107, 101, 121, 115, 116, 114,
                14, 0, 1, 4, 73, 0, 0, 0, 118, 97, 108, 115, 116, 114}, asm.build());
        }

        { // Null value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendShort((short)33);
            asm.appendString("keystr2");
            asm.appendNull();
            asm.appendNull();

            assertRowBytesEquals(new byte[] {
                42, 0, 114, 0, -80, -78, -91, 1,
                12, 1, 2, 33, 0, 107, 101, 121, 115, 116, 114, 50,
                2, 3}, asm.build());
        }

        { // No value.
            RowAssembler asm = new RowAssembler(schema, 128, 1, 128, 0);

            asm.appendShort((short)33);
            asm.appendString("keystr");

            assertRowBytesEquals(new byte[] {
                42, 0, 115, 0, 5, -39, 55, 59,
                11, 1, 2, 33, 0, 107, 101, 121, 115, 116, 114,}, asm.build());
        }
    }

    /**
     * @param expected Expected row bytes.
     * @param actual Actual row bytes.
     */
    private void assertRowBytesEquals(byte[] expected, byte[] actual) {
        assertArrayEquals(expected, actual, Arrays.toString(actual));
    }
}
