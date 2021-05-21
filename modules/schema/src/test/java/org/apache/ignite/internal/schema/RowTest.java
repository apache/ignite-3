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
import java.util.BitSet;
import java.util.Collections;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeTypes.BYTE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INTEGER;
import static org.apache.ignite.internal.schema.NativeTypes.LONG;
import static org.apache.ignite.internal.schema.NativeTypes.SHORT;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests row assembling and reading.
 */
public class RowTest {
    /** Random. */
    private Random rnd;

    /**
     * Initialization.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    /**
     * Check row serialization for schema with nullable fix-sized columns only.
     */
    @Test
    public void nullableFixSizedColumns() {
        Column[] keyCols = new Column[] {
            new Column("keyByteCol", BYTE, false),
            new Column("keyShortCol", SHORT, false),
            new Column("keyIntCol", INTEGER, false),
            new Column("keyLongCol", LONG, false),
            new Column("keyFloatCol", FLOAT, false),
            new Column("keyDoubleCol", DOUBLE, false),
            new Column("keyUuidCol", UUID, false),
            new Column("keyBitmask1Col", NativeTypes.bitmaskOf(4), false),
            new Column("keyBitmask2Col", NativeTypes.bitmaskOf(22), false)
        };

        Column[] valCols = new Column[] {
            new Column("valByteCol", BYTE, false),
            new Column("valShortCol", SHORT, false),
            new Column("valIntCol", INTEGER, false),
            new Column("valLongCol", LONG, false),
            new Column("valFloatCol", FLOAT, false),
            new Column("valDoubleCol", DOUBLE, false),
            new Column("valUuidCol", UUID, false),
            new Column("valBitmask1Col", NativeTypes.bitmaskOf(4), false),
            new Column("valBitmask2Col", NativeTypes.bitmaskOf(22), false)
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with non-nullable fix-sized columns only.
     */
    @Test
    public void fixSizedColumns() {
        Column[] keyCols = new Column[] {
            new Column("keyByteCol", BYTE, true),
            new Column("keyShortCol", SHORT, true),
            new Column("keyIntCol", INTEGER, true),
            new Column("keyLongCol", LONG, true),
            new Column("keyFloatCol", FLOAT, true),
            new Column("keyDoubleCol", DOUBLE, true),
            new Column("keyUuidCol", UUID, true),
            new Column("keyBitmask1Col", NativeTypes.bitmaskOf(4), true),
            new Column("keyBitmask2Col", NativeTypes.bitmaskOf(22), true)
        };

        Column[] valCols = new Column[] {
            new Column("valByteCol", BYTE, true),
            new Column("valShortCol", SHORT, true),
            new Column("valIntCol", INTEGER, true),
            new Column("valLongCol", LONG, true),
            new Column("valFloatCol", FLOAT, true),
            new Column("valDoubleCol", DOUBLE, true),
            new Column("valUuidCol", UUID, true),
            new Column("valBitmask1Col", NativeTypes.bitmaskOf(4), true),
            new Column("valBitmask2Col", NativeTypes.bitmaskOf(22), true)
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with various columns.
     */
    @Test
    public void mixedColumns() {
        Column[] keyCols = new Column[] {
            new Column("keyByteCol", BYTE, false),
            new Column("keyShortCol", SHORT, false),
            new Column("keyIntCol", INTEGER, false),
            new Column("keyLongCol", LONG, false),
            new Column("keyBytesCol", BYTES, false),
            new Column("keyStringCol", STRING, false),
        };

        Column[] valCols = new Column[] {
            new Column("keyByteCol", BYTE, true),
            new Column("keyShortCol", SHORT, true),
            new Column("keyIntCol", INTEGER, true),
            new Column("keyLongCol", LONG, true),
            new Column("valBytesCol", BYTES, true),
            new Column("valStringCol", STRING, true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with non-nullable varlen columns only.
     */
    @Test
    public void varlenColumns() {
        Column[] keyCols = new Column[] {
            new Column("keyBytesCol", BYTES, false),
            new Column("keyStringCol", STRING, false),
        };

        Column[] valCols = new Column[] {
            new Column("valBytesCol", BYTES, false),
            new Column("valStringCol", STRING, false),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for schema with nullable varlen columns only.
     */
    @Test
    public void nullableVarlenColumns() {
        Column[] keyCols = new Column[] {
            new Column("keyBytesCol", BYTES, true),
            new Column("keyStringCol", STRING, true),
        };

        Column[] valCols = new Column[] {
            new Column("valBytesCol", BYTES, true),
            new Column("valStringCol", STRING, true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Checks schema is independent from prodived column order.
     *
     * @param keyCols Key columns.
     * @param valCols Value columns.
     */
    private void checkSchema(Column[] keyCols, Column[] valCols) {
        checkSchemaShuffled(keyCols, valCols);

        shuffle(keyCols);
        shuffle(valCols);

        checkSchemaShuffled(keyCols, valCols);
    }

    /**
     * Checks schema for given columns.
     *
     * @param keyCols Key columns.
     * @param valCols Value columns.
     */
    private void checkSchemaShuffled(Column[] keyCols, Column[] valCols) {
        SchemaDescriptor sch = new SchemaDescriptor(java.util.UUID.randomUUID(), 1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch);

        checkValues(sch, checkArr);

        for (int idx = 0; idx < checkArr.length; idx++) {
            if (!sch.column(idx).nullable())
                continue;

            Object prev = checkArr[idx];
            checkArr[idx] = null;

            checkValues(sch, checkArr);

            checkArr[idx] = prev;
        }
    }

    /**
     * Generate row values for given row schema.
     *
     * @param schema Row schema.
     * @return Row values.
     */
    private Object[] generateRowValues(SchemaDescriptor schema) {
        Object[] res = new Object[schema.length()];

        for (int i = 0; i < res.length; i++) {
            NativeType type = schema.column(i).type();

            res[i] = generateRandomValue(type);
        }

        return res;
    }

    /**
     * Generates random value of a given type.
     *
     * @param type Value type.
     * @return Random value of requested type.
     */
    private Object generateRandomValue(NativeType type) {
        return TestUtils.generateRandomValue(rnd, type);
    }

    /**
     * Validates row values after serialization-then-deserialization.
     *
     * @param schema Row schema.
     * @param vals Row values.
     */
    private void checkValues(SchemaDescriptor schema, Object... vals) {
        assertEquals(schema.keyColumns().length() + schema.valueColumns().length(), vals.length);

        int nonNullVarLenKeyCols = 0;
        int nonNullVarLenValCols = 0;
        int nonNullVarLenKeySize = 0;
        int nonNullVarLenValSize = 0;

        for (int i = 0; i < vals.length; i++) {
            NativeTypeSpec type = schema.column(i).type().spec();

            if (vals[i] != null && !type.fixedLength()) {
                if (type == NativeTypeSpec.BYTES) {
                    byte[] val = (byte[])vals[i];
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += val.length;
                    }
                    else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += val.length;
                    }
                }
                else if (type == NativeTypeSpec.STRING) {
                    if (schema.isKeyColumn(i)) {
                        nonNullVarLenKeyCols++;
                        nonNullVarLenKeySize += RowAssembler.utf8EncodedLength((CharSequence)vals[i]);
                    }
                    else {
                        nonNullVarLenValCols++;
                        nonNullVarLenValSize += RowAssembler.utf8EncodedLength((CharSequence)vals[i]);
                    }
                }
                else
                    throw new IllegalStateException("Unsupported variable-length type: " + type);
            }
        }

        int size = RowAssembler.rowSize(
            schema.keyColumns(), nonNullVarLenKeyCols, nonNullVarLenKeySize,
            schema.valueColumns(), nonNullVarLenValCols, nonNullVarLenValSize);

        RowAssembler asm = new RowAssembler(schema, size, nonNullVarLenKeyCols, nonNullVarLenValCols);

        for (int i = 0; i < vals.length; i++) {
            if (vals[i] == null)
                asm.appendNull();
            else {
                NativeType type = schema.column(i).type();

                switch (type.spec()) {
                    case BYTE:
                        asm.appendByte((Byte)vals[i]);
                        break;

                    case SHORT:
                        asm.appendShort((Short)vals[i]);
                        break;

                    case INTEGER:
                        asm.appendInt((Integer)vals[i]);
                        break;

                    case LONG:
                        asm.appendLong((Long)vals[i]);
                        break;

                    case FLOAT:
                        asm.appendFloat((Float)vals[i]);
                        break;

                    case DOUBLE:
                        asm.appendDouble((Double)vals[i]);
                        break;

                    case UUID:
                        asm.appendUuid((java.util.UUID)vals[i]);
                        break;

                    case STRING:
                        asm.appendString((String)vals[i]);
                        break;

                    case BYTES:
                        asm.appendBytes((byte[])vals[i]);
                        break;

                    case BITMASK:
                        asm.appendBitmask((BitSet)vals[i]);
                        break;

                    default:
                        throw new IllegalStateException("Unsupported test type: " + type);
                }
            }
        }

        byte[] data = asm.build();

        Row tup = new Row(schema, new ByteBufferRow(data));

        for (int i = 0; i < vals.length; i++) {
            Column col = schema.column(i);

            NativeTypeSpec type = col.type().spec();

            if (type == NativeTypeSpec.BYTES)
                assertArrayEquals((byte[])vals[i], (byte[])NativeTypeSpec.BYTES.objectValue(tup, i),
                    "Failed for column: " + col);
            else
                assertEquals(vals[i], type.objectValue(tup, i), "Failed for column: " + col);
        }
    }

    /**
     * Shuffle columns.
     */
    private void shuffle(Column[] cols) {
        Collections.shuffle(Arrays.asList(cols));
    }
}
