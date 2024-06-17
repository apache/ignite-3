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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomString;
import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.UUID;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

        Loggers.forClass(RowTest.class).info("Using seed: " + seed + "L; //");

        rnd = new Random(seed);
    }

    /**
     * Check row serialization for a schema with nullable fix-sized columns only.
     */
    @Test
    public void nullableFixSizedColumns() {
        Column[] keyCols = new Column[]{
                new Column("keyBooleanCol", BOOLEAN, false),
                new Column("keyByteCol", INT8, false),
                new Column("keyShortCol", INT16, false),
                new Column("keyIntCol", INT32, false),
                new Column("keyLongCol", INT64, false),
                new Column("keyFloatCol", FLOAT, false),
                new Column("keyDoubleCol", DOUBLE, false),
                new Column("keyUuidCol", UUID, false),
                new Column("keyDateCol", DATE, false),
                new Column("keyTimeCol", time(0), false),
                new Column("keyDateTimeCol", datetime(3), false),
                new Column("keyTimeStampCol", timestamp(3), false),
                new Column("keyBitmask1Col", NativeTypes.bitmaskOf(4), false),
                new Column("keyBitmask2Col", NativeTypes.bitmaskOf(22), false)
        };

        Column[] valCols = new Column[]{
                new Column("valBooleanCol", BOOLEAN, false),
                new Column("valByteCol", INT8, false),
                new Column("valShortCol", INT16, false),
                new Column("valIntCol", INT32, false),
                new Column("valLongCol", INT64, false),
                new Column("valFloatCol", FLOAT, false),
                new Column("valDoubleCol", DOUBLE, false),
                new Column("valUuidCol", UUID, false),
                new Column("valDateCol", DATE, false),
                new Column("valTimeCol", time(0), false),
                new Column("valDateTimeCol", datetime(3), false),
                new Column("valTimeStampCol", timestamp(3), false),
                new Column("valBitmask1Col", NativeTypes.bitmaskOf(4), false),
                new Column("valBitmask2Col", NativeTypes.bitmaskOf(22), false)
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for a schema with non-nullable fix-sized columns only.
     */
    @Test
    public void fixSizedColumns() {
        Column[] keyCols = new Column[]{
                new Column("keyBooleanCol", BOOLEAN, false),
                new Column("keyByteCol", INT8, false),
                new Column("keyShortCol", INT16, false),
                new Column("keyIntCol", INT32, false),
                new Column("keyLongCol", INT64, false),
                new Column("keyFloatCol", FLOAT, false),
                new Column("keyDoubleCol", DOUBLE, false),
                new Column("keyUuidCol", UUID, false),
                new Column("keyDateCol", DATE, false),
                new Column("keyTimeCol", time(0), false),
                new Column("keyDateTimeCol", datetime(3), false),
                new Column("keyTimeStampCol", timestamp(3), false),
                new Column("keyBitmask1Col", NativeTypes.bitmaskOf(4), false),
                new Column("keyBitmask2Col", NativeTypes.bitmaskOf(22), false),
        };

        Column[] valCols = new Column[]{
                new Column("valBooleanCol", BOOLEAN, true),
                new Column("valByteCol", INT8, true),
                new Column("valShortCol", INT16, true),
                new Column("valIntCol", INT32, true),
                new Column("valLongCol", INT64, true),
                new Column("valFloatCol", FLOAT, true),
                new Column("valDoubleCol", DOUBLE, true),
                new Column("valUuidCol", UUID, true),
                new Column("valDateCol", DATE, true),
                new Column("valTimeCol", time(0), true),
                new Column("valDateTimeCol", datetime(3), true),
                new Column("valTimeStampCol", timestamp(3), true),
                new Column("valBitmask1Col", NativeTypes.bitmaskOf(4), true),
                new Column("valBitmask2Col", NativeTypes.bitmaskOf(22), true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for a schema with various columns.
     */
    @Test
    public void mixedColumns() {
        Column[] keyCols = new Column[]{
                new Column("keyByteCol", INT8, false),
                new Column("keyShortCol", INT16, false),
                new Column("keyIntCol", INT32, false),
                new Column("keyLongCol", INT64, false),
                new Column("keyDateTimeCol", datetime(3), false),
                new Column("keyBytesCol", BYTES, false),
                new Column("keyStringCol", STRING, false),
                new Column("keyNumberCol", NativeTypes.numberOf(9), false),
                new Column("keyDecimalCol", NativeTypes.decimalOf(20, 3), false),
        };

        Column[] valCols = new Column[]{
                new Column("valByteCol", INT8, true),
                new Column("valShortCol", INT16, true),
                new Column("valIntCol", INT32, true),
                new Column("valLongCol", INT64, true),
                new Column("valDateTimeCol", datetime(3), true),
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true),
                new Column("valNumberCol", NativeTypes.numberOf(9), true),
                new Column("valDecimalCol", NativeTypes.decimalOf(20, 3), true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for a schema with various columns.
     */
    @Test
    public void temporalColumns() {
        Column[] keyCols = new Column[]{
                new Column("keyTimestampCol1", timestamp(0), false),
                new Column("keyTimestampCol2", timestamp(1), false),
                new Column("keyTimestampCol3", timestamp(2), false),
                new Column("keyTimestampCol4", timestamp(3), false),
        };

        Column[] valCols = new Column[]{
                new Column("valDateTimeCol1", datetime(0), false),
                new Column("valDateTimeCol2", datetime(1), true),
                new Column("valDateTimeCol3", datetime(2), false),
                new Column("valDateTimeCol4", datetime(3), true),
                new Column("valTimeCol1", time(0), true),
                new Column("valTimeCol2", time(1), true),
                new Column("valTimeCol3", time(2), false),
                new Column("valTimeCol4", time(3), false),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for a schema with non-nullable varlen columns only.
     */
    @Test
    public void varlenColumns() {
        Column[] keyCols = new Column[]{
                new Column("keyBytesCol", BYTES, false),
                new Column("keyStringCol", STRING, false),
                new Column("keyNumberCol", NativeTypes.numberOf(9), false),
                new Column("keyDecimalCol", NativeTypes.decimalOf(20, 3), false),
        };

        Column[] valCols = new Column[]{
                new Column("valBytesCol", BYTES, false),
                new Column("valStringCol", STRING, false),
                new Column("valNumberCol", NativeTypes.numberOf(9), false),
                new Column("valDecimalCol", NativeTypes.decimalOf(20, 3), false),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization for a schema with nullable varlen columns only.
     */
    @Test
    public void nullableVarlenColumns() {
        Column[] keyCols = new Column[]{
                new Column("keyBytesCol", BYTES, false),
                new Column("keyStringCol", STRING, false),
        };

        Column[] valCols = new Column[]{
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true),
                new Column("valNumberCol", NativeTypes.numberOf(9), true),
                new Column("valDecimalCol", NativeTypes.decimalOf(20, 3), true),
        };

        checkSchema(keyCols, valCols);
    }

    /**
     * Check row serialization when all varlen columns are empty (zero size).
     */
    @Test
    public void emptyVarlenColumns() {
        Column[] keyCols = new Column[]{
                new Column("keyInt8Col", INT8, false),
                new Column("keyInt32Col", INT16, false),
                new Column("keyBytesCol", BYTES, false),
                new Column("keyStringCol", STRING, false),
        };

        Column[] valCols = new Column[]{
                new Column("valInt8Col", INT8, true),
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true),
        };

        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = new Object[]{(byte) 11, (short) 22, new byte[]{}, "", (byte) 78, new byte[]{}, ""};

        checkValues(sch, checkArr);
    }

    /**
     * Check row serialization for a schema with large varlen columns (64Kb+).
     */
    @Test
    public void largeVarlenColumns() {
        Column[] keyCols = new Column[]{
                new Column("keyBytesCol", BYTES, false),
                new Column("keyStringCol", STRING, false),
        };

        Column[] valCols = new Column[]{
                new Column("valBytesCol", BYTES, true),
                new Column("valStringCol", STRING, true),
        };

        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch, t -> (t.spec() == NativeTypeSpec.BYTES)
                ? randomBytes(rnd, rnd.nextInt(Constants.MiB) + (2 << 16)) :
                randomString(rnd, rnd.nextInt(Constants.MiB) + (2 << 16)));

        checkValues(sch, checkArr);

        for (int idx = 0; idx < checkArr.length; idx++) {
            if (!sch.column(idx).nullable()) {
                continue;
            }

            Object prev = checkArr[idx];
            checkArr[idx] = null;

            checkValues(sch, checkArr);

            checkArr[idx] = prev;
        }
    }

    /**
     * Check row serialization for a schema with many empty varlen columns (255+).
     */
    @Test
    public void bigVartable() {
        Column[] keyCols = new Column[]{
                new Column("id", INT64, false),
        };

        int columnCount = 260;

        Column[] valCols = IntStream.range(1, columnCount)
                .mapToObj(i -> new Column("val" + i, STRING, true))
                .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = IntStream.range(0, columnCount)
                .mapToObj(i -> i == 0 ? 42L : (i > columnCount - 5) ? "str" : "").toArray();

        checkValues(sch, checkArr);
    }

    /**
     * Check row serialization for 256+ fixsized columns.
     */
    @Test
    public void mediumLenWithFixSizedColumns() {
        Column[] keyCols = IntStream.range(0, 300)
                .mapToObj(i -> new Column("keyCol" + i, INT8, false))
                .toArray(Column[]::new);
        Column[] valCols = IntStream.range(0, 330)
                .mapToObj(i -> new Column("valCol" + i, INT8, true))
                .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch);

        checkValues(sch, checkArr);
    }

    /**
     * Check row serialization for 256+ varlen columns.
     */
    @Test
    public void mediumLenWithVarlenColumns() {
        Column[] keyCols = IntStream.range(0, 300)
                .mapToObj(i -> new Column("keyCol" + i, STRING, false))
                .toArray(Column[]::new);
        Column[] valCols = IntStream.range(0, 300)
                .mapToObj(i -> new Column("valCol" + i, STRING, true))
                .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch, t -> randomString(rnd, rnd.nextInt(5)));

        checkValues(sch, checkArr);
    }

    /**
     * Parametrized test for correctness of empty string insertion.
     *
     * @param emptyKey If {@code true} then probes with empty key string.
     * @param emptyVal  If {@code true} then probes with empty value string.
     * @param nullableVal If {@code true} then probes with nullable value string.
     */
    @ParameterizedTest
    @MethodSource("provideStrOrderingAndNulls")
    public void testSingleStringInsertion(boolean emptyKey, boolean emptyVal, boolean nullableVal) {
        Column[] keyCol = {new Column("keyCol", STRING, false)};

        Column[] valColsMulti =
                new Column[]{new Column("valCol1", STRING, nullableVal), new Column("valCol2", STRING, nullableVal)};
        Column[] valColsSingle = new Column[]{new Column("valCol1", STRING, nullableVal)};

        SchemaDescriptor schemaMulti = new SchemaDescriptor(1, keyCol, valColsMulti);
        SchemaDescriptor schemaSingle = new SchemaDescriptor(1, keyCol, valColsSingle);

        Object[] checkArr = generateRowValuesWithEmptyVals(schemaMulti, t -> randomString(rnd, rnd.nextInt(5)), emptyKey, emptyVal);
        checkValues(schemaMulti, checkArr);

        checkArr = generateRowValuesWithEmptyVals(schemaSingle, t -> randomString(rnd, rnd.nextInt(5)), emptyKey, emptyVal);
        checkValues(schemaSingle, checkArr);
    }

    private static Stream<Arguments> provideStrOrderingAndNulls() {
        return Stream.of(
                Arguments.of(true, true, true), // 111
                Arguments.of(true, true, false), // 110
                Arguments.of(true, false, true), // 101
                Arguments.of(true, false, false), // 100

                Arguments.of(false, true, true), // 011
                Arguments.of(false, true, false), // 010
                Arguments.of(false, false, true), // 001
                Arguments.of(false, false, false) // 000
        );
    }

    /**
     * Check row serialization for 64K+ fixlen columns.
     */
    @Test
    public void largeLenWithFixSizedColumns() {
        Column[] keyCols = IntStream.range(0, (2 << 16) + rnd.nextInt(20))
                .mapToObj(i -> new Column("keyCol" + i, INT8, false))
                .toArray(Column[]::new);
        Column[] valCols = IntStream.range(0, (2 << 16) + rnd.nextInt(20))
                .mapToObj(i -> new Column("valCol" + i, INT8, true))
                .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch);

        checkValues(sch, checkArr);
    }

    /**
     * Check row serialization for 1K+ varlen columns with total chunk length of 64k+.
     */
    @Test
    public void largeLenWithVarlenColumns() {
        Column[] keyCols = IntStream.range(0, 1000 + rnd.nextInt(20))
                .mapToObj(i -> new Column("keyCol" + i, STRING, false))
                .toArray(Column[]::new);

        Column[] valCols = IntStream.range(0, 1000 + rnd.nextInt(20))
                .mapToObj(i -> new Column("valCol" + i, STRING, true))
                .toArray(Column[]::new);

        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch, t -> randomString(rnd, 65 + rnd.nextInt(500)));

        checkValues(sch, checkArr);
    }

    /**
     * Checks schema is independent of provided column order.
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
        SchemaDescriptor sch = new SchemaDescriptor(1, keyCols, valCols);

        Object[] checkArr = generateRowValues(sch);

        checkValues(sch, checkArr);

        for (int idx = 0; idx < checkArr.length; idx++) {
            if (!sch.column(idx).nullable()) {
                continue;
            }

            Object prev = checkArr[idx];
            checkArr[idx] = null;

            checkValues(sch, checkArr);

            checkArr[idx] = prev;
        }
    }

    /**
     * Generate row values for given row schema possibly with empty strings.
     *
     * @param schema Row schema.
     * @param rnd   Function that returns random value for the type.
     * @param emptyKey If {@code true} then generates empty key string.
     * @param emptyVal  If {@code true} then generates empty value string.
     * @return Row values.
     */
    private Object[] generateRowValuesWithEmptyVals(
            SchemaDescriptor schema, Function<NativeType, Object> rnd, boolean emptyKey, boolean emptyVal) {
        assert schema.keyColumns().size() == 1;

        Object[] res = new Object[schema.length()];

        for (int i = 0; i < res.length; i++) {
            NativeType type = schema.column(i).type();

            if (emptyKey && (i == 0)) {
                res[i] = "";
            } else if (emptyVal && (i == 1)) {
                res[i] = "";
            } else {
                res[i] = rnd.apply(type);
            }
        }

        return res;
    }

    /**
     * Generate row values for given row schema.
     *
     * @param schema Row schema.
     * @return Row values.
     */
    private Object[] generateRowValues(SchemaDescriptor schema) {
        return generateRowValues(schema, this::generateRandomValue);
    }

    /**
     * Generate row values for given row schema.
     *
     * @param schema Row schema.
     * @param rnd    Function that returns random value for the type.
     * @return Row values.
     */
    private Object[] generateRowValues(SchemaDescriptor schema, Function<NativeType, Object> rnd) {
        Object[] res = new Object[schema.length()];

        for (int i = 0; i < res.length; i++) {
            NativeType type = schema.column(i).type();

            res[i] = rnd.apply(type);
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
        return SchemaTestUtils.generateRandomValue(rnd, type);
    }

    /**
     * Validates row values after serialization-then-deserialization.
     *
     * @param schema Row schema.
     * @param vals   Row values.
     */
    private static void checkValues(SchemaDescriptor schema, Object... vals) {
        assertEquals(schema.keyColumns().size() + schema.valueColumns().size(), vals.length);

        RowAssembler asm = new RowAssembler(schema, -1);

        for (Object val : vals) {
            asm.appendValue(val);
        }

        Row row = Row.wrapBinaryRow(schema, asm.build());

        for (int i = 0; i < vals.length; i++) {
            Column col = schema.column(i);

            NativeTypeSpec type = col.type().spec();

            if (type == NativeTypeSpec.BYTES) {
                assertArrayEquals((byte[]) vals[i], row.bytesValue(i), "Failed for column: " + col);
            } else {
                assertEquals(vals[i], row.value(i), "Failed for column: " + col);
            }
        }
    }

    /**
     * Shuffle columns.
     */
    private void shuffle(Column[] cols) {
        Collections.shuffle(Arrays.asList(cols));
    }
}
