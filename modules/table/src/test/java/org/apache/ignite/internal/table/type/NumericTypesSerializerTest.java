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

package org.apache.ignite.internal.table.type;

import static org.apache.ignite.internal.table.KeyValueTestUtils.createMarshaller;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.binarytuple.BinaryTupleFormatException;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Check numeric typed columns serialization.
 */
public class NumericTypesSerializerTest {
    /** Random. */
    private Random rnd = new Random();

    /**
     * Returns list of string decimal representations for test.
     */
    private static String[] stringDecimalRepresentation() {
        return new String[]{
                "0", "0.00", "123", "-123", "1.23E3", "1.23E+3", "12.3E+7", "12.0", "12.3", "0.00123",
                "-1.23E-12", "1234.5E-4", "0E+7", "-0", "123456789.0123", "123456789.1", "123456789.112312315413",
                "123456789.0123", "123.123456789", "123456789.3210"};
    }

    /**
     * Returns list of pairs to compare byte representation.
     */
    private static List<Pair<BigDecimal, BigDecimal>> sameDecimals() {
        return Arrays.asList(
                new Pair<>(BigDecimal.TEN, BigDecimal.valueOf(10)),
                new Pair<>(new BigDecimal("10.00"), BigDecimal.valueOf(10)),
                new Pair<>(new BigDecimal("999999999"), BigDecimal.valueOf(999999999L)),
                new Pair<>(new BigDecimal("-999999999"), BigDecimal.valueOf(-999999999L)),
                new Pair<>(new BigDecimal("1E3"), BigDecimal.valueOf(1000)),
                new Pair<>(new BigDecimal("1E-3"), new BigDecimal("0.001")),
                new Pair<>(new BigDecimal("0E-3"), new BigDecimal("0.00000")),
                new Pair<>(new BigDecimal("0E-3"), new BigDecimal("0E+3")),
                new Pair<>(new BigDecimal("123.3211"), new BigDecimal("123.321"))
        );
    }

    /**
     * Before each.
     */
    @BeforeEach
    public void setup() {
        long seed = System.currentTimeMillis();

        rnd = new Random(seed);
    }

    @Test
    public void testPrecisionRestrictionsForDecimal() {
        SchemaDescriptor schema = new SchemaDescriptor(
                42,
                new Column[]{new Column("KEY", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("DECIMALCOL", NativeTypes.decimalOf(9, 3), false),
                }
        );

        final Tuple badTup = createTuple().set("key", rnd.nextLong());

        TupleMarshaller marshaller = createMarshaller(schema);

        assertThrowsWithCause(
                () -> marshaller.marshal(badTup.set("decimalCol", new BigDecimal("123456789.0123"))),
                MarshallerException.class,
                "Numeric field overflow"
        );
        assertThrowsWithCause(
                () -> marshaller.marshal(badTup.set("decimalCol", new BigDecimal("-1234567890123"))),
                MarshallerException.class,
                "Numeric field overflow"
        );
        assertThrowsWithCause(
                () -> marshaller.marshal(badTup.set("decimalCol", new BigDecimal("1234567"))),
                MarshallerException.class,
                "Numeric field overflow"
        );
        assertThrowsWithCause(
                () -> marshaller.marshal(badTup.set("decimalCol", new BigDecimal("12345678.9"))),
                MarshallerException.class,
                "Numeric field overflow"
        );
    }

    @Test
    public void testStringDecimalSpecialCase() {
        SchemaDescriptor schema = new SchemaDescriptor(
                42,
                new Column[]{new Column("KEY", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("DECIMALCOL", NativeTypes.decimalOf(1, 0), false),
                }
        );

        // representation of "0000" value.
        final Tuple tup = createTuple().set("key", rnd.nextLong()).set("decimalCol", new BigDecimal("0E+3"));

        TupleMarshaller marshaller = createMarshaller(schema);

        final Row row = marshaller.marshal(tup);

        assertEquals(row.decimalValue(1), BigDecimal.ZERO);
    }

    /**
     * Test.
     */
    @ParameterizedTest
    @MethodSource("stringDecimalRepresentation")
    public void testUpscaleForDecimal(String decimalStr) {
        SchemaDescriptor schema = new SchemaDescriptor(
                42,
                new Column[]{new Column("KEY", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("DECIMALCOL1", NativeTypes.decimalOf(9, 0), false)
                }
        );

        final Tuple tup = createTuple()
                .set("key", rnd.nextLong())
                .set("decimalCol1", new BigDecimal(decimalStr));

        TupleMarshaller marshaller = createMarshaller(schema);

        final Row row = marshaller.marshal(tup);

        assertEquals(row.decimalValue(1), new BigDecimal(decimalStr).setScale(0, RoundingMode.HALF_UP));
    }

    @Test
    public void testDecimalMaxScale() {
        int maxScale = CatalogUtils.MAX_DECIMAL_SCALE;

        SchemaDescriptor schema = new SchemaDescriptor(
                42,
                new Column[]{new Column("KEY", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("DECIMALCOL", NativeTypes.decimalOf(CatalogUtils.MAX_DECIMAL_PRECISION, maxScale), false),
                }
        );

        final Tuple tup = createTuple()
                .set("key", rnd.nextLong())
                .set("decimalCol", BigDecimal.valueOf(123, maxScale));

        TupleMarshaller marshaller = createMarshaller(schema);

        final Row row = marshaller.marshal(tup);

        assertEquals(row.decimalValue(1), BigDecimal.valueOf(123, maxScale));
    }

    @ParameterizedTest
    @CsvSource({
            "32768, Decimal scale is too large: 32768 > 32767",
            "-32769, Decimal scale is too small: -32769 < -32768",
    })
    public void testDecimalScaleTooLarge(int scale, String message) {
        SchemaDescriptor schema = new SchemaDescriptor(
                42,
                new Column[]{new Column("KEY", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("DECIMALCOL", NativeTypes.decimalOf(CatalogUtils.MAX_DECIMAL_PRECISION, scale), false),
                }
        );

        Tuple badTup = createTuple()
                .set("key", rnd.nextLong())
                .set("decimalCol", BigDecimal.valueOf(123, scale));

        TupleMarshaller marshaller = createMarshaller(schema);

        assertThrowsWithCause(
                () -> marshaller.marshal(badTup),
                BinaryTupleFormatException.class,
                message
        );
    }

    /**
     * Test.
     */
    @ParameterizedTest
    @MethodSource("sameDecimals")
    public void testSameBinaryRepresentation(Pair<BigDecimal, BigDecimal> pair) {
        SchemaDescriptor schema = new SchemaDescriptor(
                42,
                new Column[]{new Column("KEY", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("DECIMALCOL", NativeTypes.decimalOf(19, 3), false),
                }
        );

        TupleMarshaller marshaller = createMarshaller(schema);

        long randomKey = rnd.nextLong();

        Tuple firstTup = createTuple().set("key", randomKey).set("decimalCol", pair.getFirst());
        Tuple secondTup = createTuple().set("key", randomKey).set("decimalCol", pair.getSecond());

        Row firstRow = marshaller.marshal(firstTup);
        Row secondRow = marshaller.marshal(secondTup);

        assertEquals(firstRow, secondRow);
    }

    private Tuple createTuple() {
        return Tuple.create();
    }
}
