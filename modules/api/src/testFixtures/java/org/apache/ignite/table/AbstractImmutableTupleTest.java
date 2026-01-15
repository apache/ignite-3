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

package org.apache.ignite.table;

import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Base test class for immutable Tuple implementation.
 */
public abstract class AbstractImmutableTupleTest {
    protected static final int NANOS_IN_SECOND = 9;
    protected static final int TIMESTAMP_PRECISION = 6;
    protected static final int TIME_PRECISION = 0;

    protected static final Instant TIMESTAMP_VALUE;
    protected static final LocalDate DATE_VALUE;
    protected static final LocalTime TIME_VALUE;
    protected static final LocalDateTime DATETIME_VALUE;

    protected static final byte[] BYTE_ARRAY_VALUE;
    protected static final String STRING_VALUE;
    protected static final BigInteger BIG_INTEGER_VALUE;
    protected static final BigDecimal BIG_DECIMAL_VALUE;
    protected static final UUID UUID_VALUE = UUID.randomUUID();

    /** The error message displayed when attempting to convert a {@code null} value to a primitive data type. */
    protected static final String NULL_TO_PRIMITIVE_ERROR_MESSAGE =
            "The value of field at index %d is null and cannot be converted to a primitive data type.";

    /** The error message displayed when attempting to convert a null value to a primitive data type. */
    protected static final String NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE =
            "The value of field '%s' is null and cannot be converted to a primitive data type.";

    static {
        Random rnd = new Random();

        byte[] randomBytes = new byte[15];
        rnd.nextBytes(randomBytes);

        TIMESTAMP_VALUE = Instant.now();
        DATE_VALUE = LocalDate.ofInstant(TIMESTAMP_VALUE, ZoneId.systemDefault());
        TIME_VALUE = LocalTime.ofInstant(TIMESTAMP_VALUE, ZoneId.systemDefault());
        DATETIME_VALUE = LocalDateTime.ofInstant(TIMESTAMP_VALUE, ZoneId.systemDefault());
        STRING_VALUE = "🔥 Ignite";
        BYTE_ARRAY_VALUE = randomBytes;
        BIG_INTEGER_VALUE = BigInteger.valueOf(rnd.nextLong());
        BIG_DECIMAL_VALUE = BigDecimal.valueOf(rnd.nextLong(), 5);
    }

    protected abstract Tuple createTuple(Function<Tuple, Tuple> transformer);

    protected Tuple createTuple() {
        return createTuple(Function.identity());
    }

    protected Tuple getTuple() {
        return createTuple(AbstractImmutableTupleTest::addColumnsForDefaultSchema);
    }

    protected Tuple getTupleWithColumnOfAllTypes() {
        return createTuple(AbstractImmutableTupleTest::addColumnOfAllTypes);
    }

    protected abstract Tuple createTupleOfSingleColumn(ColumnType type, String columnName, Object value);

    protected abstract Tuple createNullValueTuple(ColumnType valueType);

    @EnumSource(value = ColumnType.class, mode = Mode.EXCLUDE, names = {"PERIOD", "DURATION", "NULL", "STRUCT"})
    @ParameterizedTest
    public void testTupleColumnsEquality(ColumnType type) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Object value = generateValue(rnd, type);

        Tuple expected = Tuple.create().set("col", value);
        Tuple actual = createTupleOfSingleColumn(type, "COL", value);

        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());
        assertNotEquals(Tuple.create().set("col", null), actual);
    }

    @Test
    public void testValueReturnsValueByName() {
        assertEquals(3L, (Long) getTuple().value("id"));
        assertNull(getTuple().value("noValue"));

        // Ensure unquoted name is case-insensitive
        assertEquals("simple", getTuple().value("simplename"));
        assertEquals("simple", getTuple().value("SimpleName"));
        assertEquals("simple", getTuple().value("SIMPLENAME"));

        assertEquals("simple", getTuple().value("\"SIMPLENAME\""));
        assertThrows(IllegalArgumentException.class, () -> getTuple().value("\"simplename\""));
        assertThrows(IllegalArgumentException.class, () -> getTuple().value("\"SimpleName\""));

        // Ensure quoted name is case-sensitive
        assertEquals("quoted", getTuple().value("\"QuotedName\""));
        assertThrows(IllegalArgumentException.class, () -> getTuple().value("\"Quotedname\""));
        assertThrows(IllegalArgumentException.class, () -> getTuple().value("\"QUOTEDNAME\""));

        assertThrows(IllegalArgumentException.class, () -> getTuple().value("quotedname"));
        assertThrows(IllegalArgumentException.class, () -> getTuple().value("QuotedName"));
        assertThrows(IllegalArgumentException.class, () -> getTuple().value("QUOTEDNAME"));
    }

    @Test
    public void testValueOrDefault() {
        assertEquals(3L, getTuple().valueOrDefault("id", 1L));
        assertNull(getTuple().valueOrDefault("noValue", "default"));

        // Ensure unquoted name is case-insensitive
        assertEquals("simple", getTuple().valueOrDefault("simplename", "default"));
        assertEquals("simple", getTuple().valueOrDefault("SimpleName", "default"));
        ;
        assertEquals("simple", getTuple().valueOrDefault("SIMPLENAME", "default"));

        assertEquals("simple", getTuple().valueOrDefault("\"SIMPLENAME\"", "default"));
        assertEquals("default", getTuple().valueOrDefault("\"simplename\"", "default"));
        assertEquals("default", getTuple().valueOrDefault("\"SimpleName\"", "default"));

        // Ensure quoted name is case-sensitive
        assertEquals("quoted", getTuple().valueOrDefault("\"QuotedName\"", "default"));
        assertEquals("default", getTuple().valueOrDefault("\"Quotedname\"", "default"));
        assertEquals("default", getTuple().valueOrDefault("\"QUOTEDNAME\"", "default"));

        assertEquals("default", getTuple().valueOrDefault("quotedname", "default"));
        assertEquals("default", getTuple().valueOrDefault("QuotedName", "default"));
        assertEquals("default", getTuple().valueOrDefault("QUOTEDNAME", "default"));
    }

    @Test
    public void testValueThrowsOnInvalidColumnName() {
        var ex = assertThrows(IllegalArgumentException.class, () -> getTuple().value("x"));
        assertEquals("Column doesn't exist [name=x]", ex.getMessage());
    }

    @Test
    public void testValueReturnsValueByIndex() {
        assertEquals(3L, (Long) getTuple().value(0));
        assertEquals("simple", getTuple().value(1));
        assertEquals("quoted", getTuple().value(2));
        assertNull(getTuple().value(3));
    }

    @Test
    public void testValueThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(-1));
        assertEquals("Index -1 out of bounds for length 4", ex.getMessage());

        ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(4));
        assertEquals("Index 4 out of bounds for length 4", ex.getMessage());
    }

    @Test
    public void testValueOrDefaultReturnsValueByName() {
        assertEquals(3L, getTuple().valueOrDefault("id", -1L));
        assertEquals("simple", getTuple().valueOrDefault("SimpleName", "y"));
        assertNull(getTuple().valueOrDefault("noValue", "foo"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsNotSet() {
        assertEquals("foo", getTuple().valueOrDefault("x", "foo"));
        assertNull(getTuple().valueOrDefault("x", null));
    }

    @Test
    public void testColumnCount() {
        Tuple tuple = getTuple();

        int columnCount = tuple.columnCount();

        tuple.valueOrDefault("SimpleName", "foo");
        assertEquals(columnCount, tuple.columnCount());

        tuple.valueOrDefault("foo", "bar");
        assertEquals(columnCount, tuple.columnCount());

    }

    /** For non-quoted name, any of then next will be valid: non-quoted string in uppercase or quoted string in uppercase. */
    @Test
    public void testColumnNameReturnsNameByIndex() {
        assertEquals("ID", getTuple().columnName(0));
        assertEquals("SIMPLENAME", getTuple().columnName(1));
        assertEquals("\"QuotedName\"", getTuple().columnName(2));
        assertEquals("NOVALUE", getTuple().columnName(3));
    }

    @Test
    public void testColumnNameThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().columnName(-1));
        assertEquals("Index -1 out of bounds for length 4", ex.getMessage());

        ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().columnName(4));
        assertEquals("Index 4 out of bounds for length 4", ex.getMessage());
    }

    @Test
    public void testColumnIndexReturnsIndexByName() {
        assertEquals(0, getTuple().columnIndex("id"));
        assertEquals(1, getTuple().columnIndex("simplename"));
        assertEquals(1, getTuple().columnIndex("SimpleName"));
        assertEquals(1, getTuple().columnIndex("SIMPLENAME"));
        assertEquals(1, getTuple().columnIndex("\"SIMPLENAME\""));
        assertEquals(2, getTuple().columnIndex("\"QuotedName\""));
    }

    @Test
    public void testColumnResolvableName() {
        Tuple tuple = getTuple();

        assertEquals(0, tuple.columnIndex(tuple.columnName(0)));
        assertEquals(1, tuple.columnIndex(tuple.columnName(1)));
        assertEquals(2, tuple.columnIndex(tuple.columnName(2)));
        assertEquals(3, tuple.columnIndex(tuple.columnName(3)));
    }

    @Test
    public void testColumnIndexForMissingColumns() {
        assertEquals(-1, getTuple().columnIndex("foo"));

        assertEquals(-1, getTuple().columnIndex("\"simplename\""));
        assertEquals(-1, getTuple().columnIndex("\"SimpleName\""));

        assertEquals(-1, getTuple().columnIndex("quotedname"));
        assertEquals(-1, getTuple().columnIndex("QuotedName"));
        assertEquals(-1, getTuple().columnIndex("QUOTEDNAME"));
        assertEquals(-1, getTuple().columnIndex("\"quotedname\""));
        assertEquals(-1, getTuple().columnIndex("\"QUTEDNAME\""));
    }

    @Test
    public void testTupleEquality() {
        assertEquals(getTuple(), getTuple());
        assertEquals(getTupleWithColumnOfAllTypes(), getTupleWithColumnOfAllTypes());
    }

    @Test
    public void testVariousColumnTypes() {
        Tuple tuple = getTupleWithColumnOfAllTypes();

        for (int i = 0; i < tuple.columnCount(); i++) {
            String name = tuple.columnName(i);

            if (tuple.value(i) instanceof byte[]) {
                assertArrayEquals((byte[]) tuple.value(i), tuple.value(tuple.columnIndex(name)), "columnIdx=" + i);
            } else {
                assertEquals((Object) tuple.value(i), tuple.value(tuple.columnIndex(name)), "columnIdx=" + i);
            }
        }
    }

    @Test
    public void testSerialization() throws Exception {
        Tuple tup1 = getTupleWithColumnOfAllTypes();
        Tuple tup2 = deserializeTuple(serializeTuple(tup1));

        assertEquals(tup1, tup2);
        assertEquals(tup2, tup1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessorsUsingFieldIndex")
    public void nullPointerWhenReadingNullAsPrimitive(
            ColumnType type,
            BiConsumer<Tuple, Integer> fieldAccessor
    ) {
        Tuple tuple = createNullValueTuple(type);

        var err = assertThrows(NullPointerException.class, () -> fieldAccessor.accept(tuple, 1));
        assertEquals(String.format(NULL_TO_PRIMITIVE_ERROR_MESSAGE, 1), err.getMessage());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessorsUsingFieldName")
    public void nullPointerWhenReadingNullByNameAsPrimitive(
            ColumnType type,
            BiConsumer<Tuple, String> fieldAccessor
    ) {
        Tuple tuple = createNullValueTuple(type);

        var err = assertThrows(NullPointerException.class, () -> fieldAccessor.accept(tuple, "VAL"));
        assertEquals(String.format(NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE, "VAL"), err.getMessage());
    }

    @Test
    void testReadAsByte() {
        Tuple tuple = createTupleOfSingleColumn(ColumnType.INT8, "INT8", Byte.MAX_VALUE);

        assertThat(tuple.byteValue("INT8"), is(Byte.MAX_VALUE));
        assertThat(tuple.shortValue("INT8"), is((short) Byte.MAX_VALUE));
        assertThat(tuple.intValue("INT8"), is((int) Byte.MAX_VALUE));
        assertThat(tuple.longValue("INT8"), is((long) Byte.MAX_VALUE));

        assertThat(tuple.byteValue(0), is(Byte.MAX_VALUE));
        assertThat(tuple.shortValue(0), is((short) Byte.MAX_VALUE));
        assertThat(tuple.intValue(0), is((int) Byte.MAX_VALUE));
        assertThat(tuple.longValue(0), is((long) Byte.MAX_VALUE));
    }

    @Test
    void testReadAsShort() {
        // The field value is within the byte range
        {
            String columnName = "INT16";
            short value = Byte.MAX_VALUE;
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT16, columnName, value);

            assertThat(tuple.byteValue(columnName), is((byte) value));
            assertThat(tuple.shortValue(columnName), is(value));
            assertThat(tuple.intValue(columnName), is((int) value));
            assertThat(tuple.longValue(columnName), is((long) value));

            assertThat(tuple.byteValue(0), is((byte) value));
            assertThat(tuple.shortValue(0), is(value));
            assertThat(tuple.intValue(0), is((int) value));
            assertThat(tuple.longValue(0), is((long) value));
        }

        // The field value is out of the byte range.
        {
            String columnName = "INT16";
            short value = Byte.MAX_VALUE + 1;
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT16, columnName, value);

            ArithmeticException ex0 = assertThrows(ArithmeticException.class, () -> tuple.byteValue(columnName));
            assertThat(ex0.getMessage(), equalTo("Byte value overflow: " + value));

            assertThat(tuple.shortValue(columnName), is(value));
            assertThat(tuple.intValue(columnName), is((int) value));
            assertThat(tuple.longValue(columnName), is((long) value));

            ArithmeticException ex1 = assertThrows(ArithmeticException.class, () -> tuple.byteValue(0));
            assertThat(ex1.getMessage(), equalTo("Byte value overflow: " + value));

            assertThat(tuple.shortValue(0), is(value));
            assertThat(tuple.intValue(0), is((int) value));
            assertThat(tuple.longValue(0), is((long) value));
        }
    }

    @Test
    void testReadAsInt() {
        {
            int value = Byte.MAX_VALUE;
            String columnName = "VALUE";
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT32, columnName, value);

            assertThat(tuple.byteValue(columnName), is((byte) value));
            assertThat(tuple.shortValue(columnName), is((short) value));
            assertThat(tuple.intValue(columnName), is(value));
            assertThat(tuple.longValue(columnName), is((long) value));

            assertThat(tuple.byteValue(0), is((byte) value));
            assertThat(tuple.shortValue(0), is((short) value));
            assertThat(tuple.intValue(0), is(value));
            assertThat(tuple.longValue(0), is((long) value));
        }

        {
            int value = Byte.MAX_VALUE + 1;
            String columnName = "VALUE";
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT32, columnName, value);

            ArithmeticException ex0 = assertThrows(ArithmeticException.class, () -> tuple.byteValue(columnName));
            assertThat(ex0.getMessage(), equalTo("Byte value overflow: " + value));

            assertThat(tuple.shortValue(columnName), is((short) value));
            assertThat(tuple.intValue(columnName), is(value));
            assertThat(tuple.longValue(columnName), is((long) value));

            ArithmeticException ex1 = assertThrows(ArithmeticException.class, () -> tuple.byteValue(0));
            assertThat(ex1.getMessage(), equalTo("Byte value overflow: " + value));

            assertThat(tuple.shortValue(0), is((short) value));
            assertThat(tuple.intValue(0), is(value));
            assertThat(tuple.longValue(0), is((long) value));
        }

        {
            int value = Short.MAX_VALUE + 1;
            String columnName = "VALUE";
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT32, columnName, value);

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.byteValue(columnName));
                assertThat(ex.getMessage(), equalTo("Byte value overflow: " + value));
            }

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.shortValue(columnName));
                assertThat(ex.getMessage(), equalTo("Short value overflow: " + value));
            }

            assertThat(tuple.intValue(columnName), is(value));
            assertThat(tuple.longValue(columnName), is((long) value));

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.byteValue(0));
                assertThat(ex.getMessage(), equalTo("Byte value overflow: " + value));
            }

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.shortValue(0));
                assertThat(ex.getMessage(), equalTo("Short value overflow: " + value));
            }

            assertThat(tuple.intValue(0), is(value));
            assertThat(tuple.longValue(0), is((long) value));
        }
    }

    @Test
    void testReadAsLong() {
        {
            long value = Byte.MAX_VALUE;
            String columnName = "VALUE";
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT64, columnName, value);

            assertThat(tuple.byteValue(columnName), is((byte) value));
            assertThat(tuple.shortValue(columnName), is((short) value));
            assertThat(tuple.intValue(columnName), is((int) value));
            assertThat(tuple.longValue(columnName), is(value));

            assertThat(tuple.byteValue(0), is((byte) value));
            assertThat(tuple.shortValue(0), is((short) value));
            assertThat(tuple.intValue(0), is((int) value));
            assertThat(tuple.longValue(0), is(value));
        }

        {
            long value = Byte.MAX_VALUE + 1;
            String columnName = "VALUE";
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT64, columnName, value);

            ArithmeticException ex0 = assertThrows(ArithmeticException.class, () -> tuple.byteValue(columnName));
            assertThat(ex0.getMessage(), equalTo("Byte value overflow: " + value));

            assertThat(tuple.shortValue(columnName), is((short) value));
            assertThat(tuple.intValue(columnName), is((int) value));
            assertThat(tuple.longValue(columnName), is(value));

            ArithmeticException ex1 = assertThrows(ArithmeticException.class, () -> tuple.byteValue(0));
            assertThat(ex1.getMessage(), equalTo("Byte value overflow: " + value));

            assertThat(tuple.shortValue(0), is((short) value));
            assertThat(tuple.intValue(0), is((int) value));
            assertThat(tuple.longValue(0), is(value));
        }

        {
            long value = Short.MAX_VALUE + 1;
            String columnName = "VALUE";
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT64, columnName, value);

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.byteValue(columnName));
                assertThat(ex.getMessage(), equalTo("Byte value overflow: " + value));
            }

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.shortValue(columnName));
                assertThat(ex.getMessage(), equalTo("Short value overflow: " + value));
            }

            assertThat(tuple.intValue(columnName), is((int) value));
            assertThat(tuple.longValue(columnName), is(value));

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.byteValue(0));
                assertThat(ex.getMessage(), equalTo("Byte value overflow: " + value));
            }

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.shortValue(0));
                assertThat(ex.getMessage(), equalTo("Short value overflow: " + value));
            }

            assertThat(tuple.intValue(0), is((int) value));
            assertThat(tuple.longValue(0), is(value));
        }

        {
            long value = Integer.MAX_VALUE + 1L;
            String columnName = "VALUE";
            Tuple tuple = createTupleOfSingleColumn(ColumnType.INT64, columnName, value);

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.byteValue(columnName));
                assertThat(ex.getMessage(), equalTo("Byte value overflow: " + value));
            }

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.shortValue(columnName));
                assertThat(ex.getMessage(), equalTo("Short value overflow: " + value));
            }

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.intValue(columnName));
                assertThat(ex.getMessage(), equalTo("Int value overflow: " + value));
            }

            assertThat(tuple.longValue(columnName), is(value));

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.byteValue(0));
                assertThat(ex.getMessage(), equalTo("Byte value overflow: " + value));
            }

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.shortValue(0));
                assertThat(ex.getMessage(), equalTo("Short value overflow: " + value));
            }

            {
                ArithmeticException ex = assertThrows(ArithmeticException.class, () -> tuple.intValue(0));
                assertThat(ex.getMessage(), equalTo("Int value overflow: " + value));
            }

            assertThat(tuple.longValue(0), is(value));
        }
    }

    @Test
    void testReadAsFloat() {
        {
            Tuple tuple = createTupleOfSingleColumn(ColumnType.FLOAT, "FLOAT", Float.MAX_VALUE);

            assertThat(tuple.floatValue("FLOAT"), is(Float.MAX_VALUE));
            assertThat(tuple.doubleValue("FLOAT"), is((double) Float.MAX_VALUE));

            assertThat(tuple.floatValue(0), is(Float.MAX_VALUE));
            assertThat(tuple.doubleValue(0), is((double) Float.MAX_VALUE));
        }

        // NaN
        {
            Tuple tuple = createTupleOfSingleColumn(ColumnType.FLOAT, "FLOAT", Float.NaN);

            assertThat(Float.isNaN(tuple.floatValue("FLOAT")), is(true));
            assertThat(Double.isNaN(tuple.doubleValue("FLOAT")), is(true));

            assertThat(Float.isNaN(tuple.floatValue(0)), is(true));
            assertThat(Double.isNaN(tuple.doubleValue(0)), is(true));
        }

        // Positive infinity
        {
            Tuple tuple = createTupleOfSingleColumn(ColumnType.FLOAT, "FLOAT", Float.POSITIVE_INFINITY);

            assertThat(tuple.floatValue("FLOAT"), is(Float.POSITIVE_INFINITY));
            assertThat(tuple.doubleValue("FLOAT"), is(Double.POSITIVE_INFINITY));

            assertThat(tuple.floatValue(0), is(Float.POSITIVE_INFINITY));
            assertThat(tuple.doubleValue(0), is(Double.POSITIVE_INFINITY));
        }

        // Negative infinity
        {
            Tuple tuple = createTupleOfSingleColumn(ColumnType.FLOAT, "FLOAT", Float.NEGATIVE_INFINITY);

            assertThat(tuple.floatValue("FLOAT"), is(Float.NEGATIVE_INFINITY));
            assertThat(tuple.doubleValue("FLOAT"), is(Double.NEGATIVE_INFINITY));

            assertThat(tuple.floatValue(0), is(Float.NEGATIVE_INFINITY));
            assertThat(tuple.doubleValue(0), is(Double.NEGATIVE_INFINITY));
        }
    }

    @Test
    void testReadAsDouble() {
        String columnName = "DOUBLE";

        // The field value can be represented as float.
        {
            double value = Float.MAX_VALUE;
            Tuple tuple = createTupleOfSingleColumn(ColumnType.DOUBLE, columnName, value);

            assertThat(tuple.floatValue(columnName), is((float) value));
            assertThat(tuple.floatValue(0), is((float) value));

            assertThat(tuple.doubleValue(columnName), is(value));
            assertThat(tuple.doubleValue(0), is(value));
        }

        // The field value cannot be represented as float.
        {
            double value = Double.MAX_VALUE;
            Tuple tuple = createTupleOfSingleColumn(ColumnType.DOUBLE, columnName, value);

            ArithmeticException ex0 = assertThrows(ArithmeticException.class, () -> tuple.floatValue(columnName));
            assertThat(ex0.getMessage(), equalTo("Float value overflow: " + value));

            ArithmeticException ex1 = assertThrows(ArithmeticException.class, () -> tuple.floatValue(0));
            assertThat(ex1.getMessage(), equalTo("Float value overflow: " + value));

            assertThat(tuple.doubleValue(columnName), is(value));
            assertThat(tuple.doubleValue(0), is(value));
        }

        // NaN
        {
            Tuple tuple = createTupleOfSingleColumn(ColumnType.DOUBLE, columnName, Double.NaN);

            assertThat(Float.isNaN(tuple.floatValue(columnName)), is(true));
            assertThat(Double.isNaN(tuple.doubleValue(columnName)), is(true));

            assertThat(Float.isNaN(tuple.floatValue(0)), is(true));
            assertThat(Double.isNaN(tuple.doubleValue(0)), is(true));
        }

        // Positive infinity
        {
            Tuple tuple = createTupleOfSingleColumn(ColumnType.DOUBLE, columnName, Double.POSITIVE_INFINITY);

            assertThat(tuple.floatValue(columnName), is(Float.POSITIVE_INFINITY));
            assertThat(tuple.doubleValue(columnName), is(Double.POSITIVE_INFINITY));

            assertThat(tuple.floatValue(0), is(Float.POSITIVE_INFINITY));
            assertThat(tuple.doubleValue(0), is(Double.POSITIVE_INFINITY));
        }

        // Negative infinity
        {
            Tuple tuple = createTupleOfSingleColumn(ColumnType.DOUBLE, columnName, Double.NEGATIVE_INFINITY);

            assertThat(tuple.floatValue(columnName), is(Float.NEGATIVE_INFINITY));
            assertThat(tuple.doubleValue(columnName), is(Double.NEGATIVE_INFINITY));

            assertThat(tuple.floatValue(0), is(Float.NEGATIVE_INFINITY));
            assertThat(tuple.doubleValue(0), is(Double.NEGATIVE_INFINITY));
        }
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("allTypesUnsupportedConversionArgs")
    public void allTypesUnsupportedConversion(ColumnType from, ColumnType to) {
        Object value = generateMaxValue(from);
        String columnName = "VALUE";
        Tuple tuple = createTupleOfSingleColumn(from, columnName, value);

        {
            ClassCastException ex = assertThrows(ClassCastException.class, () -> readValue(tuple, to, columnName, null));
            String template = "Column with name '%s' has type %s but %s was requested";
            assertThat(ex.getMessage(), containsString(String.format(template, columnName, from.name(), to.name())));
        }

        {
            ClassCastException ex = assertThrows(ClassCastException.class, () -> readValue(tuple, to, null, 0));
            String template = "Column with index %d has type %s but %s was requested";
            assertThat(ex.getMessage(), containsString(String.format(template, 0, from.name(), to.name())));
        }
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("allTypesDowncastOverflowArgs")
    public void allTypesDowncastOverflow(ColumnType from, ColumnType to) {
        Object value = generateMaxValue(from);
        String columnName = "VALUE";
        Tuple tuple = createTupleOfSingleColumn(from, columnName, value);

        assertThrows(ArithmeticException.class, () -> readValue(tuple, to, columnName, null));
        assertThrows(ArithmeticException.class, () -> readValue(tuple, to, null, 0));
    }

    /**
     * Adds sample values for columns of default schema: id (long), simpleName (string), "QuotedName" (string), noValue (null).
     *
     * @return A given tuple for chaining.
     */
    protected static Tuple addColumnsForDefaultSchema(Tuple tuple) {
        return tuple
                .set("id", 3L)
                .set("SimpleName", "simple")
                .set("\"QuotedName\"", "quoted")
                .set("noValue", null);

    }

    protected static Tuple addColumnOfAllTypes(Tuple tuple) {
        return tuple
                .set("valBoolCol", true)
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("valUuidCol", UUID_VALUE)
                .set("valDateCol", LocalDate.ofInstant(TIMESTAMP_VALUE, ZoneId.systemDefault()))
                .set("valDateTimeCol", truncateToDefaultPrecision(LocalDateTime.ofInstant(TIMESTAMP_VALUE, ZoneId.systemDefault())))
                .set("valTimeCol", truncateToDefaultPrecision(LocalTime.ofInstant(TIMESTAMP_VALUE, ZoneId.systemDefault())))
                .set("valTimeStampCol", truncateToDefaultPrecision(TIMESTAMP_VALUE))
                .set("valBytesCol", BYTE_ARRAY_VALUE)
                .set("valStringCol", STRING_VALUE)
                .set("valDecimalCol", BIG_DECIMAL_VALUE);
    }

    /**
     * Deserializes tuple.
     *
     * @param data Tuple bytes.
     * @return Tuple.
     * @throws Exception If failed.
     */
    protected static Tuple deserializeTuple(byte[] data) throws Exception {
        try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (Tuple) is.readObject();
        }
    }

    /**
     * Serailizes tuple.
     *
     * @param tup Tuple.
     * @return Tuple bytes.
     * @throws Exception If failed.
     */
    protected static byte[] serializeTuple(Tuple tup) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(tup);
        }

        return baos.toByteArray();
    }

    private static <T extends Temporal> T truncateToDefaultPrecision(T temporal) {
        int precision = temporal instanceof Instant ? TIMESTAMP_PRECISION
                : TIME_PRECISION;

        return (T) temporal.with(NANO_OF_SECOND,
                truncatePrecision(temporal.get(NANO_OF_SECOND), tailFactor(precision)));
    }

    private static int truncatePrecision(int nanos, int factor) {
        return nanos / factor * factor;
    }

    private static int tailFactor(int precision) {
        if (precision >= NANOS_IN_SECOND) {
            return 1;
        }

        return new BigInteger("10").pow(NANOS_IN_SECOND - precision).intValue();
    }

    private static @Nullable Object generateValue(Random rnd, ColumnType type) {
        switch (type) {
            case NULL:
                return null;
            case BOOLEAN:
                return rnd.nextBoolean();
            case INT8:
                return (byte) rnd.nextInt(255);
            case INT16:
                return (short) rnd.nextInt(65535);
            case INT32:
                return rnd.nextInt();
            case INT64:
                return rnd.nextLong();
            case FLOAT:
                return rnd.nextFloat();
            case DOUBLE:
                return rnd.nextDouble();
            case DECIMAL:
                return BigDecimal.valueOf(rnd.nextInt(), 5);
            case DATE: {
                Year year = Year.of(1 + rnd.nextInt(9998));
                return LocalDate.ofYearDay(year.getValue(), rnd.nextInt(year.length()) + 1);
            }
            case TIME:
                return LocalTime.of(rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60),
                        rnd.nextInt(1_000_000) * 1000);
            case DATETIME:
                return LocalDateTime.ofInstant(generateInstant(rnd), ZoneOffset.UTC);

            case TIMESTAMP:
                return generateInstant(rnd);

            case UUID:
                return new UUID(rnd.nextLong(), rnd.nextLong());

            case STRING: {
                int length = rnd.nextInt(256);

                StringBuilder sb = new StringBuilder(length);
                IntStream.generate(() -> rnd.nextInt(Character.MAX_VALUE + 1))
                        .filter(c -> !Character.isSurrogate((char) c))
                        .limit(length)
                        .forEach(sb::appendCodePoint);

                return sb.toString();
            }
            case BYTE_ARRAY: {
                int length = rnd.nextInt(256);
                byte[] bytes = new byte[length];
                rnd.nextBytes(bytes);
                return bytes;
            }
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static Object generateMaxValue(ColumnType type) {
        switch (type) {
            case INT8:
                return Byte.MAX_VALUE;
            case INT16:
                return Short.MAX_VALUE;
            case INT32:
                return Integer.MAX_VALUE;
            case INT64:
                return Long.MAX_VALUE;
            case FLOAT:
                return Float.MAX_VALUE;
            case DOUBLE:
                return Double.MAX_VALUE;
            default:
                return Objects.requireNonNull(generateValue(ThreadLocalRandom.current(), type));
        }
    }

    private static Instant generateInstant(Random rnd) {
        long minTs = LocalDateTime.of(LocalDate.of(1, 1, 1), LocalTime.MIN)
                .minusSeconds(ZoneOffset.MIN.getTotalSeconds()).toInstant(ZoneOffset.UTC).toEpochMilli();
        long maxTs = LocalDateTime.of(LocalDate.of(9999, 12, 31), LocalTime.MAX)
                .minusSeconds(ZoneOffset.MAX.getTotalSeconds()).toInstant(ZoneOffset.UTC).toEpochMilli();

        return Instant.ofEpochMilli(minTs + (long) (rnd.nextDouble() * (maxTs - minTs)));
    }

    private static List<Arguments> primitiveAccessorsUsingFieldIndex() {
        return List.of(
                Arguments.of(ColumnType.BOOLEAN, (BiConsumer<Tuple, Integer>) Tuple::booleanValue),
                Arguments.of(ColumnType.INT8, (BiConsumer<Tuple, Integer>) Tuple::byteValue),
                Arguments.of(ColumnType.INT16, (BiConsumer<Tuple, Integer>) Tuple::shortValue),
                Arguments.of(ColumnType.INT32, (BiConsumer<Tuple, Integer>) Tuple::intValue),
                Arguments.of(ColumnType.INT64, (BiConsumer<Tuple, Integer>) Tuple::longValue),
                Arguments.of(ColumnType.FLOAT, (BiConsumer<Tuple, Integer>) Tuple::floatValue),
                Arguments.of(ColumnType.DOUBLE, (BiConsumer<Tuple, Integer>) Tuple::doubleValue)
        );
    }

    private static List<Arguments> primitiveAccessorsUsingFieldName() {
        return List.of(
                Arguments.of(ColumnType.BOOLEAN, (BiConsumer<Tuple, String>) Tuple::booleanValue),
                Arguments.of(ColumnType.INT8, (BiConsumer<Tuple, String>) Tuple::byteValue),
                Arguments.of(ColumnType.INT16, (BiConsumer<Tuple, String>) Tuple::shortValue),
                Arguments.of(ColumnType.INT32, (BiConsumer<Tuple, String>) Tuple::intValue),
                Arguments.of(ColumnType.INT64, (BiConsumer<Tuple, String>) Tuple::longValue),
                Arguments.of(ColumnType.FLOAT, (BiConsumer<Tuple, String>) Tuple::floatValue),
                Arguments.of(ColumnType.DOUBLE, (BiConsumer<Tuple, String>) Tuple::doubleValue)
        );
    }

    private static Object readValue(Tuple tuple, ColumnType type, @Nullable String colName, @Nullable Integer index) {
        assert colName == null ^ index == null;

        switch (type) {
            case TIME:
                return colName == null ? tuple.timeValue(index) : tuple.timeValue(colName);
            case TIMESTAMP:
                return colName == null ? tuple.timestampValue(index) : tuple.timestampValue(colName);
            case DATE:
                return colName == null ? tuple.dateValue(index) : tuple.dateValue(colName);
            case DATETIME:
                return colName == null ? tuple.datetimeValue(index) : tuple.datetimeValue(colName);
            case INT8:
                return colName == null ? tuple.byteValue(index) : tuple.byteValue(colName);
            case INT16:
                return colName == null ? tuple.shortValue(index) : tuple.shortValue(colName);
            case INT32:
                return colName == null ? tuple.intValue(index) : tuple.intValue(colName);
            case INT64:
                return colName == null ? tuple.longValue(index) : tuple.longValue(colName);
            case FLOAT:
                return colName == null ? tuple.floatValue(index) : tuple.floatValue(colName);
            case DOUBLE:
                return colName == null ? tuple.doubleValue(index) : tuple.doubleValue(colName);
            case UUID:
                return colName == null ? tuple.uuidValue(index) : tuple.uuidValue(colName);
            case STRING:
                return colName == null ? tuple.stringValue(index) : tuple.stringValue(colName);
            case BOOLEAN:
                return colName == null ? tuple.booleanValue(index) : tuple.booleanValue(colName);
            case DECIMAL:
                return colName == null ? tuple.decimalValue(index) : tuple.decimalValue(colName);
            case BYTE_ARRAY:
                return colName == null ? tuple.bytesValue(index) : tuple.bytesValue(colName);
            default:
                throw new UnsupportedOperationException("Unexpected type: " + type);
        }
    }

    private static List<Arguments> allTypesUnsupportedConversionArgs() {
        EnumSet<ColumnType> allTypes = EnumSet.complementOf(
                EnumSet.of(ColumnType.NULL, ColumnType.STRUCT, ColumnType.PERIOD, ColumnType.DURATION));
        List<Arguments> arguments = new ArrayList<>();

        for (ColumnType from : allTypes) {
            for (ColumnType to : allTypes) {
                if (from == to || isSupportedUpcast(from, to) || isSupportedDowncast(from, to)) {
                    continue;
                }

                arguments.add(Arguments.of(from, to));
            }
        }

        return arguments;
    }

    private static List<Arguments> allTypesDowncastOverflowArgs() {
        EnumSet<ColumnType> allTypes = EnumSet.complementOf(
                EnumSet.of(ColumnType.NULL, ColumnType.STRUCT, ColumnType.PERIOD, ColumnType.DURATION));
        List<Arguments> arguments = new ArrayList<>();

        for (ColumnType from : allTypes) {
            for (ColumnType to : allTypes) {
                if (isSupportedDowncast(from, to)) {
                    arguments.add(Arguments.of(from, to));
                }
            }
        }

        return arguments;
    }

    private static boolean isSupportedUpcast(ColumnType source, ColumnType target) {
        switch (source) {
            case INT8:
                return target == ColumnType.INT16 || target == ColumnType.INT32 || target == ColumnType.INT64;
            case INT16:
                return target == ColumnType.INT32 || target == ColumnType.INT64;
            case INT32:
                return target == ColumnType.INT64;
            case FLOAT:
                return target == ColumnType.DOUBLE;
            default:
                return false;
        }
    }

    private static boolean isSupportedDowncast(ColumnType source, ColumnType target) {
        switch (source) {
            case INT64:
                return target == ColumnType.INT8 || target == ColumnType.INT16 || target == ColumnType.INT32;
            case INT32:
                return target == ColumnType.INT8 || target == ColumnType.INT16;
            case INT16:
                return target == ColumnType.INT8;
            case DOUBLE:
                return target == ColumnType.FLOAT;
            default:
                return false;
        }
    }
}
