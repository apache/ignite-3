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
import java.util.List;
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
}
