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
import java.time.ZoneId;
import java.time.temporal.Temporal;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

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
}
