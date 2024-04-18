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

package org.apache.ignite.internal.table;

import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.type.NativeTypes.BYTES;
import static org.apache.ignite.internal.type.NativeTypes.DATE;
import static org.apache.ignite.internal.type.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.type.NativeTypes.FLOAT;
import static org.apache.ignite.internal.type.NativeTypes.INT16;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.INT8;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.datetime;
import static org.apache.ignite.internal.type.NativeTypes.time;
import static org.apache.ignite.internal.type.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.time.temporal.Temporal;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests server tuple builder implementation.
 *
 * <p>Should be in sync with org.apache.ignite.client.ClientTupleBuilderTest.
 */
public class MutableRowTupleAdapterTest {
    private static final int NANOS_IN_SECOND = 9;
    private static final int TIMESTAMP_PRECISION = 6;
    private static final int TIME_PRECISION = 0;

    /** Schema descriptor. */
    private final SchemaDescriptor schema = new SchemaDescriptor(
            42,
            new Column[]{new Column("id".toUpperCase(), INT64, false)},
            new Column[]{new Column("name".toUpperCase(), STRING, false)}
    );

    /** Schema descriptor. */
    private final SchemaDescriptor fullSchema = new SchemaDescriptor(42,
            List.of(
                    new Column("valByteCol".toUpperCase(), INT8, true),
                    new Column("valShortCol".toUpperCase(), INT16, true),
                    new Column("valIntCol".toUpperCase(), INT32, true),
                    new Column("valLongCol".toUpperCase(), INT64, true),
                    new Column("valFloatCol".toUpperCase(), FLOAT, true),
                    new Column("valDoubleCol".toUpperCase(), DOUBLE, true),
                    new Column("valDateCol".toUpperCase(), DATE, true),
                    new Column("keyUuidCol".toUpperCase(), NativeTypes.UUID, false),
                    new Column("valTimeCol".toUpperCase(), time(TIME_PRECISION), true),
                    new Column("valDateTimeCol".toUpperCase(), datetime(TIMESTAMP_PRECISION), true),
                    new Column("valTimeStampCol".toUpperCase(), timestamp(TIMESTAMP_PRECISION), true),
                    new Column("valBitmask1Col".toUpperCase(), NativeTypes.bitmaskOf(22), true),
                    new Column("valBytesCol".toUpperCase(), BYTES, false),
                    new Column("valStringCol".toUpperCase(), STRING, false),
                    new Column("valNumberCol".toUpperCase(), NativeTypes.numberOf(20), false),
                    new Column("valDecimalCol".toUpperCase(), NativeTypes.decimalOf(25, 5), false)
            ),
            List.of("keyUuidCol".toUpperCase()),
            null
    );

    @Test
    public void testValueReturnsValueByName() {
        assertEquals(3L, (Long) getTuple().value("id"));
        assertEquals("Shirt", getTuple().value("name"));
    }

    @Test
    public void testValueThrowsOnInvalidColumnName() {
        var ex = assertThrows(IllegalArgumentException.class, () -> getTuple().value("x"));
        assertEquals("Invalid column name: columnName=x", ex.getMessage());
    }

    @Test
    public void testValueReturnsValueByIndex() {
        assertEquals(3L, (Long) getTuple().value(0));
        assertEquals("Shirt", getTuple().value(1));
    }

    @Test
    public void testValueThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(-1));
        assertEquals("Index -1 out of bounds for length 2", ex.getMessage());

        ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(3));
        assertEquals("Index 3 out of bounds for length 2", ex.getMessage());
    }

    @Test
    public void testValueOrDefaultReturnsValueByName() {
        assertEquals(3L, getTuple().valueOrDefault("id", -1L));
        assertEquals("Shirt", getTuple().valueOrDefault("name", "y"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsNotSet() {
        assertEquals("foo", getTuple().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueReturnsOverwrittenValue() {
        assertEquals("foo", getTuple().set("name", "foo").value("name"));
        assertEquals("foo", getTuple().set("name", "foo").value(1));

        assertEquals("foo", getTuple().set("name", "foo").valueOrDefault("name", "bar"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsSetToNull() {
        assertNull(getTuple().set("name", null).valueOrDefault("name", "foo"));
    }

    @Test
    public void testColumnCountReturnsSchemaSize() {
        assertEquals(2, getTuple().columnCount());

        Tuple tuple = getTuple();

        assertEquals(2, tuple.columnCount());
        assertEquals(2, tuple.set("id", -1).columnCount());

        tuple.valueOrDefault("name", "foo");
        assertEquals(2, tuple.columnCount());

        tuple.valueOrDefault("foo", "bar");
        assertEquals(2, tuple.columnCount());

        tuple.set("foo", "bar");
        assertEquals(3, tuple.columnCount());
    }

    @Test
    public void testColumnNameReturnsNameByIndex() {
        assertEquals("ID", getTuple().columnName(0));
        assertEquals("NAME", getTuple().columnName(1));
    }

    @Test
    public void testColumnNameThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().columnName(-1));
        assertEquals("Index -1 out of bounds for length 2", ex.getMessage());

        ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().columnName(3));
        assertEquals("Index 3 out of bounds for length 2", ex.getMessage());
    }

    @Test
    public void testColumnIndexReturnsIndexByName() {
        assertEquals(0, getTuple().columnIndex("id"));
        assertEquals(1, getTuple().columnIndex("name"));
    }

    @Test
    public void testColumnIndexForMissingColumns() {
        assertEquals(-1, getTuple().columnIndex("foo"));
    }

    @Test
    public void testKeyValueChunks() throws TupleMarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(
                42,
                List.of(
                        new Column("ID2", INT64, false),
                        new Column("NAME", STRING, true),
                        new Column("PRICE", DOUBLE, true),
                        new Column("ID1", INT64, false)
                ),
                List.of("ID1", "ID2"),
                null
        );

        Tuple original = Tuple.create()
                .set("id1", 3L)
                .set("name", "Shirt")
                .set("price", 5.99d)
                .set("id2", 33L);

        TupleMarshaller marshaller = new TupleMarshallerImpl(schema);

        Row row = marshaller.marshal(original);

        Tuple key = TableRow.keyTuple(row);
        Tuple val = TableRow.valueTuple(row);

        assertEquals(3L, (Long) key.value("id1"));
        assertEquals(3L, (Long) key.value(0));

        assertEquals(33L, (Long) key.value("id2"));
        assertEquals(33L, (Long) key.value(1));

        assertEquals("Shirt", val.value("name"));
        assertEquals("Shirt", val.value(0));

        assertEquals(5.99d, val.value("price"));
        assertEquals(5.99d, val.value(1));

        // Wrong columns.
        assertThrows(IndexOutOfBoundsException.class, () -> key.value(2));
        assertThrows(IllegalArgumentException.class, () -> key.value("price"));

        assertThrows(IndexOutOfBoundsException.class, () -> val.value(2));
        assertThrows(IllegalArgumentException.class, () -> val.value("id"));
    }

    @Test
    public void testRowTupleMutability() throws TupleMarshallerException {
        TupleMarshaller marshaller = new TupleMarshallerImpl(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("name", "Shirt"));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        final Tuple val = TableRow.valueTuple(row);

        tuple.set("id", 2L);

        assertEquals(2L, (Long) tuple.value("id"));
        assertEquals(1L, (Long) key.value("id"));

        tuple.set("name", "noname");

        assertEquals("noname", tuple.value("name"));
        assertEquals("Shirt", val.value("name"));

        tuple.set("foo", "bar");

        assertEquals("bar", tuple.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> key.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> val.value("foo"));
    }

    @Test
    public void testKeyValueTupleMutability() throws TupleMarshallerException {
        TupleMarshaller marshaller = new TupleMarshallerImpl(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("name", "Shirt"));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        final Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        key.set("id", 3L);

        assertEquals(3L, (Long) key.value("id"));
        assertEquals(1L, (Long) tuple.value("id"));

        val.set("name", "noname");

        assertEquals("noname", val.value("name"));
        assertEquals("Shirt", tuple.value("name"));

        val.set("foo", "bar");

        assertEquals("bar", val.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> key.value("foo"));
        assertThrows(IllegalArgumentException.class, () -> tuple.value("foo"));
    }

    @Test
    public void testRowTupleSchemaAwareness() throws TupleMarshallerException {
        TupleMarshaller marshaller = new TupleMarshallerImpl(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("name", "Shirt"));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        final Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        assertNotNull(((SchemaAware) tuple).schema());
        assertNotNull(((SchemaAware) key).schema());
        assertNotNull(((SchemaAware) val).schema());

        tuple.set("name", "noname");

        assertNull(((SchemaAware) tuple).schema());
        assertNotNull(((SchemaAware) key).schema());
        assertNotNull(((SchemaAware) val).schema());
    }

    @Test
    public void testKeyValueTupleSchemaAwareness() throws TupleMarshallerException {
        TupleMarshaller marshaller = new TupleMarshallerImpl(schema);

        Row row = marshaller.marshal(Tuple.create().set("id", 1L).set("name", "Shirt"));

        Tuple tuple = TableRow.tuple(row);
        Tuple key = TableRow.keyTuple(row);
        final Tuple val = TableRow.valueTuple(row);

        assertTrue(tuple instanceof SchemaAware);

        key.set("foo", "bar");

        assertNotNull(((SchemaAware) tuple).schema());
        assertNull(((SchemaAware) key).schema());
        assertNotNull(((SchemaAware) val).schema());

        val.set("id", 1L);

        assertNotNull(((SchemaAware) tuple).schema());
        assertNull(((SchemaAware) key).schema());
        assertNull(((SchemaAware) val).schema());
    }

    @Test
    public void testVariousColumnTypes() throws TupleMarshallerException {
        Random rnd = new Random();

        TupleMarshaller marshaller = new TupleMarshallerImpl(fullSchema);

        Tuple tuple = Tuple.create()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("keyUuidCol", UUID.randomUUID())
                .set("valDateCol", LocalDate.now())
                .set("valDateTimeCol", truncatedLocalDateTimeNow())
                .set("valTimeCol", truncatedLocalTimeNow())
                .set("valTimeStampCol", truncatedInstantNow())
                .set("valBitmask1Col", randomBitSet(rnd, 12))
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

        Tuple rowTuple = TableRow.tuple(marshaller.marshal(tuple));

        assertEquals(tuple, rowTuple);

        rowTuple.set("foo", "bar"); // Force row to tuple conversion.
        tuple.set("foo", "bar"); // Force row to tuple conversion.

        assertEquals(tuple, rowTuple);
    }

    @Test
    public void testSerialization() throws Exception {
        Random rnd = new Random();

        Tuple tup1 = Tuple.create()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("keyUuidCol", UUID.randomUUID())
                .set("valDateCol", LocalDate.now())
                .set("valDateTimeCol", truncatedLocalDateTimeNow())
                .set("valTimeCol", truncatedLocalTimeNow())
                .set("valTimeStampCol", truncatedInstantNow())
                .set("valBitmask1Col", randomBitSet(rnd, 12))
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

        TupleMarshaller marshaller = new TupleMarshallerImpl(fullSchema);

        Row row = marshaller.marshal(tup1);

        Tuple tup2 = deserializeTuple(serializeTuple(TableRow.tuple(row)));

        assertEquals(tup1, tup2);
        assertEquals(tup2, tup1);
    }

    private Instant truncatedInstantNow() {
        return truncateToDefaultPrecision(Instant.now());
    }

    private LocalTime truncatedLocalTimeNow() {
        return truncateToDefaultPrecision(LocalTime.now());
    }

    private LocalDateTime truncatedLocalDateTimeNow() {
        return truncateToDefaultPrecision(LocalDateTime.now());
    }

    @Test
    public void testTupleEquality() throws Exception {
        Random rnd = new Random();

        Tuple keyTuple = Tuple.create().set("keyUuidCol", UUID.randomUUID());
        Tuple valTuple = Tuple.create()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("valDateCol", LocalDate.now())
                .set("valDateTimeCol", truncatedLocalDateTimeNow())
                .set("valTimeCol", truncatedLocalTimeNow())
                .set("valTimeStampCol", truncatedInstantNow())
                .set("valBitmask1Col", randomBitSet(rnd, 12))
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

        Tuple tuple = Tuple.create(valTuple).set(keyTuple.columnName(0), keyTuple.value(0));

        // Check tuples backed with Row.
        TupleMarshaller marshaller = new TupleMarshallerImpl(fullSchema);

        Row row = marshaller.marshal(keyTuple, valTuple);

        Tuple rowKeyTuple = TableRow.keyTuple(row);
        Tuple rowValTuple = TableRow.valueTuple(row);
        final Tuple rowTuple = TableRow.tuple(marshaller.marshal(tuple));

        assertEquals(keyTuple, rowKeyTuple);
        assertEquals(rowKeyTuple, keyTuple);

        assertEquals(valTuple, rowValTuple);
        assertEquals(rowValTuple, valTuple);

        assertEquals(tuple, rowTuple);
        assertEquals(rowTuple, tuple);

        // Check deserialized.
        Tuple keyTuple2 = deserializeTuple(serializeTuple(rowKeyTuple));
        Tuple valTuple2 = deserializeTuple(serializeTuple(rowValTuple));
        final Tuple tuple2 = deserializeTuple(serializeTuple(rowTuple));

        assertEquals(keyTuple, keyTuple2);
        assertEquals(keyTuple2, keyTuple);

        assertEquals(valTuple, valTuple2);
        assertEquals(valTuple2, valTuple);

        assertEquals(tuple, tuple2);
        assertEquals(tuple2, tuple);

        // Check the tuples backed with Row after update.
        rowKeyTuple.set("foo", "bar");
        rowValTuple.set("foo", "bar");
        rowTuple.set("foo", "bar");

        assertNotEquals(keyTuple, rowKeyTuple);
        assertNotEquals(rowKeyTuple, keyTuple);

        assertNotEquals(valTuple, rowValTuple);
        assertNotEquals(rowValTuple, valTuple);

        assertNotEquals(tuple, rowTuple);
        assertNotEquals(rowTuple, tuple);

        // Update original to make them equal.
        keyTuple.set("foo", "bar");
        valTuple.set("foo", "bar");
        tuple.set("foo", "bar");

        assertEquals(keyTuple, rowKeyTuple);
        assertEquals(rowKeyTuple, keyTuple);

        assertEquals(valTuple, rowValTuple);
        assertEquals(rowValTuple, valTuple);

        assertEquals(tuple, rowTuple);
        assertEquals(rowTuple, tuple);

    }

    @Test
    public void testKeyValueSerialization() throws Exception {
        Random rnd = new Random();

        Tuple key1 = Tuple.create().set("keyUuidCol", UUID.randomUUID());
        Tuple val1 = Tuple.create()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("valDateCol", LocalDate.now())
                .set("valDateTimeCol", truncatedLocalDateTimeNow())
                .set("valTimeCol", truncatedLocalTimeNow())
                .set("valTimeStampCol", truncatedInstantNow())
                .set("valBitmask1Col", randomBitSet(rnd, 12))
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

        TupleMarshaller marshaller = new TupleMarshallerImpl(fullSchema);

        Row row = marshaller.marshal(key1, val1);

        Tuple key2 = deserializeTuple(serializeTuple(TableRow.keyTuple(row)));
        Tuple val2 = deserializeTuple(serializeTuple(TableRow.valueTuple(row)));

        assertEquals(key1, key2);
        assertEquals(val1, val2);
    }

    @Test
    void testTemporalValuesPrecisionConstraint() throws Exception {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(1,
                new Column[]{new Column("key", INT32, false)},
                new Column[]{
                        new Column("time", time(2), true),
                        new Column("datetime", datetime(2), true),
                        new Column("timestamp", timestamp(2), true)
                }
        );

        Tuple tuple = Tuple.create().set("key", 1)
                .set("time", LocalTime.of(1, 2, 3, 456_700_000))
                .set("datetime", LocalDateTime.of(2022, 1, 2, 3, 4, 5, 678_900_000))
                .set("timestamp", Instant.ofEpochSecond(123, 456_700_000));
        Tuple expected = Tuple.create().set("key", 1)
                .set("time", LocalTime.of(1, 2, 3, 450_000_000))
                .set("datetime", LocalDateTime.of(2022, 1, 2, 3, 4, 5, 670_000_000))
                .set("timestamp", Instant.ofEpochSecond(123, 450_000_000));

        TupleMarshaller marshaller = new TupleMarshallerImpl(schemaDescriptor);

        Row row = marshaller.marshal(tuple);

        Tuple tuple1 = deserializeTuple(serializeTuple(TableRow.tuple(row)));

        assertEquals(expected, tuple1);
    }

    @Test
    void testVarlenValuesLengthConstraints() throws Exception {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(1,
                new Column[]{new Column("key", INT32, false)},
                new Column[]{
                        new Column("string", NativeTypes.stringOf(5), true),
                        new Column("bytes", NativeTypes.blobOf(5), true),
                }
        );

        TupleMarshaller marshaller = new TupleMarshallerImpl(schemaDescriptor);

        Tuple tuple1 = Tuple.create().set("key", 1)
                .set("string", "abcef")
                .set("bytes", new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        Tuple tuple2 = Tuple.create().set("key", 1)
                .set("string", "abcefghi")
                .set("bytes", new byte[]{1, 2, 3, 4, 5});

        assertThrowsWithCause(() -> marshaller.marshal(tuple1), InvalidTypeException.class, "Column's type mismatch");
        assertThrowsWithCause(() -> marshaller.marshal(tuple2), InvalidTypeException.class, "Column's type mismatch");

        Tuple expected = Tuple.create().set("key", 1)
                .set("string", "abc")
                .set("bytes", new byte[]{1, 2, 3});

        Row row = marshaller.marshal(expected);

        assertEquals(expected, deserializeTuple(serializeTuple(TableRow.tuple(row))));
    }

    @Test
    void testDecimalPrecisionConstraint() {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(1,
                new Column[]{new Column("key", INT32, false)},
                new Column[]{
                        new Column("decimal", NativeTypes.decimalOf(7, 2), true),
                }
        );

        TupleMarshaller marshaller = new TupleMarshallerImpl(schemaDescriptor);

        Tuple tuple1 = Tuple.create().set("key", 1).set("decimal", new BigDecimal("123456.7"));

        assertThrowsWithCause(() -> marshaller.marshal(tuple1), SchemaMismatchException.class,
                "Failed to set decimal value for column 'decimal' (max precision exceeds allocated precision)");
    }

    @Test
    void testDecimalScaleConstraint() throws Exception {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(1,
                new Column[]{new Column("key", INT32, false)},
                new Column[]{
                        new Column("decimal", NativeTypes.decimalOf(5, 2), true),
                }
        );

        TupleMarshaller marshaller = new TupleMarshallerImpl(schemaDescriptor);

        Tuple tuple = Tuple.create().set("key", 1).set("decimal", new BigDecimal("123.458"));
        Tuple expected = Tuple.create().set("key", 1).set("decimal", new BigDecimal("123.46")); // Rounded.

        Row row = marshaller.marshal(tuple);

        assertEquals(expected, deserializeTuple(serializeTuple(TableRow.tuple(row))));
    }

    private <T extends Temporal> T truncateToDefaultPrecision(T temporal) {
        int precision = temporal instanceof Instant ? TIMESTAMP_PRECISION
                : TIME_PRECISION;

        return (T) temporal.with(NANO_OF_SECOND,
                truncatePrecision(temporal.get(NANO_OF_SECOND), tailFactor(precision)));
    }

    private int truncatePrecision(int nanos, int factor) {
        return nanos / factor * factor;
    }

    private int tailFactor(int precision) {
        if (precision >= NANOS_IN_SECOND) {
            return 1;
        }

        return new BigInteger("10").pow(NANOS_IN_SECOND - precision).intValue();
    }

    /**
     * Deserializes tuple.
     *
     * @param data Tuple bytes.
     * @return Tuple.
     * @throws Exception If failed.
     */
    private Tuple deserializeTuple(byte[] data) throws Exception {
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
    private byte[] serializeTuple(Tuple tup) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(tup);
        }

        return baos.toByteArray();
    }

    private Tuple getTuple() {
        try {
            Tuple original = Tuple.create()
                    .set("id", 3L)
                    .set("name", "Shirt");

            TupleMarshaller marshaller = new TupleMarshallerImpl(schema);

            return TableRow.tuple(marshaller.marshal(original));
        } catch (TupleMarshallerException e) {
            return fail();
        }
    }

    /**
     * Returns random BitSet.
     *
     * @param rnd Random generator.
     * @param bits Amount of bits in bitset.
     */
    private static BitSet randomBitSet(Random rnd, int bits) {
        BitSet set = new BitSet();

        for (int i = 0; i < bits; i++) {
            if (rnd.nextBoolean()) {
                set.set(i);
            }
        }

        return set;
    }
}
