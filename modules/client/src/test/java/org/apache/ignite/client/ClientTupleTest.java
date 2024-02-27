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

package org.apache.ignite.client;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.Period;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTuple;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests client tuple builder implementation.
 *
 * <p>Should be in sync with org.apache.ignite.internal.table.TupleBuilderImplTest.
 */
public class ClientTupleTest {
    private static final ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

    private static final ClientSchema SCHEMA = new ClientSchema(1, new ClientColumn[]{
            new ClientColumn("ID", ColumnType.INT64, false, 0, 0, 0),
            new ClientColumn("NAME", ColumnType.STRING, false, -1, -1, 1)
    }, marshallers);

    private static final ClientSchema FULL_SCHEMA = new ClientSchema(100, new ClientColumn[]{
            new ClientColumn("I8", ColumnType.INT8, false, -1, -1, 0),
            new ClientColumn("I16", ColumnType.INT16, false, -1, -1, 1),
            new ClientColumn("I32", ColumnType.INT32, false, -1, -1, 2),
            new ClientColumn("I64", ColumnType.INT64, false, -1, -1, 3),
            new ClientColumn("FLOAT", ColumnType.FLOAT, false, -1, -1, 4),
            new ClientColumn("DOUBLE", ColumnType.DOUBLE, false, -1, -1, 5),
            new ClientColumn("UUID", ColumnType.UUID, false, -1, -1, 6),
            new ClientColumn("STR", ColumnType.STRING, false, -1, -1, 7),
            new ClientColumn("BITS", ColumnType.BITMASK, false, -1, -1, 8),
            new ClientColumn("DATE", ColumnType.DATE, false, -1, -1, 9),
            new ClientColumn("TIME", ColumnType.TIME, false, -1, -1, 10),
            new ClientColumn("DATETIME", ColumnType.DATETIME, false, -1, -1, 11),
            new ClientColumn("TIMESTAMP", ColumnType.TIMESTAMP, false, -1, -1, 12),
            new ClientColumn("BOOL", ColumnType.BOOLEAN, false, -1, -1, 13),
            new ClientColumn("DECIMAL", ColumnType.DECIMAL, false, -1, -1, 14),
            new ClientColumn("BYTES", ColumnType.BYTE_ARRAY, false, -1, -1, 15),
            new ClientColumn("PERIOD", ColumnType.PERIOD, false, -1, -1, 16),
            new ClientColumn("DURATION", ColumnType.DURATION, false, -1, -1, 17),
            new ClientColumn("NUMBER", ColumnType.NUMBER, false, -1, -1, 18)
    }, marshallers);

    private static final UUID GUID = UUID.randomUUID();

    private static final LocalDate DATE = LocalDate.of(1995, Month.MAY, 23);

    private static final LocalTime TIME = LocalTime.of(17, 0, 1, 222_333_444);

    private static final LocalDateTime DATE_TIME = LocalDateTime.of(1995, Month.MAY, 23, 17, 0, 1, 222_333_444);

    private static final Instant TIMESTAMP = Instant.now();

    @Test
    public void testValueReturnsValueByName() {
        assertEquals(3L, (Long) createTuple().value("id"));
        assertEquals("Shirt", createTuple().value("name"));
    }

    @Test
    public void testValueReturnsValueByIndex() {
        assertEquals(3L, (Long) createTuple().value(0));
        assertEquals("Shirt", createTuple().value(1));
    }

    @Test
    public void testValueOrDefaultReturnsValueByName() {
        assertEquals(3L, createTuple().valueOrDefault("id", -1L));
        assertEquals("Shirt", createTuple().valueOrDefault("name", "y"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsNotPresent() {
        assertEquals("foo", createTuple().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsSetToNull() {
        var tuple = createTuple().set("name", null);

        assertNull(tuple.valueOrDefault("name", "foo"));
    }

    @Test
    public void testValueThrowsWhenColumnIsNotPresent() {
        var ex = assertThrows(IllegalArgumentException.class, () -> createTuple().value("x"));
        assertThat(ex.getMessage(), containsString("Column doesn't exist [name=x]"));

        var ex2 = assertThrows(IndexOutOfBoundsException.class, () -> createTuple().value(100));
        assertThat(ex2.getMessage(), containsString("Index 100 out of bounds for length 2"));
    }

    @Test
    public void testColumnCountReturnsSchemaSize() {
        assertEquals(SCHEMA.columns().length, createTuple().columnCount());
    }

    @Test
    public void testColumnNameReturnsNameByIndex() {
        assertEquals("ID", createTuple().columnName(0));
        assertEquals("NAME", createTuple().columnName(1));
    }

    @Test
    public void testColumnNameThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> createTuple().columnName(-1));
        assertEquals("Index -1 out of bounds for length 2", ex.getMessage());
    }

    @Test
    public void testColumnIndexReturnsIndexByName() {
        assertEquals(0, createTuple().columnIndex("id"));
        assertEquals(1, createTuple().columnIndex("name"));
    }

    @Test
    public void testColumnIndexForMissingColumns() {
        assertEquals(-1, createTuple().columnIndex("foo"));
    }

    @Test
    public void testTypedGetters() {
        ClientTuple tuple = createFullSchemaTuple();

        assertEquals(1, tuple.byteValue(0));
        assertEquals(1, tuple.byteValue("i8"));

        assertEquals(2, tuple.shortValue(1));
        assertEquals(2, tuple.shortValue("i16"));

        assertEquals(3, tuple.intValue(2));
        assertEquals(3, tuple.intValue("i32"));

        assertEquals(4, tuple.longValue(3));
        assertEquals(4, tuple.longValue("i64"));

        assertEquals(5.5, tuple.floatValue(4));
        assertEquals(5.5, tuple.floatValue("float"));

        assertEquals(6.6, tuple.doubleValue(5));
        assertEquals(6.6, tuple.doubleValue("double"));

        assertEquals(GUID, tuple.uuidValue(6));
        assertEquals(GUID, tuple.uuidValue("uuid"));

        assertEquals("8", tuple.stringValue(7));
        assertEquals("8", tuple.stringValue("str"));

        assertEquals(0, tuple.bitmaskValue(8).length());
        assertEquals(0, tuple.bitmaskValue("bits").length());

        assertEquals(DATE, tuple.dateValue("date"));
        assertEquals(TIME, tuple.timeValue("time"));
        assertEquals(DATE_TIME, tuple.datetimeValue("datetime"));
        assertEquals(TIMESTAMP, tuple.timestampValue("timestamp"));
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testTypedGettersWithIncorrectType() {
        ClientTuple tuple = createFullSchemaTuple();

        assertThrowsWithCause(
                () -> tuple.byteValue(8),
                ClassCastException.class,
                "Column with index 8 has type BITMASK but INT8 was requested");

        assertThrowsWithCause(
                () -> tuple.floatValue("Str"),
                ClassCastException.class,
                "Column with name 'Str' has type STRING but FLOAT was requested");
    }

    @Test
    public void testBasicTupleEquality() {
        var tuple = createTuple();
        var tuple2 = createTuple();

        assertEquals(tuple, tuple);
        assertEquals(tuple, tuple2);
        assertEquals(tuple.hashCode(), tuple2.hashCode());

        assertEquals(createTuple().set("name", null), createTuple().set("name", null));
        assertEquals(createTuple().set("name", null).hashCode(), createTuple().set("name", null).hashCode());

        assertEquals(createTuple().set("name", "bar"), createTuple().set("name", "bar"));
        assertEquals(createTuple().set("name", "bar").hashCode(), createTuple().set("name", "bar").hashCode());

        assertNotEquals(createTuple().set("name", "foo"), createTuple().set("id", 1));
        assertNotEquals(createTuple().set("name", "foo"), createTuple().set("name", "bar"));

        tuple = createTuple();
        tuple2 = createTuple();

        tuple.set("name", "bar");

        assertEquals(tuple, tuple);
        assertNotEquals(tuple, tuple2);
        assertNotEquals(tuple2, tuple);

        tuple2.set("name", "baz");

        assertNotEquals(tuple, tuple2);
        assertNotEquals(tuple2, tuple);

        tuple2.set("name", "bar");

        assertEquals(tuple, tuple2);
        assertEquals(tuple2, tuple);
    }

    @Test
    public void testTupleEquality() {
        var tuple = createFullSchemaTuple();

        var randomIdx = IntStream.range(0, tuple.columnCount()).boxed().collect(Collectors.toList());

        Collections.shuffle(randomIdx);

        var shuffledTuple = createFullSchemaTuple();

        for (Integer i : randomIdx) {
            shuffledTuple.set(tuple.columnName(i), tuple.value(i));
        }

        assertEquals(tuple, shuffledTuple);
        assertEquals(tuple.hashCode(), shuffledTuple.hashCode());
    }

    @Test
    public void testTupleEqualityCompatibility() {
        var clientTuple = createFullSchemaTuple();
        var tuple = Tuple.create();

        for (int i = 0; i < clientTuple.columnCount(); i++) {
            tuple.set(clientTuple.columnName(i), clientTuple.value(i));
        }

        assertEquals(clientTuple, tuple);
        assertEquals(clientTuple.hashCode(), tuple.hashCode());
    }

    @Test
    public void testFullSchemaHasAllTypes() {
        Set<ColumnType> schemaTypes = Arrays.stream(FULL_SCHEMA.columns())
                .map(ClientColumn::type)
                .collect(Collectors.toSet());

        for (ColumnType columnType : ColumnType.values()) {
            if (columnType == ColumnType.NULL) {
                continue;
            }

            assertTrue(schemaTypes.contains(columnType), "Schema does not contain " + columnType);
        }
    }

    private static Tuple createTuple() {
        var binTupleBuf = new BinaryTupleBuilder(SCHEMA.columns().length)
                .appendLong(3L)
                .appendString("Shirt")
                .build();

        var binTuple = new BinaryTupleReader(SCHEMA.columns().length, binTupleBuf);

        return new ClientTuple(SCHEMA, SCHEMA.columns(), binTuple);
    }

    private static ClientTuple createFullSchemaTuple() {
        var binTupleBuf = new BinaryTupleBuilder(FULL_SCHEMA.columns().length)
                .appendByte((byte) 1)
                .appendShort((short) 2)
                .appendInt(3)
                .appendLong(4)
                .appendFloat(5.5f)
                .appendDouble(6.6)
                .appendUuid(GUID)
                .appendString("8")
                .appendBitmask(new BitSet(3))
                .appendDate(DATE)
                .appendTime(TIME)
                .appendDateTime(DATE_TIME)
                .appendTimestamp(TIMESTAMP)
                .appendByte((byte) 1)
                .appendDecimal(BigDecimal.valueOf(1.234), 3)
                .appendBytes(new byte[] {1, 2, 3})
                .appendPeriod(Period.ofDays(16))
                .appendDuration(Duration.ofDays(17))
                .appendNumber(BigInteger.valueOf(18))
                .build();

        var binTuple = new BinaryTupleReader(FULL_SCHEMA.columns().length, binTupleBuf);

        return new ClientTuple(FULL_SCHEMA, FULL_SCHEMA.columns(), binTuple);
    }
}
