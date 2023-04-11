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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.BitSet;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientDataType;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTuple;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests client tuple builder implementation.
 *
 * <p>Should be in sync with org.apache.ignite.internal.table.TupleBuilderImplTest.
 */
public class ClientTupleTest {
    private static final ClientSchema SCHEMA = new ClientSchema(1, new ClientColumn[]{
            new ClientColumn("ID", ClientDataType.INT64, false, true, true, 0),
            new ClientColumn("NAME", ClientDataType.STRING, false, false, false, 1)
    });

    private static final ClientSchema FULL_SCHEMA = new ClientSchema(100, new ClientColumn[]{
        new ClientColumn("I8", ClientDataType.INT8, false, false, false, 0),
                new ClientColumn("I16", ClientDataType.INT16, false, false, false, 1),
                new ClientColumn("I32", ClientDataType.INT32, false, false, false, 2),
                new ClientColumn("I64", ClientDataType.INT64, false, false, false, 3),
                new ClientColumn("FLOAT", ClientDataType.FLOAT, false, false, false, 4),
                new ClientColumn("DOUBLE", ClientDataType.DOUBLE, false, false, false, 5),
                new ClientColumn("UUID", ClientDataType.UUID, false, false, false, 6),
                new ClientColumn("STR", ClientDataType.STRING, false, false, false, 7),
                new ClientColumn("BITS", ClientDataType.BITMASK, false, false, false, 8),
                new ClientColumn("DATE", ClientDataType.DATE, false, false, false, 9),
                new ClientColumn("TIME", ClientDataType.TIME, false, false, false, 10),
                new ClientColumn("DATETIME", ClientDataType.DATETIME, false, false, false, 11),
                new ClientColumn("TIMESTAMP", ClientDataType.TIMESTAMP, false, false, false, 12)
    });

    private static final UUID GUID = UUID.randomUUID();

    private static final LocalDate DATE = LocalDate.of(1995, Month.MAY, 23);

    private static final LocalTime TIME = LocalTime.of(17, 0, 1, 222_333_444);

    private static final LocalDateTime DATE_TIME = LocalDateTime.of(1995, Month.MAY, 23, 17, 0, 1, 222_333_444);

    private static final Instant TIMESTAMP = Instant.now();

    @Test
    public void testValueReturnsValueByName() {
        assertEquals(3L, (Long) getTuple().value("id"));
        assertEquals("Shirt", getTuple().value("name"));
    }

    @Test
    public void testValueReturnsValueByIndex() {
        assertEquals(3L, (Long) getTuple().value(0));
        assertEquals("Shirt", getTuple().value(1));
    }

    @Test
    public void testValueOrDefaultReturnsValueByName() {
        assertEquals(3L, getTuple().valueOrDefault("id", -1L));
        assertEquals("Shirt", getTuple().valueOrDefault("name", "y"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsNotPresent() {
        assertEquals("foo", getTuple().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsSetToNull() {
        var tuple = getTuple().set("name", null);

        assertNull(tuple.valueOrDefault("name", "foo"));
    }

    @Test
    public void testValueThrowsWhenColumnIsNotPresent() {
        var ex = assertThrows(ColumnNotFoundException.class, () -> getTuple().value("x"));
        assertThat(ex.getMessage(), containsString("Column does not exist [name=\"x\"]"));

        var ex2 = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(100));
        assertThat(ex2.getMessage(), containsString("Index 100 out of bounds for length 2"));
    }

    @Test
    public void testColumnCountReturnsSchemaSize() {
        assertEquals(SCHEMA.columns().length, getTuple().columnCount());
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
    public void testTypedGetters() {
        ClientTuple tuple = getFullSchemaTuple();

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
        ClientTuple tuple = getFullSchemaTuple();

        assertThrowsWithCause(
                () -> tuple.byteValue(1),
                ClassCastException.class,
                "Column with index 1 has type INT16 but INT8 was requested");

        assertThrowsWithCause(
                () -> tuple.byteValue("i16"),
                ClassCastException.class,
                "Column with name 'i16' has type INT16 but INT8 was requested");

        assertThrowsWithCause(
                () -> tuple.shortValue(2),
                ClassCastException.class,
                "Column with index 2 has type INT32 but INT16 was requested");

        assertThrowsWithCause(
                () -> tuple.shortValue("i32"),
                ClassCastException.class,
                "Column with name 'i32' has type INT32 but INT16 was requested");

        assertThrowsWithCause(
                () -> tuple.intValue(3),
                ClassCastException.class,
                "Column with index 3 has type INT64 but INT32 was requested");

        assertThrowsWithCause(
                () -> tuple.intValue("i64"),
                ClassCastException.class,
                "Column with name 'i64' has type INT64 but INT32 was requested");

        assertThrowsWithCause(
                () -> tuple.longValue(4),
                ClassCastException.class,
                "Column with index 4 has type FLOAT but INT64 was requested");

        assertThrowsWithCause(
                () -> tuple.longValue("float"),
                ClassCastException.class,
                "Column with name 'float' has type FLOAT but INT64 was requested");

        assertThrowsWithCause(
                () -> tuple.floatValue(5),
                ClassCastException.class,
                "Column with index 5 has type DOUBLE but FLOAT was requested");

        assertThrowsWithCause(
                () -> tuple.floatValue("double"),
                ClassCastException.class,
                "Column with name 'double' has type DOUBLE but FLOAT was requested");

        assertThrowsWithCause(
                () -> tuple.doubleValue(6),
                ClassCastException.class,
                "Column with index 6 has type UUID but DOUBLE was requested");

        assertThrowsWithCause(
                () -> tuple.doubleValue("uuid"),
                ClassCastException.class,
                "Column with name 'uuid' has type UUID but DOUBLE was requested");

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

    @Test
    public void testBasicTupleEquality() {
        var tuple = getTuple();
        var tuple2 = getTuple();

        assertEquals(tuple, tuple);
        assertEquals(tuple, tuple2);
        assertEquals(tuple.hashCode(), tuple2.hashCode());

        assertEquals(getTuple().set("name", null), getTuple().set("name", null));
        assertEquals(getTuple().set("name", null).hashCode(), getTuple().set("name", null).hashCode());

        assertEquals(getTuple().set("name", "bar"), getTuple().set("name", "bar"));
        assertEquals(getTuple().set("name", "bar").hashCode(), getTuple().set("name", "bar").hashCode());

        assertNotEquals(getTuple().set("name", "foo"), getTuple().set("id", 1));
        assertNotEquals(getTuple().set("name", "foo"), getTuple().set("name", "bar"));

        tuple = getTuple();
        tuple2 = getTuple();

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
        var tuple = getFullSchemaTuple();

        var randomIdx = IntStream.range(0, tuple.columnCount()).boxed().collect(Collectors.toList());

        Collections.shuffle(randomIdx);

        var shuffledTuple = getFullSchemaTuple();

        for (Integer i : randomIdx) {
            shuffledTuple.set(tuple.columnName(i), tuple.value(i));
        }

        assertEquals(tuple, shuffledTuple);
        assertEquals(tuple.hashCode(), shuffledTuple.hashCode());
    }

    @Test
    public void testTupleEqualityCompatibility() {
        var clientTuple = getFullSchemaTuple();
        var tuple = Tuple.create();

        for (int i = 0; i < clientTuple.columnCount(); i++) {
            tuple.set(clientTuple.columnName(i), clientTuple.value(i));
        }

        assertEquals(clientTuple, tuple);
        assertEquals(clientTuple.hashCode(), tuple.hashCode());
    }

    private static Tuple getTuple() {
        var binTupleBuf = new BinaryTupleBuilder(SCHEMA.columns().length, false)
                .appendLong(3L)
                .appendString("Shirt")
                .build();

        var binTuple = new BinaryTupleReader(SCHEMA.columns().length, binTupleBuf);

        return new ClientTuple(SCHEMA, binTuple, 0, SCHEMA.columns().length);
    }

    private static ClientTuple getFullSchemaTuple() {
        var binTupleBuf = new BinaryTupleBuilder(FULL_SCHEMA.columns().length, false)
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
                .build();

        var binTuple = new BinaryTupleReader(FULL_SCHEMA.columns().length, binTupleBuf);

        return new ClientTuple(FULL_SCHEMA, binTuple, 0, FULL_SCHEMA.columns().length);
    }
}
