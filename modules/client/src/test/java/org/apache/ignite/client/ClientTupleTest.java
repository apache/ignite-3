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

package org.apache.ignite.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.Period;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
            new ClientColumn("ID", ClientDataType.INT64, false, true, 0),
            new ClientColumn("NAME", ClientDataType.STRING, false, false, 1)
    });

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
        assertEquals("foo", getBuilder().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsPresentButNotSet() {
        assertEquals("foo", getBuilder().valueOrDefault("name", "foo"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsSetToNull() {
        var tuple = getBuilder().set("name", null);

        assertNull(tuple.valueOrDefault("name", "foo"));
    }

    @Test
    public void testEmptySchemaThrows() {
        assertThrows(AssertionError.class, () -> new ClientTuple(new ClientSchema(1, new ClientColumn[0])));
    }

    @Test
    public void testSetThrowsWhenColumnIsNotPresent() {
        var ex = assertThrows(ColumnNotFoundException.class, () -> getBuilder().set("x", "y"));
        assertThat(ex.getMessage(), containsString("Column 'X' does not exist"));
    }

    @Test
    public void testValueThrowsWhenColumnIsNotPresent() {
        var ex = assertThrows(ColumnNotFoundException.class, () -> getBuilder().value("x"));
        assertThat(ex.getMessage(), containsString("Column 'X' does not exist"));

        var ex2 = assertThrows(IndexOutOfBoundsException.class, () -> getBuilder().value(100));
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
        List<ClientColumn> columns = new ArrayList<>();

        int idx = 0;

        for (ClientDataType val : ClientDataType.values()) {
            columns.add(new ClientColumn(val.name(), val.type(), false, false, idx++));
        }

        var schema = new ClientSchema(100, columns.toArray(new ClientColumn[0]));

        var uuid = UUID.randomUUID();

        var date = LocalDate.of(1995, Month.MAY, 23);
        var time = LocalTime.of(17, 0, 1, 222_333_444);
        var datetime = LocalDateTime.of(1995, Month.MAY, 23, 17, 0, 1, 222_333_444);
        var timestamp = Instant.now();
        var duration = Duration.ofMinutes(1);
        var period = Period.ofDays(2);
        var bytes = new byte[]{1, 2};
        var bi = BigInteger.valueOf(10);

        Map<String, Object> data = new HashMap<>();
        data.put("int8", (byte) 1);
        data.put("int16", (short) 2);
        data.put("int32", 3);
        data.put("int64", (long) 4);
        data.put("float", (float) 5.5);
        data.put("double", 6.6);
        data.put("decimal", 7.7);
        data.put("uuid", uuid);
        data.put("string", "8");
        data.put("bytes", bytes);
        data.put("bitmask", new BitSet(3));
        data.put("date", date);
        data.put("time", time);
        data.put("datetime", datetime);
        data.put("timestamp", timestamp);
        data.put("number", 8);
        data.put("boolean", true);
        data.put("biginteger", bi);
        data.put("duration", duration);
        data.put("period", period);

        var tuple = new ClientTuple(schema);

        data.forEach(tuple::set);

        assertEquals(data.size(), columns.size(), "Some data types are not covered.");

        assertEquals(1, tuple.byteValue(0));
        assertEquals(1, tuple.byteValue("int8"));

        assertEquals(2, tuple.shortValue(1));
        assertEquals(2, tuple.shortValue("int16"));

        assertEquals(3, tuple.intValue(2));
        assertEquals(3, tuple.intValue("int32"));

        assertEquals(4, tuple.longValue(3));
        assertEquals(4, tuple.longValue("int64"));

        assertEquals(5.5, tuple.floatValue(4));
        assertEquals(5.5, tuple.floatValue("float"));

        assertEquals(6.6, tuple.doubleValue(5));
        assertEquals(6.6, tuple.doubleValue("double"));

        assertEquals(7.7, tuple.value(6));
        assertEquals(7.7, tuple.value("decimal"));

        assertEquals(uuid, tuple.uuidValue(7));
        assertEquals(uuid, tuple.uuidValue("uuid"));

        assertEquals("8", tuple.stringValue(8));
        assertEquals("8", tuple.stringValue("string"));

        assertEquals(bytes, tuple.value(9));
        assertEquals(bytes, tuple.value("bytes"));

        assertEquals(0, tuple.bitmaskValue(10).length());
        assertEquals(0, tuple.bitmaskValue("bitmask").length());

        assertEquals(date, tuple.dateValue("date"));
        assertEquals(time, tuple.timeValue("time"));
        assertEquals(datetime, tuple.datetimeValue("datetime"));
        assertEquals(timestamp, tuple.timestampValue("timestamp"));
        assertEquals(8.8, tuple.value("number"));
        assertEquals(true, tuple.value("boolean"));
        assertEquals(bi, tuple.value("biginteger"));
        assertEquals(duration, tuple.durationValue(18));
        assertEquals(duration, tuple.durationValue("duration"));
        assertEquals(period, tuple.periodValue(19));
        assertEquals(period, tuple.periodValue("period"));
    }

    @Test
    public void testBasicTupleEquality() {
        var tuple = new ClientTuple(SCHEMA);
        var tuple2 = new ClientTuple(SCHEMA);

        assertEquals(tuple, tuple);
        assertEquals(tuple, tuple2);
        assertEquals(tuple.hashCode(), tuple2.hashCode());

        assertNotEquals(new ClientTuple(SCHEMA), new ClientTuple(new ClientSchema(1, new ClientColumn[]{
                new ClientColumn("id", ClientDataType.INT64, false, true, 0)})));

        assertEquals(new ClientTuple(SCHEMA).set("name", null), new ClientTuple(SCHEMA).set("name", null));
        assertEquals(new ClientTuple(SCHEMA).set("name", null).hashCode(), new ClientTuple(SCHEMA).set("name", null).hashCode());

        assertEquals(new ClientTuple(SCHEMA).set("name", "bar"), new ClientTuple(SCHEMA).set("name", "bar"));
        assertEquals(new ClientTuple(SCHEMA).set("name", "bar").hashCode(), new ClientTuple(SCHEMA).set("name", "bar").hashCode());

        assertNotEquals(new ClientTuple(SCHEMA).set("name", "foo"), new ClientTuple(SCHEMA).set("id", 1));
        assertNotEquals(new ClientTuple(SCHEMA).set("name", "foo"), new ClientTuple(SCHEMA).set("name", "bar"));

        tuple = new ClientTuple(SCHEMA);
        tuple2 = new ClientTuple(SCHEMA);

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
        List<ClientColumn> columns = new ArrayList<>();

        int idx = 0;

        for (ClientDataType val : ClientDataType.values()) {
            columns.add(new ClientColumn(val.name(), val.type(), false, false, idx++));
        }

        var schema = new ClientSchema(100, columns.toArray(new ClientColumn[0]));

        var tuple = testTuple();
        var randomIdx = IntStream.range(0, tuple.columnCount()).boxed().collect(Collectors.toList());

        Collections.shuffle(randomIdx);

        var shuffledTuple = new ClientTuple(schema);

        for (Integer i : randomIdx) {
            shuffledTuple.set(tuple.columnName(i), tuple.value(i));
        }

        assertEquals(tuple, shuffledTuple);
        assertEquals(tuple.hashCode(), shuffledTuple.hashCode());
    }

    @Test
    public void testTupleEqualityCompatibility() {
        var clientTuple = testTuple();

        var tuple = Tuple.create();

        for (int i = 0; i < clientTuple.columnCount(); i++) {
            tuple.set(clientTuple.columnName(i), clientTuple.value(i));
        }

        assertEquals(clientTuple, tuple);
        assertEquals(clientTuple.hashCode(), tuple.hashCode());
    }

    private static ClientTuple getBuilder() {
        return new ClientTuple(SCHEMA);
    }

    private static Tuple getTuple() {
        return new ClientTuple(SCHEMA)
                .set("id", 3L)
                .set("name", "Shirt");
    }

    private Tuple testTuple() {
        List<ClientColumn> columns = new ArrayList<>();

        int idx = 0;

        for (ClientDataType val : ClientDataType.values()) {
            columns.add(new ClientColumn(val.name(), val.type(), false, false, idx++));
        }

        var schema = new ClientSchema(100, columns.toArray(new ClientColumn[0]));

        var uuid = UUID.randomUUID();

        var date = LocalDate.of(1995, Month.MAY, 23);
        var time = LocalTime.of(17, 0, 1, 222_333_444);
        var datetime = LocalDateTime.of(1995, Month.MAY, 23, 17, 0, 1, 222_333_444);
        var timestamp = Instant.now();

        return new ClientTuple(schema)
                .set("int8", (byte) 1)
                .set("int16", (short) 2)
                .set("int32", 3)
                .set("int64", (long) 4)
                .set("float", (float) 5.5)
                .set("double", 6.6)
                .set("decimal", 7.7)
                .set("uuid", uuid)
                .set("string", "8")
                .set("bitmask", new BitSet(3))
                .set("date", date)
                .set("time", time)
                .set("datetime", datetime)
                .set("timestamp", timestamp);
    }
}
