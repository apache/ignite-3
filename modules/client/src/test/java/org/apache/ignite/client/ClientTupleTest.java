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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Period;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTuple;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.AbstractMutableTupleTest;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests client tuple builder implementation.
 *
 * <p>The class contains implementation-specific tests. Tuple interface contract conformance/violation tests are inherited from the base
 * class.
 */
public class ClientTupleTest extends AbstractMutableTupleTest {
    private static final ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

    private static final ClientSchema SCHEMA = new ClientSchema(1, new ClientColumn[]{
            new ClientColumn("ID", ColumnType.INT64, false, 0, -1, 0, 0),
            new ClientColumn("SIMPLENAME", ColumnType.STRING, false, -1, 0, -1, 1),
            new ClientColumn("QuotedName", ColumnType.STRING, false, -1, 2, -1, 2),
            new ClientColumn("NOVALUE", ColumnType.STRING, true, -1, 1, -1, 3)
    }, marshallers);

    private static final ClientSchema FULL_SCHEMA = new ClientSchema(100, new ClientColumn[]{
            new ClientColumn("I8", ColumnType.INT8, false, -1, 0, -1, 0),
            new ClientColumn("i16", ColumnType.INT16, false, -1, 1, -1, 1),
            new ClientColumn("I32", ColumnType.INT32, false, 0, -1, -1, 2),
            new ClientColumn("i64", ColumnType.INT64, false, 1, -1, -1, 3),
            new ClientColumn("FLOAT", ColumnType.FLOAT, false, -1, 2, -1, 4),
            new ClientColumn("DOUBLE", ColumnType.DOUBLE, false, -1, 3, -1, 5),
            new ClientColumn("UUID", ColumnType.UUID, false, -1, 4, -1, 6),
            new ClientColumn("STR", ColumnType.STRING, false, 2, -1, -1, 7),
            new ClientColumn("DATE", ColumnType.DATE, false, -1, 5, -1, 8),
            new ClientColumn("TIME", ColumnType.TIME, false, -1, 6, -1, 9),
            new ClientColumn("DATETIME", ColumnType.DATETIME, false, -1, 7, -1, 10),
            new ClientColumn("TIMESTAMP", ColumnType.TIMESTAMP, false, -1, 8, -1, 11),
            new ClientColumn("BOOL", ColumnType.BOOLEAN, false, -1, 9, -1, 12),
            new ClientColumn("DECIMAL", ColumnType.DECIMAL, false, -1, 10, -1, 13, 3, 10),
            new ClientColumn("BYTES", ColumnType.BYTE_ARRAY, false, -1, 11, -1, 14),
            new ClientColumn("PERIOD", ColumnType.PERIOD, false, -1, 12, -1, 15),
            new ClientColumn("DURATION", ColumnType.DURATION, false, -1, 13, -1, 16)
    }, marshallers);

    @Test
    @Override
    public void testSerialization() {
        Assumptions.abort("ClientTuple is not serializable.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testColumnCountKeyOnlyReturnsKeySize(boolean partialData) {
        assertEquals(FULL_SCHEMA.columns(TuplePart.KEY).length, createTuplePart(TuplePart.KEY, partialData).columnCount());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testColumnCountValOnlyReturnsValSize(boolean partialData) {
        assertEquals(FULL_SCHEMA.columns(TuplePart.VAL).length, createTuplePart(TuplePart.VAL, partialData).columnCount());
    }

    @Test
    public void testColumnNameReturnsNameByIndexKeyOnly() {
        assertEquals("I32", createTuplePart(TuplePart.KEY, false).columnName(0));
        assertEquals("\"i64\"", createTuplePart(TuplePart.KEY, false).columnName(1));
    }

    @Test
    public void testColumnNameReturnsNameByIndexValOnly() {
        assertEquals("I8", createTuplePart(TuplePart.VAL, false).columnName(0));
        assertEquals("\"i16\"", createTuplePart(TuplePart.VAL, false).columnName(1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testColumnIndexReturnsIndexByNameKeyOnly(boolean partialData) {
        assertEquals(0, createTuplePart(TuplePart.KEY, partialData).columnIndex("I32"));
        assertEquals(1, createTuplePart(TuplePart.KEY, partialData).columnIndex("\"i64\""));
        assertEquals(2, createTuplePart(TuplePart.KEY, partialData).columnIndex("\"STR\""));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testColumnIndexReturnsIndexByNameValOnly(boolean partialData) {
        assertEquals(0, createTuplePart(TuplePart.VAL, partialData).columnIndex("I8"));
        assertEquals(1, createTuplePart(TuplePart.VAL, partialData).columnIndex("\"i16\""));
        assertEquals(4, createTuplePart(TuplePart.VAL, partialData).columnIndex("\"UUID\""));
    }

    @Test
    public void testColumnIndexForMissingColumnsKeyOnly() {
        assertEquals(-1, createTuplePart(TuplePart.KEY, true).columnIndex("foo"));
        assertEquals(-1, createTuplePart(TuplePart.KEY, true).columnIndex("UUID"));
        assertEquals(-1, createTuplePart(TuplePart.KEY, true).columnIndex("i64"));
        assertEquals(-1, createTuplePart(TuplePart.KEY, true).columnIndex("\"i32\""));
    }

    @Test
    public void testColumnIndexForMissingColumnsValOnly() {
        assertEquals(-1, createTuplePart(TuplePart.VAL, true).columnIndex("foo"));
        assertEquals(-1, createTuplePart(TuplePart.VAL, true).columnIndex("I32"));
        assertEquals(-1, createTuplePart(TuplePart.VAL, true).columnIndex("i16"));
        assertEquals(-1, createTuplePart(TuplePart.VAL, true).columnIndex("\"i8\""));
    }

    @Test
    public void testTypedGetters() {
        Tuple tuple = getTupleWithColumnOfAllTypes();

        assertEquals(1, tuple.byteValue(0));
        assertEquals(1, tuple.byteValue("i8"));

        assertEquals(2, tuple.shortValue(1));
        assertEquals(2, tuple.shortValue("\"i16\""));

        assertEquals(3, tuple.intValue(2));
        assertEquals(3, tuple.intValue("i32"));

        assertEquals(4, tuple.longValue(3));
        assertEquals(4, tuple.longValue("\"i64\""));

        assertEquals(5.5, tuple.floatValue(4));
        assertEquals(5.5, tuple.floatValue("float"));

        assertEquals(6.6, tuple.doubleValue(5));
        assertEquals(6.6, tuple.doubleValue("double"));

        assertEquals(UUID_VALUE, tuple.uuidValue(6));
        assertEquals(UUID_VALUE, tuple.uuidValue("uuid"));

        assertEquals(STRING_VALUE, tuple.stringValue(7));
        assertEquals(STRING_VALUE, tuple.stringValue("str"));

        assertEquals(DATE_VALUE, tuple.dateValue("date"));
        assertEquals(TIME_VALUE, tuple.timeValue("time"));
        assertEquals(DATETIME_VALUE, tuple.datetimeValue("datetime"));
        assertEquals(TIMESTAMP_VALUE, tuple.timestampValue("timestamp"));
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testTypedGettersWithIncorrectType() {
        Tuple tuple = getTupleWithColumnOfAllTypes();

        assertThrowsWithCause(
                () -> tuple.floatValue(7),
                ClassCastException.class,
                "Column with index 7 has type STRING but FLOAT was requested");

        assertThrowsWithCause(
                () -> tuple.floatValue("Str"),
                ClassCastException.class,
                "Column with name 'Str' has type STRING but FLOAT was requested");
    }

    @Test
    public void testTupleEqualityCompatibility() {
        var clientTuple = getTupleWithColumnOfAllTypes();
        var tuple = Tuple.create();

        for (int i = 0; i < clientTuple.columnCount(); i++) {
            tuple.set(clientTuple.columnName(i), clientTuple.value(i));
        }

        assertEquals(clientTuple, tuple);
        assertEquals(tuple, clientTuple);
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

    @Test
    public void testKeyOnlyTupleEquality() {
        var keyTupleFullData = createTuplePart(TuplePart.KEY, false);
        var keyTuplePartialData = createTuplePart(TuplePart.KEY, true);
        var keyTupleUser = Tuple.create().set("I32", 3).set("\"i64\"", 4L).set("str", STRING_VALUE);

        assertEquals(keyTupleFullData, keyTuplePartialData);
        assertEquals(keyTupleUser, keyTupleFullData);
        assertEquals(keyTupleUser, keyTuplePartialData);
    }

    @Test
    public void testValOnlyTupleEquality() {
        var valTupleFullData = createTuplePart(TuplePart.VAL, false);
        var valTuplePartialData = createTuplePart(TuplePart.VAL, true);

        var valTupleUser = Tuple.create()
                .set("I8", (byte) 1)
                .set("\"i16\"", (short) 2)
                .set("FLOAT", 5.5f)
                .set("DOUBLE", 6.6)
                .set("UUID", UUID_VALUE)
                .set("DATE", DATE_VALUE)
                .set("TIME", TIME_VALUE)
                .set("DATETIME", DATETIME_VALUE)
                .set("TIMESTAMP", TIMESTAMP_VALUE)
                .set("BOOL", true)
                .set("DECIMAL", BigDecimal.valueOf(1.234))
                .set("BYTES", BYTE_ARRAY_VALUE)
                .set("PERIOD", Period.ofDays(16))
                .set("DURATION", Duration.ofDays(17));

        assertEquals(valTupleFullData, valTuplePartialData);
        assertEquals(valTupleUser, valTupleFullData);
        assertEquals(valTupleUser, valTuplePartialData);
    }

    @Override
    protected Tuple createTuple(Function<Tuple, Tuple> transformer) {
        return transformer.apply(getTuple());
    }

    @Override
    protected Tuple getTuple() {
        var binTupleBuf = new BinaryTupleBuilder(SCHEMA.columns().length)
                .appendLong(3L)
                .appendString("simple")
                .appendString("quoted")
                .appendNull()
                .build();

        var binTuple = new BinaryTupleReader(SCHEMA.columns().length, binTupleBuf);

        return new ClientTuple(SCHEMA, TuplePart.KEY_AND_VAL, binTuple);
    }

    @Override
    protected Tuple getTupleWithColumnOfAllTypes() {
        return createTuplePart(TuplePart.KEY_AND_VAL, false);
    }

    private static ClientTuple createTuplePart(TuplePart part, boolean partialData) {
        var binTupleBuf = new BinaryTupleBuilder(FULL_SCHEMA.columns().length)
                .appendByte((byte) 1)
                .appendShort((short) 2)
                .appendInt(3)
                .appendLong(4)
                .appendFloat(5.5f)
                .appendDouble(6.6)
                .appendUuid(UUID_VALUE)
                .appendString(STRING_VALUE)
                .appendDate(DATE_VALUE)
                .appendTime(TIME_VALUE)
                .appendDateTime(DATETIME_VALUE)
                .appendTimestamp(TIMESTAMP_VALUE)
                .appendByte((byte) 1)
                .appendDecimal(BigDecimal.valueOf(1.234), 3)
                .appendBytes(BYTE_ARRAY_VALUE)
                .appendPeriod(Period.ofDays(16))
                .appendDuration(Duration.ofDays(17))
                .build();

        var binTupleColumnCount = FULL_SCHEMA.columns().length;

        if (part == TuplePart.KEY && partialData) {
            BinaryTupleBuilder keyBuilder = new BinaryTupleBuilder(3)
                    .appendInt(3)
                    .appendLong(4)
                    .appendString(STRING_VALUE);

            binTupleBuf = keyBuilder.build();

            binTupleColumnCount = keyBuilder.numElements();
        }

        if (part == TuplePart.VAL && partialData) {
            BinaryTupleBuilder valueBuilder = new BinaryTupleBuilder(14)
                    .appendByte((byte) 1)
                    .appendShort((short) 2)
                    .appendFloat(5.5f)
                    .appendDouble(6.6)
                    .appendUuid(UUID_VALUE)
                    .appendDate(DATE_VALUE)
                    .appendTime(TIME_VALUE)
                    .appendDateTime(DATETIME_VALUE)
                    .appendTimestamp(TIMESTAMP_VALUE)
                    .appendByte((byte) 1)
                    .appendDecimal(BigDecimal.valueOf(1.234), 3)
                    .appendBytes(BYTE_ARRAY_VALUE)
                    .appendPeriod(Period.ofDays(16))
                    .appendDuration(Duration.ofDays(17));

            binTupleBuf = valueBuilder.build();

            binTupleColumnCount = valueBuilder.numElements();
        }

        var binTuple = new BinaryTupleReader(binTupleColumnCount, binTupleBuf);

        return new ClientTuple(FULL_SCHEMA, part, binTuple);
    }
}
