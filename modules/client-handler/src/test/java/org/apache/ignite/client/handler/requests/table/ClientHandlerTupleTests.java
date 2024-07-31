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

package org.apache.ignite.client.handler.requests.table;

import static org.apache.ignite.internal.type.NativeTypes.BYTES;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClientHandlerTuple}.
 */
@SuppressWarnings("DataFlowIssue")
public class ClientHandlerTupleTests {
    private static final UUID GUID = UUID.randomUUID();

    private static final LocalDate DATE = LocalDate.of(1995, Month.MAY, 23);

    private static final LocalTime TIME = LocalTime.of(17, 0, 1, 222_333_444);

    private static final LocalDateTime DATE_TIME = LocalDateTime.of(1995, Month.MAY, 23, 17, 0, 1, 222_333_444);

    private static final Instant TIMESTAMP = Instant.now();

    /** Schema descriptor. */
    private final SchemaDescriptor fullSchema = new SchemaDescriptor(42,
            new Column[]{new Column("keyUuidCol".toUpperCase(), NativeTypes.UUID, false)},
            new Column[]{
                    new Column("valByteCol".toUpperCase(), INT8, true),
                    new Column("valShortCol".toUpperCase(), INT16, true),
                    new Column("valIntCol".toUpperCase(), INT32, true),
                    new Column("valLongCol".toUpperCase(), INT64, true),
                    new Column("valFloatCol".toUpperCase(), FLOAT, true),
                    new Column("valDoubleCol".toUpperCase(), DOUBLE, true),
                    new Column("valDateCol".toUpperCase(), NativeTypes.DATE, true),
                    new Column("valTimeCol".toUpperCase(), time(NativeTypes.MAX_TIME_PRECISION), true),
                    new Column("valDateTimeCol".toUpperCase(), datetime(NativeTypes.MAX_TIME_PRECISION), true),
                    new Column("valTimeStampCol".toUpperCase(), timestamp(NativeTypes.MAX_TIME_PRECISION), true),
                    new Column("valBytesCol".toUpperCase(), BYTES, false),
                    new Column("valStringCol".toUpperCase(), STRING, false),
                    new Column("valDecimalCol".toUpperCase(), NativeTypes.decimalOf(25, 5), false),
            }
    );

    @Test
    public void testTupleEquality() {
        Tuple tuple = createTuple();

        BinaryTupleReader binaryTuple = new TupleMarshallerImpl(fullSchema).marshal(tuple).binaryTuple();
        Tuple clientHandlerTuple = new ClientHandlerTuple(fullSchema, null, binaryTuple, false);

        assertEquals(tuple, clientHandlerTuple);
    }

    @Test
    public void testTupleEqualityKeyOnly() {
        Tuple tuple = createKeyTuple();

        BinaryTupleReader binaryTuple = new TupleMarshallerImpl(fullSchema).marshalKey(tuple).binaryTuple();
        Tuple clientHandlerTuple = new ClientHandlerTuple(fullSchema, null, binaryTuple, true);

        assertEquals(tuple, clientHandlerTuple);
    }

    @Test
    public void testValueReturnsValueByName() {
        Tuple tuple = createTuple();

        assertEquals(1, (byte) tuple.value("valByteCol"));
        assertEquals(DATE, tuple.value("valDateCol"));

        assertThrows(IllegalArgumentException.class, () -> tuple.value("bad-name"));
    }

    @Test
    public void testValueReturnsValueByNameKeyOnly() {
        Tuple tuple = createKeyTuple();

        assertEquals(GUID, tuple.value("keyUuidCol"));

        assertThrows(IllegalArgumentException.class, () -> tuple.value("valDateCol"));
    }

    @Test
    public void testValueReturnsValueByIndex() {
        Tuple tuple = createTuple();

        assertEquals(1, (byte) tuple.value(0));
        assertEquals(4L, tuple.longValue(3));

        assertThrows(IndexOutOfBoundsException.class, () -> tuple.value(123));
    }

    @Test
    public void testValueReturnsValueByIndexKeyOnly() {
        Tuple tuple = createKeyTuple();

        assertEquals(GUID, tuple.value(0));

        assertThrows(IndexOutOfBoundsException.class, () -> tuple.value(1));
    }

    @Test
    public void testColumnCount() {
        assertEquals(14, createTuple().columnCount());
        assertEquals(1, createKeyTuple().columnCount());
    }

    @Test
    public void testColumnIndex() {
        Tuple tuple = createTuple();

        assertEquals(0, tuple.columnIndex("valByteCol"));
        assertEquals(3, tuple.columnIndex("valLongCol"));
        assertEquals(-1, tuple.columnIndex("bad-name"));
    }

    @Test
    public void testColumnIndexKeyOnly() {
        Tuple tuple = createKeyTuple();

        assertEquals(0, tuple.columnIndex("keyUuidCol"));
        assertEquals(-1, tuple.columnIndex("valLongCol"));
    }

    private static Tuple createKeyTuple() {
        return Tuple.create()
                .set("keyUuidCol", GUID);
    }

    private static Tuple createTuple() {
        Random rnd = new Random();

        return Tuple.create()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("keyUuidCol", GUID)
                .set("valDateCol", DATE)
                .set("valDateTimeCol", DATE_TIME)
                .set("valTimeCol", TIME)
                .set("valTimeStampCol", TIMESTAMP)
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));
    }
}
