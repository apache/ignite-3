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

import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
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
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.table.KeyValueTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
                    new Column("valBooleanCol".toUpperCase(), BOOLEAN, true),
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

        BinaryTupleReader binaryTuple = KeyValueTestUtils.createMarshaller(fullSchema).marshal(tuple).binaryTuple();
        Tuple clientHandlerTuple = new ClientHandlerTuple(fullSchema, null, binaryTuple, false);

        assertEquals(tuple, clientHandlerTuple);
    }

    @Test
    public void testTupleEqualityKeyOnly() {
        Tuple tuple = createKeyTuple();

        BinaryTupleReader binaryTuple = KeyValueTestUtils.createMarshaller(fullSchema).marshalKey(tuple).binaryTuple();
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

        assertEquals(true, tuple.value(0));
        assertEquals(1, (byte) tuple.value(1));
        assertEquals(4L, tuple.longValue(4));

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
        assertEquals(15, createTuple().columnCount());
        assertEquals(1, createKeyTuple().columnCount());
    }

    @Test
    public void testColumnIndex() {
        Tuple tuple = createTuple();

        assertEquals(0, tuple.columnIndex("valBooleanCol"));
        assertEquals(1, tuple.columnIndex("valByteCol"));
        assertEquals(4, tuple.columnIndex("valLongCol"));
        assertEquals(-1, tuple.columnIndex("bad_name"));
    }

    @Test
    public void testColumnIndexKeyOnly() {
        Tuple tuple = createKeyTuple();

        assertEquals(0, tuple.columnIndex("keyUuidCol"));
        assertEquals(-1, tuple.columnIndex("valLongCol"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessors")
    @SuppressWarnings("ThrowableNotThrown")
    void nullPointerWhenReadingNullAsPrimitive(
            NativeType type,
            BiConsumer<Tuple, Object> fieldAccessor
    ) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("ID", INT32, false)},
                new Column[]{new Column("VAL", type, true)}
        );

        BinaryRow binaryRow = SchemaTestUtils.binaryRow(schema, 1, null);
        Tuple row = new ClientHandlerTuple(schema, null, new BinaryTupleReader(2, binaryRow.tupleSlice()), false);

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> fieldAccessor.accept(row, 1),
                IgniteStringFormatter.format(IgniteUtils.NULL_TO_PRIMITIVE_ERROR_MESSAGE, 1)
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> fieldAccessor.accept(row, "VAL"),
                IgniteStringFormatter.format(IgniteUtils.NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE, "VAL")
        );

        IgniteTestUtils.assertThrows(
                UnsupportedOperationException.class,
                () -> row.set("NEW", null),
                null
        );
    }

    private static Tuple createKeyTuple() {
        return Tuple.create()
                .set("keyUuidCol", GUID);
    }

    private static Tuple createTuple() {
        Random rnd = new Random();

        return Tuple.create()
                .set("valBooleanCol", true)
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

    private static Stream<Arguments> primitiveAccessors() {
        IdentityHashMap<NativeType, BiConsumer<Tuple, Object>> map = new IdentityHashMap<>();

        map.put(BOOLEAN, (tuple, index) -> invoke(index, tuple::booleanValue, tuple::booleanValue));
        map.put(INT8, (tuple, index) -> invoke(index, tuple::byteValue, tuple::byteValue));
        map.put(INT16, (tuple, index) -> invoke(index, tuple::shortValue, tuple::shortValue));
        map.put(INT32, (tuple, index) -> invoke(index, tuple::intValue, tuple::intValue));
        map.put(INT64, (tuple, index) -> invoke(index, tuple::longValue, tuple::longValue));
        map.put(FLOAT, (tuple, index) -> invoke(index, tuple::floatValue, tuple::floatValue));
        map.put(DOUBLE, (tuple, index) -> invoke(index, tuple::doubleValue, tuple::doubleValue));

        return map.entrySet().stream()
                .map(e -> Arguments.of(e.getKey(), e.getValue()));
    }

    private static void invoke(Object index, IntConsumer intConsumer, Consumer<String> strConsumer) {
        if (index instanceof Integer) {
            intConsumer.accept((int) index);
        } else {
            assert index instanceof String : index.getClass();

            strConsumer.accept((String) index);
        }
    }
}
