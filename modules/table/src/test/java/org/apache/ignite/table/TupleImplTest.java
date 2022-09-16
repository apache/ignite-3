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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBitSet;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests server tuple builder implementation.
 *
 * <p>Should be in sync with org.apache.ignite.client.ClientTupleBuilderTest.
 */
public class TupleImplTest {
    @Test
    public void testValueReturnsValueByName() {
        assertEquals(3L, (Long) getTuple().value("id"));
        assertEquals("Shirt", getTuple().value("name"));
    }

    @Test
    public void testValueThrowsOnInvalidColumnName() {
        var ex = assertThrows(IllegalArgumentException.class, () -> getTuple().value("x"));
        assertEquals("Column not found: columnName=x", ex.getMessage());
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
        assertEquals("foo", createTuple().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueReturnsOverwrittenValue() {
        assertEquals("foo", createTuple().set("name", "foo").value("name"));
        assertEquals("foo", createTuple().set("name", "foo").valueOrDefault("name", "bar"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsSetToNull() {
        assertNull(createTuple().set("name", null).valueOrDefault("name", "foo"));

        // Overwritten column.
        assertNull(getTuple().set("name", null).valueOrDefault("name", "foo"));
    }

    @Test
    public void testColumnCountReturnsSchemaSize() {
        assertEquals(0, createTuple().columnCount());

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
    public void testVariousColumnTypes() {
        Random rnd = new Random();

        Tuple tuple = new TupleImpl()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("keyUuidCol", UUID.randomUUID())
                .set("valDateCol", LocalDate.now())
                .set("valDateTimeCol", LocalDateTime.now())
                .set("valTimeCol", LocalTime.now())
                .set("valTimeStampCol", Instant.now())
                .set("valBitmask1Col", randomBitSet(rnd, 12))
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

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
    public void testBasicTupleEquality() {
        assertEquals(new TupleImpl(), new TupleImpl());
        assertEquals(new TupleImpl().hashCode(), new TupleImpl().hashCode());

        assertEquals(new TupleImpl().set("foo", null), new TupleImpl().set("foo", null));
        assertEquals(new TupleImpl().set("foo", null).hashCode(), new TupleImpl().set("foo", null).hashCode());

        assertEquals(new TupleImpl().set("foo", "bar"), new TupleImpl().set("foo", "bar"));
        assertEquals(new TupleImpl().set("foo", "bar").hashCode(), new TupleImpl().set("foo", "bar").hashCode());

        assertNotEquals(new TupleImpl().set("foo", null), new TupleImpl().set("bar", null));
        assertNotEquals(new TupleImpl().set("foo", "foo"), new TupleImpl().set("bar", "bar"));

        TupleImpl tuple = new TupleImpl();
        final TupleImpl tuple2 = new TupleImpl();

        assertEquals(tuple, tuple);

        tuple.set("foo", "bar");

        assertEquals(tuple, tuple);
        assertNotEquals(tuple, tuple2);
        assertNotEquals(tuple2, tuple);

        tuple2.set("foo", "baz");

        assertNotEquals(tuple, tuple2);
        assertNotEquals(tuple2, tuple);

        tuple2.set("foo", "bar");

        assertEquals(tuple, tuple2);
        assertEquals(tuple2, tuple);
    }

    @Test
    public void testTupleEquality() {
        Random rnd = new Random();

        Tuple tuple = new TupleImpl()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("keyUuidCol", UUID.randomUUID())
                .set("valDateCol", LocalDate.now())
                .set("valDateTimeCol", LocalDateTime.now())
                .set("valTimeCol", LocalTime.now())
                .set("valTimeStampCol", Instant.now())
                .set("valBitmask1Col", randomBitSet(rnd, 12))
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

        List<Integer> randomIdx = IntStream.range(0, tuple.columnCount()).boxed().collect(Collectors.toList());

        Collections.shuffle(randomIdx, rnd);

        Tuple shuffledTuple = new TupleImpl();

        for (Integer i : randomIdx) {
            shuffledTuple.set(tuple.columnName(i), tuple.value(i));
        }

        assertEquals(tuple, shuffledTuple);
        assertEquals(tuple.hashCode(), shuffledTuple.hashCode());
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        Random rnd = new Random();

        Tuple tup1 = new TupleImpl()
                .set("valByteCol", (byte) 1)
                .set("valShortCol", (short) 2)
                .set("valIntCol", 3)
                .set("valLongCol", 4L)
                .set("valFloatCol", 0.055f)
                .set("valDoubleCol", 0.066d)
                .set("keyUuidCol", UUID.randomUUID())
                .set("valDateCol", LocalDate.now())
                .set("valDateTimeCol", LocalDateTime.now())
                .set("valTimeCol", LocalTime.now())
                .set("valTimeStampCol", Instant.now())
                .set("valBitmask1Col", randomBitSet(rnd, 12))
                .set("valBytesCol", IgniteTestUtils.randomBytes(rnd, 13))
                .set("valStringCol", IgniteTestUtils.randomString(rnd, 14))
                .set("valNumberCol", BigInteger.valueOf(rnd.nextLong()))
                .set("valDecimalCol", BigDecimal.valueOf(rnd.nextLong(), 5));

        Tuple tup2;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(tup1);
        }

        try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            tup2 = (Tuple) is.readObject();
        }

        assertEquals(tup1, tup2);
    }

    @Test
    void testTupleFactoryMethods() {
        assertEquals(Tuple.create(), Tuple.create(10));
        assertEquals(Tuple.create().set("id", 42L), Tuple.create(10).set("id", 42L));

        assertEquals(Tuple.create().set("id", 42L).set("name", "universe"),
                Tuple.create(Map.of("id", 42L, "name", "universe")));

        assertEquals(Tuple.create().set("id", 42L).set("name", "universe"),
                Tuple.create(Tuple.create().set("id", 42L).set("name", "universe")));
    }

    private static TupleImpl createTuple() {
        return new TupleImpl();
    }

    private static Tuple getTuple() {
        return new TupleImpl()
                .set("id", 3L)
                .set("name", "Shirt");
    }
}
