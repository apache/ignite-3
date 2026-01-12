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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Base test class for mutable Tuple implementation.
 */
public abstract class AbstractMutableTupleTest extends AbstractImmutableTupleTest {
    @Test
    @Override
    public void testColumnCount() {
        assertEquals(4, getTuple().columnCount());
        assertEquals(4, getTuple().set("id", -1).columnCount());
        assertEquals(4, getTuple().set("simplename", null).columnCount());
        assertEquals(4, getTuple().set("\"QuotedName\"", "foo").columnCount());
        assertEquals(4, getTuple().set("novalue", "foo").columnCount());

        Tuple tuple = getTuple();
        tuple.valueOrDefault("SimpleName", "foo");
        assertEquals(4, tuple.columnCount());

        tuple.valueOrDefault("foo", "bar");
        assertEquals(4, tuple.columnCount());

        tuple.set("foo", "bar");
        assertEquals(5, tuple.columnCount());

        tuple.set("nullColumn", null);
        assertEquals(6, tuple.columnCount());
    }

    @Test
    public void testValueReturnsOverwrittenValue() {
        assertEquals("foo", getTuple().set("SimpleName", "foo").value("SimpleName"));
        assertNull(getTuple().set("SimpleName", null).value("SimpleName"));

        assertEquals("foo", getTuple().set("\"QuotedName\"", "foo").value("\"QuotedName\""));
        assertNull(getTuple().set("\"QuotedName\"", null).value("\"QuotedName\""));
    }

    @Test
    public void testValueOrDefaultReturnsOverwrittenValue() {
        assertEquals("foo", getTuple().set("SimpleName", "foo").valueOrDefault("SimpleName", "bar"));
        assertNull(getTuple().set("SimpleName", null).valueOrDefault("SimpleName", "foo"));

        assertEquals("foo", getTuple().set("\"QuotedName\"", "foo").valueOrDefault("\"QuotedName\"", "bar"));
        assertNull(getTuple().set("\"QuotedName\"", null).valueOrDefault("\"QuotedName\"", "foo"));
    }

    @Test
    public void testMutableTupleEquality() {
        assertEquals(createTuple(), createTuple());
        assertEquals(createTuple().hashCode(), createTuple().hashCode());

        assertEquals(createTuple().set("foo", null), createTuple().set("foo", null));
        assertEquals(createTuple().set("foo", null).hashCode(), createTuple().set("foo", null).hashCode());

        assertEquals(createTuple().set("foo", "bar"), createTuple().set("foo", "bar"));
        assertEquals(createTuple().set("foo", "bar").hashCode(), createTuple().set("foo", "bar").hashCode());

        assertNotEquals(createTuple().set("foo", null), createTuple().set("bar", null));
        assertNotEquals(createTuple().set("foo", "foo"), createTuple().set("bar", "bar"));

        assertEquals(createTuple().set("foo", "bar"), createTuple().set("FOO", "bar"));
        assertEquals(createTuple().set("foo", "bar"), createTuple().set("\"FOO\"", "bar"));
        assertEquals(createTuple().set("\"foo\"", "bar"), createTuple().set("\"foo\"", "bar"));

        assertNotEquals(createTuple().set("foo", "foo"), createTuple().set("\"foo\"", "bar"));
        assertNotEquals(createTuple().set("FOO", "foo"), createTuple().set("\"foo\"", "bar"));
        assertNotEquals(createTuple().set("\"FOO\"", "foo"), createTuple().set("\"foo\"", "bar"));

        Tuple tuple = createTuple();
        Tuple tuple2 = createTuple();

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
    public void testTupleEqualityDifferentColumnOrder() {
        Random rnd = new Random();

        Tuple tuple = getTuple();

        List<Integer> randomIdx = IntStream.range(0, tuple.columnCount()).boxed().collect(Collectors.toList());

        Collections.shuffle(randomIdx, rnd);

        Tuple shuffledTuple = createTuple(t -> {
            for (Integer i : randomIdx) {
                t.set(tuple.columnName(i), tuple.value(i));
            }
            return t;
        });

        assertEquals(tuple, shuffledTuple);
        assertEquals(shuffledTuple, tuple);
        assertEquals(tuple.hashCode(), shuffledTuple.hashCode());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessorsUsingFieldIndex")
    public void nullPointerWhenReadingNullAsPrimitiveAfterModification(
            ColumnType type,
            BiConsumer<Tuple, Integer> fieldAccessor
    ) {
        Tuple tuple = createNullValueTuple(type);

        tuple.set("NEW", null);

        var err = assertThrows(NullPointerException.class, () -> fieldAccessor.accept(tuple, 2));
        assertEquals(String.format(NULL_TO_PRIMITIVE_ERROR_MESSAGE, 2), err.getMessage());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessorsUsingFieldName")
    public void nullPointerWhenReadingNullByNameAsPrimitiveAfterModification(
            ColumnType type,
            BiConsumer<Tuple, String> fieldAccessor
    ) {
        Tuple tuple = createNullValueTuple(type);

        tuple.set("NEW", null);

        var err = assertThrows(NullPointerException.class, () -> fieldAccessor.accept(tuple, "NEW"));
        assertEquals(String.format(NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE, "NEW"), err.getMessage());
    }
}
