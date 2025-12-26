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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests server tuple builder implementation.
 *
 * <p>The class contains implementation-specific tests. Tuple interface contract conformance/violation tests are inherited from the base
 * class.
 */
public class TupleImplTest extends AbstractMutableTupleTest {
    @Override
    protected Tuple createTuple(Function<Tuple, Tuple> transformer) {
        return transformer.apply(new TupleImpl());
    }

    @Override
    protected Tuple createTupleOfSingleColumn(ColumnType type, String columnName, Object value) {
        return new TupleImpl().set(columnName, value);
    }

    @Test
    void testTupleFactoryMethods() {
        assertEquals(Tuple.create(), Tuple.create(10));
        assertEquals(Tuple.create().set("id", 42L), Tuple.create(10).set("id", 42L));

        assertEquals(getTuple(), Tuple.copy(getTuple()));
        assertEquals(getTupleWithColumnOfAllTypes(), Tuple.copy(getTupleWithColumnOfAllTypes()));

        assertEquals(Tuple.create().set("id", 42L).set("NAME", "universe"),
                Tuple.create(Map.of("ID", 42L, "name", "universe")));

        assertEquals(Tuple.create().set("ID", 42L).set("\"name\"", "universe"),
                Tuple.create(Map.of("\"ID\"", 42L, "\"name\"", "universe")));

        assertEquals(Tuple.create().set("id", 42L).set("NAME", "universe"),
                Tuple.copy(Tuple.create().set("ID", 42L).set("name", "universe")));

        assertEquals(Tuple.create().set("id", 42L).set("\"name\"", "universe"),
                Tuple.copy(Tuple.create().set("\"ID\"", 42L).set("\"name\"", "universe")));
    }

    @Test
    void testValueOrDefaultSkipNormalization() {
        TupleImpl tuple = new TupleImpl();

        tuple.set("name", "normalized").set("\"Name\"", "non-normalized");

        assertEquals("normalized", tuple.valueOrDefaultSkipNormalization("NAME", "default"));

        // must not be found by non normalized name, this method doesn't do normalization
        assertEquals("default", tuple.valueOrDefaultSkipNormalization("name", "default"));
        assertEquals("default", tuple.valueOrDefaultSkipNormalization("\"NAME\"", "default"));

        // must be found by non normalized name, regular method does normalization
        assertEquals("normalized", tuple.valueOrDefault("name", "default"));
        assertEquals("normalized", tuple.valueOrDefault("\"NAME\"", "default"));

        assertEquals("non-normalized", tuple.valueOrDefaultSkipNormalization("Name", "default"));

        // must not be found by non normalized name, this method doesn't do normalization
        assertEquals("default", tuple.valueOrDefaultSkipNormalization("\"Name\"", "default"));

        // must be found by non normalized name, regular method does normalization
        assertEquals("non-normalized", tuple.valueOrDefault("\"Name\"", "default"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessors")
    void nullPointerWhenReadingNullAsPrimitive(Consumer<Tuple> fieldAccessor) {
        Tuple tuple = Tuple.create().set("VAL", null);

        var err = assertThrows(NullPointerException.class, () -> fieldAccessor.accept(tuple));
        assertEquals("The field value is null and cannot be converted to a primitive data type.", err.getMessage());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("primitiveAccessorsByName")
    void nullPointerWhenReadingNullByNameAsPrimitive(Consumer<Tuple> fieldAccessor) {
        Tuple tuple = Tuple.create().set("VAL", null);

        var err = assertThrows(NullPointerException.class, () -> fieldAccessor.accept(tuple));
        assertEquals("The field value is null and cannot be converted to a primitive data type.", err.getMessage());
    }

    private static List<Arguments> primitiveAccessors() {
        return List.of(
                Arguments.of(Named.of("boolean", (Consumer<Tuple>) tuple -> tuple.booleanValue(0))),
                Arguments.of(Named.of("byte", (Consumer<Tuple>) tuple -> tuple.byteValue(0))),
                Arguments.of(Named.of("short", (Consumer<Tuple>) tuple -> tuple.shortValue(0))),
                Arguments.of(Named.of("int", (Consumer<Tuple>) tuple -> tuple.intValue(0))),
                Arguments.of(Named.of("long", (Consumer<Tuple>) tuple -> tuple.longValue(0))),
                Arguments.of(Named.of("float", (Consumer<Tuple>) tuple -> tuple.floatValue(0))),
                Arguments.of(Named.of("double", (Consumer<Tuple>) tuple -> tuple.doubleValue(0)))
        );
    }

    private static List<Arguments> primitiveAccessorsByName() {
        return List.of(
                Arguments.of(Named.of("boolean", (Consumer<Tuple>) tuple -> tuple.booleanValue("VAL"))),
                Arguments.of(Named.of("byte", (Consumer<Tuple>) tuple -> tuple.byteValue("VAL"))),
                Arguments.of(Named.of("short", (Consumer<Tuple>) tuple -> tuple.shortValue("VAL"))),
                Arguments.of(Named.of("int", (Consumer<Tuple>) tuple -> tuple.intValue("VAL"))),
                Arguments.of(Named.of("long", (Consumer<Tuple>) tuple -> tuple.longValue("VAL"))),
                Arguments.of(Named.of("float", (Consumer<Tuple>) tuple -> tuple.floatValue("VAL"))),
                Arguments.of(Named.of("double", (Consumer<Tuple>) tuple -> tuple.doubleValue("VAL")))
        );
    }
}
