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

package org.apache.ignite.internal.schema.marshaller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.function.Function;
import org.apache.ignite.internal.schema.testobjects.TestOuterObject;
import org.apache.ignite.internal.schema.testobjects.TestOuterObject.NestedObject;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.MapperBuilder;
import org.junit.jupiter.api.Test;

/**
 * Columns mappers test.
 */
public class MapperTest {

    @Test
    public void misleadingMapperUsage() {
        // Empty mapping.
        assertThrows(IllegalStateException.class, () -> Mapper.builder(TestObject.class).build());
        // Many fields to one column.
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                .map("id", "key")
                .map("longCol", "key"));

        // One field to many columns.
        assertThrows(IllegalStateException.class, () -> Mapper.builder(TestObject.class)
                .map("id", "key")
                .map("id", "val1")
                .map("stringCol", "val2")
                .build());

        // Mapper builder reuse fails.
        assertThrows(IllegalStateException.class, () -> {
            MapperBuilder<TestObject> builder = Mapper.builder(TestObject.class)
                    .map("id", "key");

            builder.build();

            builder.map("stringCol", "val2");
        });
    }

    @Test
    public void supportedClassKinds() {
        class LocalClass {
            long id;
        }

        Function anonymous = (i) -> i;

        Mapper.builder(TestOuterObject.class);
        Mapper.builder(NestedObject.class);

        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(Long.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestOuterObject.InnerObject.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(AbstractTestObject.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(LocalClass.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(anonymous.getClass()));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(int[].class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(Object[].class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestInterface.class)); // Interface
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestAnnotation.class)); // annotation
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(EnumTestObject.class)); // enum

        Mapper.identity(Long.class);
        Mapper.identity(TestOuterObject.class);
        Mapper.identity(NestedObject.class);
        Mapper.identity(ArrayList.class);

        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(TestOuterObject.InnerObject.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(LocalClass.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(AbstractTestObject.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(anonymous.getClass()));
        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(int[].class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(Object[].class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(TestInterface.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(TestAnnotation.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.identity(EnumTestObject.class));

        Mapper.of(Long.class, "column");
        Mapper.of(TestOuterObject.class, "column");
        Mapper.of(NestedObject.class, "column");
        Mapper.of(AbstractTestObject.class, "column");
        Mapper.of(int[].class, "column");
        Mapper.of(Object.class, "column");
        Mapper.of(ArrayList.class, "column");
        Mapper.of(TestInterface.class, "column");

        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestOuterObject.InnerObject.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(LocalClass.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(anonymous.getClass(), "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestAnnotation.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(EnumTestObject.class, "column"));

    }
    @Test
    public void identityMapping() {
        Mapper<TestObject> mapper = Mapper.identity(TestObject.class);

        assertNull(mapper.mappedColumn());
        assertEquals("id", mapper.fieldForColumn("id"));
        assertNull(mapper.fieldForColumn("val"));
    }

    @Test
    public void basicMapping() {
        Mapper<TestObject> mapper = Mapper.identity(TestObject.class);

        assertNull(mapper.mappedColumn());
        assertEquals("id", mapper.fieldForColumn("id"));
        assertNull(mapper.fieldForColumn("val"));
    }

    /**
     * Test object.
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "unused"})
    static class TestObject {
        private long id;

        private long longCol;

        private String stringCol;
    }

    /**
     * Test object.
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "unused"})
    abstract static class AbstractTestObject {
        private long id;
    }

    /**
     * Test object.
     */
    enum EnumTestObject {
        ONE,
        TWO
    }

    /**
     * Test object.
     */
    @interface TestAnnotation {
        long id = 0L;
    }

    /**
     * Test object.
     */
    interface TestInterface {
        int id = 0;
    }
}
