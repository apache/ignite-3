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

package org.apache.ignite.configuration.sample;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.function.Supplier;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.sample.impl.TestArrayNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test configuration with array of primitive type (or {@link String}) fields.
 */
public class ConfigurationArrayTest {
    /** */
    @Config
    public static class TestArrayConfigurationSchema {
        /** */
        @Value
        private String[] array;
    }

    @Test
    public void test() {
        var arrayNode = new TestArrayNode();

        Supplier<String[]> initialSupplier = () -> {
            return new String[]{"test1", "test2"};
        };

        final String[] initialValue = initialSupplier.get();
        arrayNode.initArray(initialValue);

        // change value in initial array
        initialValue[0] = "doesn't matter";

        // test that changing value in initial array doesn't change value of array in arrayNode
        assertTrue(Arrays.equals(initialSupplier.get(), arrayNode.array()));

        // test that returned array is a copy of the field
        assertNotEquals(getField(arrayNode, "array"), arrayNode.array(), "Must be a copy of an array");

        Supplier<String[]> changeSupplier = () -> {
            return new String[]{"foo", "bar"};
        };

        final String[] changeValue = changeSupplier.get();
        arrayNode.changeArray(changeValue);

        // change value in change array
        changeValue[0] = "doesn't matter";

        // test that changing value in change array doesn't change value of array in arrayNode
        assertTrue(Arrays.equals(changeSupplier.get(), arrayNode.array()));

        // test that returned array is a copy of the field
        assertNotEquals(getField(arrayNode, "array"), arrayNode.array(), "Must be a copy of an array");
    }

    /**
     * Get field value via reflection.
     * @param object Object to get field from.
     * @param fieldName Field name.
     * @param <T> Type of the field.
     * @return Value of the field.
     */
    private <T> T getField(Object object, String fieldName) {
        try {
            final Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(object);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
