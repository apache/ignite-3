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

package org.apache.ignite.internal.sql.engine.datatypes.tests;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TestTypeArguments}.
 */
public class TestTypeArgumentsTest {

    private final TestType type = new TestType();

    private final TestDataSamples<String> samples = type.createSamples(Commons.typeFactory());

    /**
     * Tests for {@link TestTypeArguments#unary(DataTypeTestSpec, TestDataSamples, Comparable)}.
     */
    @Test
    public void testUnary() {
        List<String> args = TestTypeArguments.unary(type, samples, "1").map(TestTypeArguments::toString).collect(Collectors.toList());

        List<String> expected = List.of("TestType: 1", "INTEGER: 1", "BIGINT: 1");
        assertEquals(expected, args);
    }

    /**
     * Tests for {@link TestTypeArguments#binary(DataTypeTestSpec, TestDataSamples, Comparable, Comparable)}.
     */
    @Test
    public void testBinary() {
        List<String> args = TestTypeArguments.binary(type, samples, "1", "2")
                .map(TestTypeArguments::toString)
                .collect(Collectors.toList());

        List<String> expected = List.of("TestType TestType: 1 2",
                "TestType INTEGER: 1 2", "TestType BIGINT: 1 2",
                "INTEGER TestType: 1 2", "BIGINT TestType: 1 2");
        assertEquals(expected, args);
    }

    /**
     * Tests for {@link TestTypeArguments#nary(DataTypeTestSpec, TestDataSamples, Comparable, Comparable[])}.
     */
    @Test
    public void testNary() {
        List<String> args = TestTypeArguments.nary(type, samples, "1", "2", "3")
                .map(TestTypeArguments::toString)
                .collect(Collectors.toList());

        List<String> expected = List.of(
                "TestType TestType TestType: 1 2 3",
                "TestType INTEGER INTEGER: 1 2 3",
                "TestType BIGINT BIGINT: 1 2 3");
        assertEquals(expected, args);
    }

    private static final class TestType extends DataTypeTestSpec<String> {

        TestType() {
            super(ColumnType.INT8, "TestType", String.class);
        }

        @Override
        public boolean hasLiterals() {
            return false;
        }

        @Override
        public String toLiteral(String value) {
            return format("'{}'", value);
        }

        @Override
        public String toValueExpr(String value) {
            return format("'{}'::VARCHAR");
        }

        @Override
        public String toStringValue(String value) {
            return value;
        }

        @Override
        public String wrapIfNecessary(Object storageValue) {
            return (String) storageValue;
        }

        @Override
        public TestDataSamples<String> createSamples(IgniteTypeFactory typeFactory) {
            List<String> values = List.of("1", "2", "3");

            return TestDataSamples.<String>builder()
                    .add(values, SqlTypeName.INTEGER, String::valueOf)
                    .add(values, SqlTypeName.BIGINT, String::valueOf)
                    .build();
        }
    }
}
