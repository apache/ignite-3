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

package org.apache.ignite.migrationtools.types;

import static org.apache.ignite.migrationtools.types.InspectedField.forNamed;
import static org.apache.ignite.migrationtools.types.TypeInspector.inspectType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.OrganizationType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TypeInspectorTest {
    @ParameterizedTest
    @MethodSource("primitiveTypes")
    void testPrimitiveFieldType(Class<?> primitiveKlass, String typeName) {
        List<InspectedField> inspectedTypes = inspectType(primitiveKlass);

        var expected = InspectedField.forUnnamed(typeName, InspectedFieldType.PRIMITIVE);
        assertThat(inspectedTypes).containsExactly(expected);
    }

    private static Stream<Arguments> primitiveTypes() {
        return Stream.of(
                arguments(boolean.class, Boolean.class.getName()),
                arguments(byte.class, Byte.class.getName()),
                arguments(char.class, Character.class.getName()),
                arguments(short.class, Short.class.getName()),
                arguments(int.class, Integer.class.getName()),
                arguments(long.class, Long.class.getName()),
                arguments(double.class, Double.class.getName()),
                arguments(float.class, Float.class.getName()),
                arguments(String.class, String.class.getName()),
                arguments(Boolean.class, Boolean.class.getName()),
                arguments(Byte.class, Byte.class.getName()),
                arguments(Character.class, Character.class.getName()),
                arguments(Short.class, Short.class.getName()),
                arguments(Integer.class, Integer.class.getName()),
                arguments(Long.class, Long.class.getName()),
                arguments(Double.class, Double.class.getName()),
                arguments(Float.class, Float.class.getName())
        );
    }

    @ParameterizedTest
    @MethodSource("arrayTypes")
    void testArrayFieldType(Class<?> primitiveKlass) {
        List<InspectedField> inspectedTypes = inspectType(primitiveKlass);

        var expected = InspectedField.forUnnamed(primitiveKlass.getName(), InspectedFieldType.ARRAY);
        assertThat(inspectedTypes).containsExactly(expected);
    }

    private static Stream<Arguments> arrayTypes() {
        return Stream.of(
                arguments(boolean[].class),
                arguments(byte[].class),
                arguments(char[].class),
                arguments(short[].class),
                arguments(int[].class),
                arguments(long[].class),
                arguments(double[].class),
                arguments(float[].class),
                arguments(String[].class),
                arguments(Boolean[].class),
                arguments(Byte[].class),
                arguments(Character[].class),
                arguments(Short[].class),
                arguments(Integer[].class),
                arguments(Long[].class),
                arguments(Double[].class),
                arguments(Float[].class),
                arguments(List.class)
        );
    }

    @Test
    void testNestedPojoAttribute() {
        List<InspectedField> inspectedTypes = inspectType(Organization.class);

        InspectedField[] expected = new InspectedField[] {
                forNamed("id", Long.class.getName(), InspectedFieldType.POJO_ATTRIBUTE, true, true),
                forNamed("name", String.class.getName(), InspectedFieldType.POJO_ATTRIBUTE, true, true),
                forNamed("addr", Address.class.getName(), InspectedFieldType.NESTED_POJO_ATTRIBUTE, true, false),
                forNamed("type", OrganizationType.class.getName(), InspectedFieldType.POJO_ATTRIBUTE, true, false),
                forNamed("lastUpdated", Timestamp.class.getName(), InspectedFieldType.POJO_ATTRIBUTE, true, false)
        };

        assertThat(inspectedTypes).containsExactly(expected);
    }
}
