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

package org.apache.ignite.internal.sql.engine.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Tests for {@link TypeUtils}.
 */
public class TypeUtilsTest {

    private final ExecutionContext<?> ectx = Mockito.mock(ExecutionContext.class);

    /**
     * Checks that conversions to and from internal types is consistent.
     *
     * @see TypeUtils#fromInternal(ExecutionContext, Object, Type) to internal.
     * @see TypeUtils#toInternal(ExecutionContext, Object, Type) from internal.
     */
    @ParameterizedTest
    @MethodSource("typeAndValue")
    public void testToFromInternalMatch(Object value, Class<?> type) {
        Object internal = TypeUtils.toInternal(ectx, value, type);
        assertNotNull(internal, "Conversion to internal has produced null");

        Object original = TypeUtils.fromInternal(ectx, internal, type);
        assertEquals(value, original, "toInternal -> fromInternal");
        assertNotNull(original, "Conversion from internal has produced null");

        Object internal2 = TypeUtils.toInternal(ectx, original);
        assertEquals(internal, internal2, "toInternal w/o type parameter");
    }

    private static Stream<Arguments> typeAndValue() {
        return Stream.of(
                Arguments.of((byte) 1, Byte.class),
                Arguments.of((short) 1, Short.class),
                Arguments.of(1, Integer.class),
                Arguments.of(1L, Long.class),
                Arguments.of(1.0F, Float.class),
                Arguments.of(1.0D, Double.class),
                Arguments.of("hello", String.class),
                Arguments.of(LocalDate.of(1970, 1, 1), LocalDate.class),
                Arguments.of(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0), LocalDateTime.class),
                Arguments.of(LocalTime.NOON, LocalTime.class),
                Arguments.of(new UUID(1, 1), UUID.class)
        );
    }
}
