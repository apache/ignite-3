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

import java.time.LocalTime;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link SqlTestUtils}.
 */
public class SqlTestUtilsSelfTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @MethodSource("timeLiterals")
    public void makeTimeLiteral(int precision, LocalTime value, String literal) {
        String actual = SqlTestUtils.makeLiteral(value, NativeTypes.time(precision));
        assertEquals(literal, actual);
    }

    private static Stream<Arguments> timeLiterals() {
        return Stream.of(
                Arguments.of(0, LocalTime.of(0, 0, 0), "TIME '00:00:00'"),
                Arguments.of(1, LocalTime.of(0, 0, 0), "TIME '00:00:00.0'"),
                Arguments.of(2, LocalTime.of(0, 0, 0), "TIME '00:00:00.00'"),
                Arguments.of(3, LocalTime.of(0, 0, 0), "TIME '00:00:00.000'"),
                Arguments.of(4, LocalTime.of(0, 0, 0), "TIME '00:00:00.0000'"),
                Arguments.of(5, LocalTime.of(0, 0, 0), "TIME '00:00:00.00000'"),
                Arguments.of(6, LocalTime.of(0, 0, 0), "TIME '00:00:00.000000'"),
                Arguments.of(7, LocalTime.of(0, 0, 0), "TIME '00:00:00.0000000'"),
                Arguments.of(8, LocalTime.of(0, 0, 0), "TIME '00:00:00.00000000'"),
                Arguments.of(9, LocalTime.of(0, 0, 0), "TIME '00:00:00.000000000'")
        );
    }
}
