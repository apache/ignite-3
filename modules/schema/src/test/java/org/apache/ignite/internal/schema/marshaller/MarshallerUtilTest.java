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

package org.apache.ignite.internal.schema.marshaller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link MarshallerUtil}.
 */
public class MarshallerUtilTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @MethodSource("getValueSizes")
    public void testGetValueSize(Object val, NativeType nativeType, int expectedSize) {
        int valueSize = MarshallerUtil.getValueSize(val, nativeType);
        assertEquals(expectedSize, valueSize);
    }

    private static Stream<Arguments> getValueSizes() {
        return Stream.of(
                // bytes
                Arguments.of(new byte[0], NativeTypes.BYTES, 1),
                Arguments.of(new byte[1], NativeTypes.BYTES, 1),
                Arguments.of(new byte[]{BinaryTupleCommon.VARLEN_EMPTY_BYTE}, NativeTypes.BYTES, 2),
                Arguments.of(new byte[]{BinaryTupleCommon.VARLEN_EMPTY_BYTE, 1}, NativeTypes.BYTES, 3),
                // pojo
                Arguments.of(new Object(), NativeTypes.BYTES, 0),
                // string
                Arguments.of("", NativeTypes.STRING, 1),
                Arguments.of("1", NativeTypes.STRING, 1),
                Arguments.of("abc", NativeTypes.STRING, 3),
                Arguments.of(new String(new byte[]{-36, -128}, StandardCharsets.UTF_8), NativeTypes.STRING, 2),
                Arguments.of(new String(new byte[]{-30, -104, -128}, StandardCharsets.UTF_8), NativeTypes.STRING, 3),
                Arguments.of(new String(new byte[]{97, -36, -128, -30, -104, -128}, StandardCharsets.UTF_8), NativeTypes.STRING, 6),
                // number
                Arguments.of(BigInteger.ONE, NativeTypes.numberOf(12), 1),
                Arguments.of(BigInteger.valueOf(123456789), NativeTypes.numberOf(12), 4),
                // decimal
                Arguments.of(BigDecimal.ONE, NativeTypes.decimalOf(12, 1), 3),
                Arguments.of(BigDecimal.valueOf(123456789), NativeTypes.decimalOf(12, 3), 6)
        );
    }
}
