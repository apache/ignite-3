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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.MinMaxAccumulator;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.VarBinaryMinMax;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.VarCharMinMax;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@code MIN/MAX} accumulator functions.
 */
public class MinMaxAccumulatorTest {

    @ParameterizedTest
    @MethodSource("minMaxArgs")
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void min(NativeType nativeType, Comparable[] vals) {
        StatefulAccumulator acc = newCall(true, nativeType);

        for (var val : vals) {
            acc.add(val);
        }

        acc.add(new Object[]{null});

        Object result = Arrays.stream(vals).min(Comparator.naturalOrder()).orElseThrow();

        assertEquals(result, acc.end());
    }

    @ParameterizedTest
    @MethodSource("minMaxArgs")
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void max(NativeType nativeType, Comparable[] vals) {
        StatefulAccumulator acc = newCall(false, nativeType);

        for (var val : vals) {
            acc.add(val);
        }

        acc.add(new Object[]{null});

        Object result = Arrays.stream(vals).max(Comparator.naturalOrder()).orElseThrow();

        assertEquals(result, acc.end());
    }

    private static Stream<Arguments> minMaxArgs() {
        return Stream.of(
                Arguments.of(NativeTypes.INT8, new Comparable[]{(byte) 2, (byte) 1}),
                Arguments.of(NativeTypes.INT16, new Comparable[]{(short) 2, (short) 1}),
                Arguments.of(NativeTypes.INT32, new Comparable[]{2, 1}),
                Arguments.of(NativeTypes.INT64, new Comparable[]{2L, 1L}),
                Arguments.of(NativeTypes.FLOAT, new Comparable[]{2.0f, 1.0f}),
                Arguments.of(NativeTypes.DOUBLE, new Comparable[]{2.0d, 1.0d}),
                Arguments.of(NativeTypes.decimalOf(2, 1), new Comparable[]{new BigDecimal("20.1"), new BigDecimal(("10.8"))}),
                Arguments.of(NativeTypes.stringOf(10), new Comparable[]{"abc", "abcd"}),
                Arguments.of(NativeTypes.stringOf(10), new Comparable[]{"a", "b"}),
                Arguments.of(NativeTypes.blobOf(10), new Comparable[]{newByteString("abc"), newByteString("abcd")}),
                Arguments.of(NativeTypes.blobOf(10), new Comparable[]{newByteString("a"), newByteString("b")})
        );
    }

    private static ByteString newByteString(String str) {
        return new ByteString(str.getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @MethodSource("emptyArgs")
    public void rejectNull(NativeType nativeType) {
        StatefulAccumulator acc = newCall(true, nativeType);

        assertThrows(NullPointerException.class, () -> acc.add(null));
    }

    private static Stream<NativeType> emptyArgs() {
        return Stream.of(
                NativeTypes.INT8,
                NativeTypes.INT16,
                NativeTypes.INT32,
                NativeTypes.INT64,
                NativeTypes.FLOAT,
                NativeTypes.DOUBLE,
                NativeTypes.decimalOf(2, 1)
        );
    }

    private StatefulAccumulator newCall(boolean min, NativeType nativeType) {
        IgniteTypeFactory tf = Commons.typeFactory();
        RelDataType dataType = TypeUtils.native2relationalType(tf, nativeType);

        Supplier<Accumulator> supplier;
        if (nativeType.spec() == ColumnType.STRING) {
            supplier = min ? VarCharMinMax.MIN_FACTORY : VarCharMinMax.MAX_FACTORY;
        } else if (nativeType.spec() == ColumnType.BYTE_ARRAY) {
            supplier = min ? VarBinaryMinMax.MIN_FACTORY : VarBinaryMinMax.MAX_FACTORY;
        } else {
            supplier = MinMaxAccumulator.newAccumulator(min, tf, dataType);
        }

        return new StatefulAccumulator(supplier);
    }
}
