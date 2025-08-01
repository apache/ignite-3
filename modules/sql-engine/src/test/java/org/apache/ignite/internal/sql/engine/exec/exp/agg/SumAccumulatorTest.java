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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.DecimalSumEmptyIsZero;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.DoubleSumEmptyIsZero;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.LongSumEmptyIsZero;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.Sum;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@code SUM} accumulator functions.
 */
public class SumAccumulatorTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @MethodSource("testArgs")
    public void test(Accumulator sum, Object result, Object[] args) {
        StatefulAccumulator acc = newCall(sum);

        for (var a : args) {
            acc.add(a);
        }

        assertEquals(result, acc.end());
    }

    private static Stream<Arguments> testArgs() {
        return Stream.of(
                Arguments.of(namedAccumulator(DoubleSumEmptyIsZero.FACTORY), 4.0d, new Object[]{3.0d, 1.0d}),
                Arguments.of(namedAccumulator(LongSumEmptyIsZero.FACTORY), 4L, new Object[]{3L, 1L}),

                Arguments.of(namedAccumulator(() -> DecimalSumEmptyIsZero.FACTORY.apply(1)), new BigDecimal("3.4"),
                        new Object[]{new BigDecimal("1.3"), new BigDecimal("2.1")}),

                Arguments.of(namedAccumulator(() -> DecimalSumEmptyIsZero.FACTORY.apply(1)), new BigDecimal("3.4"),
                        new Object[]{new BigDecimal("1.31"), new BigDecimal("2.13")}),
                Arguments.of(namedAccumulator(() -> DecimalSumEmptyIsZero.FACTORY.apply(1)), new BigDecimal("3.5"),
                        new Object[]{new BigDecimal("1.32"), new BigDecimal("2.23")}),

                Arguments.of(namedAccumulator(() -> DecimalSumEmptyIsZero.FACTORY.apply(2)), new BigDecimal("3.44"),
                        new Object[]{new BigDecimal("1.31"), new BigDecimal("2.13")}),
                Arguments.of(namedAccumulator(() -> DecimalSumEmptyIsZero.FACTORY.apply(2)), new BigDecimal("3.45"),
                        new Object[]{new BigDecimal("1.32"), new BigDecimal("2.13")})
        );
    }

    @ParameterizedTest
    @MethodSource("emptyArgs")
    public void empty(Accumulator sum) {
        StatefulAccumulator acc = newCall(sum);

        assertNull(acc.end());
    }

    private static Stream<Arguments> emptyArgs() {
        return Stream.of(
                Arguments.of(namedAccumulator(DoubleSumEmptyIsZero.FACTORY)),
                Arguments.of(namedAccumulator(LongSumEmptyIsZero.FACTORY)),
                Arguments.of(namedAccumulator(() -> DecimalSumEmptyIsZero.FACTORY.apply(1)))
        );
    }

    private StatefulAccumulator newCall(Accumulator accumulator) {
        return new StatefulAccumulator(new Sum(accumulator));
    }

    private static Named<Accumulator> namedAccumulator(Supplier<Accumulator> supplier) {
        Accumulator accumulator = supplier.get();
        return Named.of(accumulator.getClass().getSimpleName(), accumulator);
    }
}
