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

package org.apache.ignite.internal.sql.engine.expressions;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.api.expressions.EvaluationContext;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionEvaluationException;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionParsingException;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionValidationException;
import org.apache.ignite.internal.sql.engine.api.expressions.IgnitePredicate;
import org.apache.ignite.internal.sql.engine.api.expressions.RowAccessor;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@SuppressWarnings("ThrowableNotThrown")
class IgnitePredicateTest extends AbstractExpressionFactoryTest {
    private final ExpressionFactory factory = new SqlExpressionFactoryAdapter(
            new SqlExpressionFactoryImpl(
                    Commons.typeFactory(), 0, EmptyCacheFactory.INSTANCE
            )
    );

    @Test
    @TestFor(ColumnType.BOOLEAN)
    void booleanTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.BOOLEAN);

        {
            IgnitePredicate predicate = factory.predicate("val", inputType);

            assertThat(predicate.test(context, row(true)), is(true));
            assertThat(predicate.test(context, row(false)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val = true", inputType);

            assertThat(predicate.test(context, row(true)), is(true));
            assertThat(predicate.test(context, row(false)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> true", inputType);

            assertThat(predicate.test(context, row(true)), is(false));
            assertThat(predicate.test(context, row(false)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(true)), is(false));
            assertThat(predicate.test(context, row(false)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(true)), is(true));
            assertThat(predicate.test(context, row(false)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.INT8)
    void byteTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.INT8);

        {
            IgnitePredicate predicate = factory.predicate("val > 0", inputType);

            assertThat(predicate.test(context, row((byte) -100)), is(false));
            assertThat(predicate.test(context, row((byte) 0)), is(false));
            assertThat(predicate.test(context, row((byte) 100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 0", inputType);

            assertThat(predicate.test(context, row((byte) -100)), is(true));
            assertThat(predicate.test(context, row((byte) 0)), is(false));
            assertThat(predicate.test(context, row((byte) 100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= 0", inputType);

            assertThat(predicate.test(context, row((byte) -100)), is(true));
            assertThat(predicate.test(context, row((byte) 0)), is(true));
            assertThat(predicate.test(context, row((byte) 100)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row((byte) -100)), is(false));
            assertThat(predicate.test(context, row((byte) 0)), is(false));
            assertThat(predicate.test(context, row((byte) 100)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row((byte) -100)), is(true));
            assertThat(predicate.test(context, row((byte) 0)), is(true));
            assertThat(predicate.test(context, row((byte) 100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val < 0.1", inputType);

            assertThat(predicate.test(context, row((byte) -100)), is(true));
            assertThat(predicate.test(context, row((byte) 0)), is(true));
            assertThat(predicate.test(context, row((byte) 100)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val > 0.1", inputType);

            assertThat(predicate.test(context, row((byte) -100)), is(false));
            assertThat(predicate.test(context, row((byte) 0)), is(false));
            assertThat(predicate.test(context, row((byte) 100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.INT16)
    void shortTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.INT16);

        {
            IgnitePredicate predicate = factory.predicate("val > 0", inputType);

            assertThat(predicate.test(context, row((short) -100)), is(false));
            assertThat(predicate.test(context, row((short) 0)), is(false));
            assertThat(predicate.test(context, row((short) 100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 0", inputType);

            assertThat(predicate.test(context, row((short) -100)), is(true));
            assertThat(predicate.test(context, row((short) 0)), is(false));
            assertThat(predicate.test(context, row((short) 100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= 0", inputType);

            assertThat(predicate.test(context, row((short) -100)), is(true));
            assertThat(predicate.test(context, row((short) 0)), is(true));
            assertThat(predicate.test(context, row((short) 100)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row((short) -100)), is(false));
            assertThat(predicate.test(context, row((short) 0)), is(false));
            assertThat(predicate.test(context, row((short) 100)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row((short) -100)), is(true));
            assertThat(predicate.test(context, row((short) 0)), is(true));
            assertThat(predicate.test(context, row((short) 100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val < 0.1", inputType);

            assertThat(predicate.test(context, row((short) -100)), is(true));
            assertThat(predicate.test(context, row((short) 0)), is(true));
            assertThat(predicate.test(context, row((short) 100)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val > 0.1", inputType);

            assertThat(predicate.test(context, row((short) -100)), is(false));
            assertThat(predicate.test(context, row((short) 0)), is(false));
            assertThat(predicate.test(context, row((short) 100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.INT32)
    void intTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.INT32);

        {
            IgnitePredicate predicate = factory.predicate("val > 0", inputType);

            assertThat(predicate.test(context, row(-100)), is(false));
            assertThat(predicate.test(context, row(0)), is(false));
            assertThat(predicate.test(context, row(100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 0", inputType);

            assertThat(predicate.test(context, row(-100)), is(true));
            assertThat(predicate.test(context, row(0)), is(false));
            assertThat(predicate.test(context, row(100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= 0", inputType);

            assertThat(predicate.test(context, row(-100)), is(true));
            assertThat(predicate.test(context, row(0)), is(true));
            assertThat(predicate.test(context, row(100)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(-100)), is(false));
            assertThat(predicate.test(context, row(0)), is(false));
            assertThat(predicate.test(context, row(100)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(-100)), is(true));
            assertThat(predicate.test(context, row(0)), is(true));
            assertThat(predicate.test(context, row(100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val < 0.1", inputType);

            assertThat(predicate.test(context, row(-100)), is(true));
            assertThat(predicate.test(context, row(0)), is(true));
            assertThat(predicate.test(context, row(100)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val > 0.1", inputType);

            assertThat(predicate.test(context, row(-100)), is(false));
            assertThat(predicate.test(context, row(0)), is(false));
            assertThat(predicate.test(context, row(100)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.INT64)
    void longTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.INT64);

        {
            IgnitePredicate predicate = factory.predicate("val > 0", inputType);

            assertThat(predicate.test(context, row(-100L)), is(false));
            assertThat(predicate.test(context, row(0L)), is(false));
            assertThat(predicate.test(context, row(100L)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 0", inputType);

            assertThat(predicate.test(context, row(-100L)), is(true));
            assertThat(predicate.test(context, row(0L)), is(false));
            assertThat(predicate.test(context, row(100L)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= 0", inputType);

            assertThat(predicate.test(context, row(-100L)), is(true));
            assertThat(predicate.test(context, row(0L)), is(true));
            assertThat(predicate.test(context, row(100L)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(-100L)), is(false));
            assertThat(predicate.test(context, row(0L)), is(false));
            assertThat(predicate.test(context, row(100L)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(-100L)), is(true));
            assertThat(predicate.test(context, row(0L)), is(true));
            assertThat(predicate.test(context, row(100L)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val < 0.1", inputType);

            assertThat(predicate.test(context, row(-100L)), is(true));
            assertThat(predicate.test(context, row(0L)), is(true));
            assertThat(predicate.test(context, row(100L)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val > 0.1", inputType);

            assertThat(predicate.test(context, row(-100L)), is(false));
            assertThat(predicate.test(context, row(0L)), is(false));
            assertThat(predicate.test(context, row(100L)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.FLOAT)
    void floatTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.FLOAT);

        {
            IgnitePredicate predicate = factory.predicate("val > 0", inputType);

            assertThat(predicate.test(context, row(-100.5f)), is(false));
            assertThat(predicate.test(context, row(0.0f)), is(false));
            assertThat(predicate.test(context, row(100.5f)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Float.POSITIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Float.NEGATIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Float.NaN)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 0", inputType);

            assertThat(predicate.test(context, row(-100.5f)), is(true));
            assertThat(predicate.test(context, row(0.0f)), is(false));
            assertThat(predicate.test(context, row(100.5f)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Float.POSITIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Float.NEGATIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Float.NaN)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= 0", inputType);

            assertThat(predicate.test(context, row(-100.5f)), is(true));
            assertThat(predicate.test(context, row(0.0f)), is(true));
            assertThat(predicate.test(context, row(100.5f)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Float.POSITIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Float.NEGATIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Float.NaN)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(-100.5f)), is(false));
            assertThat(predicate.test(context, row(0.0f)), is(false));
            assertThat(predicate.test(context, row(100.5f)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
            assertThat(predicate.test(context, row(Float.POSITIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Float.NEGATIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Float.NaN)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(-100.5f)), is(true));
            assertThat(predicate.test(context, row(0.0f)), is(true));
            assertThat(predicate.test(context, row(100.5f)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Float.POSITIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Float.NEGATIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Float.NaN)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val < 0.1", inputType);

            assertThat(predicate.test(context, row(-100.5f)), is(true));
            assertThat(predicate.test(context, row(0.0f)), is(true));
            assertThat(predicate.test(context, row(100.5f)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Float.POSITIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Float.NEGATIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Float.NaN)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val > 0.1", inputType);

            assertThat(predicate.test(context, row(-100.5f)), is(false));
            assertThat(predicate.test(context, row(0.0f)), is(false));
            assertThat(predicate.test(context, row(100.5f)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Float.POSITIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Float.NEGATIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Float.NaN)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.DOUBLE)
    void doubleTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.DOUBLE);

        {
            IgnitePredicate predicate = factory.predicate("val > 0", inputType);

            assertThat(predicate.test(context, row(-100.5)), is(false));
            assertThat(predicate.test(context, row(0.0)), is(false));
            assertThat(predicate.test(context, row(100.5)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Double.POSITIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Double.NEGATIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Double.NaN)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 0", inputType);

            assertThat(predicate.test(context, row(-100.5)), is(true));
            assertThat(predicate.test(context, row(0.0)), is(false));
            assertThat(predicate.test(context, row(100.5)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Double.POSITIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Double.NEGATIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Double.NaN)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= 0", inputType);

            assertThat(predicate.test(context, row(-100.5)), is(true));
            assertThat(predicate.test(context, row(0.0)), is(true));
            assertThat(predicate.test(context, row(100.5)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Double.POSITIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Double.NEGATIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Double.NaN)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(-100.5)), is(false));
            assertThat(predicate.test(context, row(0.0)), is(false));
            assertThat(predicate.test(context, row(100.5)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
            assertThat(predicate.test(context, row(Double.POSITIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Double.NEGATIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Double.NaN)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(-100.5)), is(true));
            assertThat(predicate.test(context, row(0.0)), is(true));
            assertThat(predicate.test(context, row(100.5)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Double.POSITIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Double.NEGATIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Double.NaN)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val < 0.1", inputType);

            assertThat(predicate.test(context, row(-100.5)), is(true));
            assertThat(predicate.test(context, row(0.0)), is(true));
            assertThat(predicate.test(context, row(100.5)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Double.POSITIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Double.NEGATIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Double.NaN)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val > 0.1", inputType);

            assertThat(predicate.test(context, row(-100.5)), is(false));
            assertThat(predicate.test(context, row(0.0)), is(false));
            assertThat(predicate.test(context, row(100.5)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
            assertThat(predicate.test(context, row(Double.POSITIVE_INFINITY)), is(true));
            assertThat(predicate.test(context, row(Double.NEGATIVE_INFINITY)), is(false));
            assertThat(predicate.test(context, row(Double.NaN)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.DECIMAL)
    void decimalTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.decimalOf(19, 3));

        {
            IgnitePredicate predicate = factory.predicate("val > 0", inputType);

            assertThat(predicate.test(context, row(new BigDecimal("-100.5"))), is(false));
            assertThat(predicate.test(context, row(BigDecimal.ZERO)), is(false));
            assertThat(predicate.test(context, row(new BigDecimal("100.5"))), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 0", inputType);

            assertThat(predicate.test(context, row(new BigDecimal("-100.5"))), is(true));
            assertThat(predicate.test(context, row(BigDecimal.ZERO)), is(false));
            assertThat(predicate.test(context, row(new BigDecimal("100.5"))), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= 0", inputType);

            assertThat(predicate.test(context, row(new BigDecimal("-100.5"))), is(true));
            assertThat(predicate.test(context, row(BigDecimal.ZERO)), is(true));
            assertThat(predicate.test(context, row(new BigDecimal("100.5"))), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(new BigDecimal("-100.5"))), is(false));
            assertThat(predicate.test(context, row(BigDecimal.ZERO)), is(false));
            assertThat(predicate.test(context, row(new BigDecimal("100.5"))), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(new BigDecimal("-100.5"))), is(true));
            assertThat(predicate.test(context, row(BigDecimal.ZERO)), is(true));
            assertThat(predicate.test(context, row(new BigDecimal("100.5"))), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val < 0.1", inputType);

            assertThat(predicate.test(context, row(new BigDecimal("-100.5"))), is(true));
            assertThat(predicate.test(context, row(BigDecimal.ZERO)), is(true));
            assertThat(predicate.test(context, row(new BigDecimal("100.5"))), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val > 0.1", inputType);

            assertThat(predicate.test(context, row(new BigDecimal("-100.5"))), is(false));
            assertThat(predicate.test(context, row(BigDecimal.ZERO)), is(false));
            assertThat(predicate.test(context, row(new BigDecimal("100.5"))), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.DATE)
    void dateTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.DATE);

        LocalDate past = LocalDate.of(2020, 1, 1);
        LocalDate present = LocalDate.of(2023, 6, 15);
        LocalDate future = LocalDate.of(2025, 12, 31);

        {
            IgnitePredicate predicate = factory.predicate("val > DATE '2023-06-15'", inputType);

            assertThat(predicate.test(context, row(past)), is(false));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> DATE '2023-06-15'", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= DATE '2023-06-15'", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(true));
            assertThat(predicate.test(context, row(future)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(past)), is(false));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(true));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.TIME)
    void timeTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.time(0));

        LocalTime early = LocalTime.of(8, 0, 0);
        LocalTime noon = LocalTime.of(12, 0, 0);
        LocalTime late = LocalTime.of(18, 0, 0);

        {
            IgnitePredicate predicate = factory.predicate("val > TIME '12:00:00'", inputType);

            assertThat(predicate.test(context, row(early)), is(false));
            assertThat(predicate.test(context, row(noon)), is(false));
            assertThat(predicate.test(context, row(late)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> TIME '12:00:00'", inputType);

            assertThat(predicate.test(context, row(early)), is(true));
            assertThat(predicate.test(context, row(noon)), is(false));
            assertThat(predicate.test(context, row(late)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= TIME '12:00:00'", inputType);

            assertThat(predicate.test(context, row(early)), is(true));
            assertThat(predicate.test(context, row(noon)), is(true));
            assertThat(predicate.test(context, row(late)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(early)), is(false));
            assertThat(predicate.test(context, row(noon)), is(false));
            assertThat(predicate.test(context, row(late)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(early)), is(true));
            assertThat(predicate.test(context, row(noon)), is(true));
            assertThat(predicate.test(context, row(late)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.DATETIME)
    void datetimeTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.datetime(6));

        LocalDateTime past = LocalDateTime.of(2020, 1, 1, 10, 0, 0);
        LocalDateTime present = LocalDateTime.of(2023, 6, 15, 12, 0, 0);
        LocalDateTime future = LocalDateTime.of(2025, 12, 31, 14, 0, 0);

        {
            IgnitePredicate predicate = factory.predicate("val > TIMESTAMP '2023-06-15 12:00:00'", inputType);

            assertThat(predicate.test(context, row(past)), is(false));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> TIMESTAMP '2023-06-15 12:00:00'", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= TIMESTAMP '2023-06-15 12:00:00'", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(true));
            assertThat(predicate.test(context, row(future)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(past)), is(false));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(true));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.TIMESTAMP)
    void timestampTest() throws Exception {
        StructNativeType inputType = singleColumnType(NativeTypes.timestamp(6));

        Instant past = Instant.parse("2020-01-01T10:00:00Z");
        Instant present = Instant.parse("2023-06-15T12:00:00Z");
        Instant future = Instant.parse("2025-12-31T14:00:00Z");

        EvaluationContext<Object[]> context = context(present);

        {
            IgnitePredicate predicate = factory.predicate("val > CURRENT_TIMESTAMP", inputType);

            assertThat(predicate.test(context, row(past)), is(false));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> CURRENT_TIMESTAMP", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= CURRENT_TIMESTAMP", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(true));
            assertThat(predicate.test(context, row(future)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(past)), is(false));
            assertThat(predicate.test(context, row(present)), is(false));
            assertThat(predicate.test(context, row(future)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(past)), is(true));
            assertThat(predicate.test(context, row(present)), is(true));
            assertThat(predicate.test(context, row(future)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.UUID)
    void uuidTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.UUID);

        UUID uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID uuid2 = UUID.fromString("12345678-1234-1234-1234-123456789012");
        UUID uuid3 = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

        {
            IgnitePredicate predicate = factory.predicate("val > uuid '12345678-1234-1234-1234-123456789012'", inputType);

            assertThat(predicate.test(context, row(uuid1)), is(false));
            assertThat(predicate.test(context, row(uuid2)), is(false));
            assertThat(predicate.test(context, row(uuid3)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> uuid '12345678-1234-1234-1234-123456789012'", inputType);

            assertThat(predicate.test(context, row(uuid1)), is(true));
            assertThat(predicate.test(context, row(uuid2)), is(false));
            assertThat(predicate.test(context, row(uuid3)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= uuid '12345678-1234-1234-1234-123456789012'", inputType);

            assertThat(predicate.test(context, row(uuid1)), is(true));
            assertThat(predicate.test(context, row(uuid2)), is(true));
            assertThat(predicate.test(context, row(uuid3)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(uuid1)), is(false));
            assertThat(predicate.test(context, row(uuid2)), is(false));
            assertThat(predicate.test(context, row(uuid3)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(uuid1)), is(true));
            assertThat(predicate.test(context, row(uuid2)), is(true));
            assertThat(predicate.test(context, row(uuid3)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.NULL)
    void nullTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.NULL);

        {
            IgnitePredicate predicate = factory.predicate("CAST(val AS BOOLEAN)", inputType);

            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val > 0", inputType);

            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 0", inputType);

            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.STRING)
    void stringTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.stringOf(100));

        {
            IgnitePredicate predicate = factory.predicate("val > 'medium'", inputType);

            assertThat(predicate.test(context, row("aaa")), is(false));
            assertThat(predicate.test(context, row("medium")), is(false));
            assertThat(predicate.test(context, row("zzz")), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> 'medium'", inputType);

            assertThat(predicate.test(context, row("aaa")), is(true));
            assertThat(predicate.test(context, row("medium")), is(false));
            assertThat(predicate.test(context, row("zzz")), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= 'medium'", inputType);

            assertThat(predicate.test(context, row("aaa")), is(true));
            assertThat(predicate.test(context, row("medium")), is(true));
            assertThat(predicate.test(context, row("zzz")), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row("aaa")), is(false));
            assertThat(predicate.test(context, row("medium")), is(false));
            assertThat(predicate.test(context, row("zzz")), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row("aaa")), is(true));
            assertThat(predicate.test(context, row("medium")), is(true));
            assertThat(predicate.test(context, row("zzz")), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val LIKE '%ed%'", inputType);

            assertThat(predicate.test(context, row("aaa")), is(false));
            assertThat(predicate.test(context, row("medium")), is(true));
            assertThat(predicate.test(context, row("zzz")), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @Test
    @TestFor(ColumnType.BYTE_ARRAY)
    void byteArrayTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        StructNativeType inputType = singleColumnType(NativeTypes.blobOf(100));

        byte[] small = {1, 2, 3};
        byte[] medium = {10, 20, 30};
        byte[] large = {100, 127, -128};

        {
            IgnitePredicate predicate = factory.predicate("val > x'0a141e'", inputType);

            assertThat(predicate.test(context, row(small)), is(false));
            assertThat(predicate.test(context, row(medium)), is(false));
            assertThat(predicate.test(context, row(large)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <> x'0a141e'", inputType);

            assertThat(predicate.test(context, row(small)), is(true));
            assertThat(predicate.test(context, row(medium)), is(false));
            assertThat(predicate.test(context, row(large)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val <= x'0a141e'", inputType);

            assertThat(predicate.test(context, row(small)), is(true));
            assertThat(predicate.test(context, row(medium)), is(true));
            assertThat(predicate.test(context, row(large)), is(false));
            assertThat(predicate.test(context, row(null)), is(false));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NULL", inputType);

            assertThat(predicate.test(context, row(small)), is(false));
            assertThat(predicate.test(context, row(medium)), is(false));
            assertThat(predicate.test(context, row(large)), is(false));
            assertThat(predicate.test(context, row(null)), is(true));
        }

        {
            IgnitePredicate predicate = factory.predicate("val IS NOT NULL", inputType);

            assertThat(predicate.test(context, row(small)), is(true));
            assertThat(predicate.test(context, row(medium)), is(true));
            assertThat(predicate.test(context, row(large)), is(true));
            assertThat(predicate.test(context, row(null)), is(false));
        }
    }

    @ParameterizedTest
    @CsvSource(delimiterString = ";", value = {
            "val bar baz; Encountered \"bar\" at line 1, column 5",
            "bar#baz; Lexical error at line 1, column 4.  Encountered: \"#\" (35)"
    })
    void parsingErrors(String expression, String expectedMessage) {
        assertThrows(
                ExpressionParsingException.class,
                () -> factory.predicate(expression, singleColumnType(NativeTypes.INT32)),
                expectedMessage
        );
    }

    @Test
    void validationErrors() {
        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("val", singleColumnType(NativeTypes.INT32)),
                "Expected BOOLEAN expression but INTEGER was provided"
        );

        // Reference to table is a bit confusing. We'll improve that later. 
        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("non_existing_col > 5", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 1 to line 1, column 16: Column 'NON_EXISTING_COL' not found in any table"
        );

        // Same here. 
        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("non_existing_row_alias.val > 5", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 1 to line 1, column 22: Table 'NON_EXISTING_ROW_ALIAS' not found"
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("val = '12'", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 1 to line 1, column 10: Cannot apply '=' to arguments of type" 
                        + " '<INTEGER> = <CHAR(2)>'. Supported form(s): '<EQUIVALENT_TYPE> = <EQUIVALENT_TYPE>'"
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("EXISTS (SELECT 1 FROM test)", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 8 to line 1, column 27: Subqueries are not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("val > 10 AND val = (SELECT id FROM test) AND val < 5", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 20 to line 1, column 40: Subqueries are not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("5 < AVG(val)", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 5 to line 1, column 12: Aggregates are not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("CURRENT_USER", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 1 to line 1, column 12: CURRENT_USER is not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("LOCALTIME", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 1 to line 1, column 9: LOCALTIME is not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("LOCALTIMESTAMP", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 1 to line 1, column 14: LOCALTIMESTAMP is not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.predicate("CURRENT_DATE", singleColumnType(NativeTypes.INT32)),
                "From line 1, column 1 to line 1, column 12: CURRENT_DATE is not supported in given context."
        );
    }

    @Test
    void runtimeErrors() {
        EvaluationContext<Object[]> context = context();

        assertThrows(
                ExpressionEvaluationException.class,
                () -> factory.predicate("val / 0 > 10", singleColumnType(NativeTypes.INT32)).test(context, row(1)),
                "Division by zero"
        );

        assertThrows(
                ExpressionEvaluationException.class,
                () -> factory.predicate("CAST('abaracadabara' AS BOOLEAN)", singleColumnType(NativeTypes.INT32)).test(context, row(1)),
                "Invalid character for cast: abaracadabara"
        );
    }

    private EvaluationContext<Object[]> context() {
        return factory.<Object[]>contextBuilder()
                .rowAccessor(ObjectArrayAccessor.INSTANCE)
                .build();
    }

    private EvaluationContext<Object[]> context(Instant currentTime) {
        return factory.<Object[]>contextBuilder()
                .rowAccessor(ObjectArrayAccessor.INSTANCE)
                .timeProvider(currentTime::toEpochMilli)
                .build();
    }

    private static StructNativeType singleColumnType(NativeType type) {
        return NativeTypes.structBuilder()
                .addField("VAL", type, true)
                .build();
    }

    private static @Nullable Object[] row(@Nullable Object value) {
        return new Object[] {value};
    }

    static class ObjectArrayAccessor implements RowAccessor<Object[]> {
        private static final ObjectArrayAccessor INSTANCE = new ObjectArrayAccessor();

        @Override
        public @Nullable Object get(int field, Object[] row) {
            return row[field];
        }

        @Override
        public int columnsCount(Object[] row) {
            return row.length;
        }

        @Override
        public boolean isNull(int field, Object[] row) {
            return row[field] == null;
        }
    }
}
