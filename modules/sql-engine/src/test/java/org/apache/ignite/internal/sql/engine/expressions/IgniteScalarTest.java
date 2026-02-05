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
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.api.expressions.EvaluationContext;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionEvaluationException;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionParsingException;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionValidationException;
import org.apache.ignite.internal.sql.engine.api.expressions.IgniteScalar;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@SuppressWarnings("ThrowableNotThrown")
class IgniteScalarTest extends AbstractExpressionFactoryTest {
    @Test
    @TestFor(ColumnType.BOOLEAN)
    void booleanTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.BOOLEAN;

        assertThat(factory.scalar("true", resultType).get(context), is(true));
        assertThat(factory.scalar("false", resultType).get(context), is(false));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
        assertThat(
                factory.scalar("CURRENT_TIMESTAMP + interval '1' day > CURRENT_TIMESTAMP", resultType)
                        .get(context),
                is(true)
        );
    }

    @Test
    @TestFor(ColumnType.INT8)
    void byteTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.INT8;

        assertThat(factory.scalar("20", resultType).get(context), is((byte) 20));
        assertThat(factory.scalar("20.20", resultType).get(context), is((byte) 20));
        assertThat(factory.scalar("-20", resultType).get(context), is((byte) -20));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
        assertThat(factory.scalar("LENGTH('foobar')", resultType).get(context), is((byte) 6));
    }

    @Test
    @TestFor(ColumnType.INT16)
    void shortTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.INT16;

        assertThat(factory.scalar("20", resultType).get(context), is((short) 20));
        assertThat(factory.scalar("20.20", resultType).get(context), is((short) 20));
        assertThat(factory.scalar("-20", resultType).get(context), is((short) -20));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
        assertThat(factory.scalar("LENGTH('foobar')", resultType).get(context), is((short) 6));
    }

    @Test
    @TestFor(ColumnType.INT32)
    void intTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.INT32;

        assertThat(factory.scalar("20", resultType).get(context), is(20));
        assertThat(factory.scalar("20.20", resultType).get(context), is(20));
        assertThat(factory.scalar("-20", resultType).get(context), is(-20));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
        assertThat(factory.scalar("LENGTH('foobar')", resultType).get(context), is(6));
    }

    @Test
    @TestFor(ColumnType.INT64)
    void longTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.INT64;

        assertThat(factory.scalar("20", resultType).get(context), is((long) 20));
        assertThat(factory.scalar("20.20", resultType).get(context), is((long) 20));
        assertThat(factory.scalar("-20", resultType).get(context), is((long) -20));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
        assertThat(factory.scalar("LENGTH('foobar')", resultType).get(context), is((long) 6));
    }

    @Test
    @TestFor(ColumnType.FLOAT)
    void floatTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.FLOAT;

        assertThat(factory.scalar("20", resultType).get(context), is(20.0f));
        assertThat(factory.scalar("20.20", resultType).get(context), is(20.2f));
        assertThat(factory.scalar("-20", resultType).get(context), is(-20.0f));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
        assertThat(factory.scalar("LENGTH('foobar')", resultType).get(context), is(6.0f));
        assertThat(factory.scalar("CAST('NaN' as REAL)", resultType).get(context), is(Float.NaN));
        assertThat(factory.scalar("CAST('Infinity' as REAL)", resultType).get(context), is(Float.POSITIVE_INFINITY));
        assertThat(factory.scalar("CAST('-Infinity' as REAL)", resultType).get(context), is(Float.NEGATIVE_INFINITY));
    }

    @Test
    @TestFor(ColumnType.DOUBLE)
    void doubleTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.DOUBLE;

        assertThat(factory.scalar("20", resultType).get(context), is(20.0d));
        assertThat(factory.scalar("20.20", resultType).get(context), is(20.2d));
        assertThat(factory.scalar("-20", resultType).get(context), is(-20.0d));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
        assertThat(factory.scalar("LENGTH('foobar')", resultType).get(context), is(6.0d));
        assertThat(factory.scalar("CAST('NaN' as DOUBLE)", resultType).get(context), is(Double.NaN));
        assertThat(factory.scalar("CAST('Infinity' as DOUBLE)", resultType).get(context), is(Double.POSITIVE_INFINITY));
        assertThat(factory.scalar("CAST('-Infinity' as DOUBLE)", resultType).get(context), is(Double.NEGATIVE_INFINITY));
    }

    @Test
    @TestFor(ColumnType.DECIMAL)
    void decimalTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.decimalOf(19, 3);

        assertThat(factory.scalar("20", resultType).get(context), is(new BigDecimal("20.000")));
        assertThat(factory.scalar("20.20", resultType).get(context), is(new BigDecimal("20.200")));
        assertThat(factory.scalar("-20", resultType).get(context), is(new BigDecimal("-20.000")));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
        assertThat(factory.scalar("LENGTH('foobar')", resultType).get(context), is(new BigDecimal("6.000")));
    }

    @Test
    @TestFor(ColumnType.DATE)
    void dateTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.DATE;

        assertThat(factory.scalar("date '2020-01-01'", resultType).get(context), is(LocalDate.of(2020, 1, 1)));
        assertThat(factory.scalar("date '2023-05-20'", resultType).get(context), is(LocalDate.of(2023, 5, 20)));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
    }

    @Test
    @TestFor(ColumnType.TIME)
    void timeTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.time(0);

        assertThat(factory.scalar("time '12:00:00'", resultType).get(context), is(LocalTime.of(12, 0, 0)));
        assertThat(factory.scalar("time '16:30:23'", resultType).get(context), is(LocalTime.of(16, 30, 23)));
        assertThat(factory.scalar("time '20:15:23.123'", resultType).get(context), is(LocalTime.of(20, 15, 23)));
        assertThat(
                factory.scalar("time '20:15:23.1234'", NativeTypes.time(1)).get(context),
                is(LocalTime.of(20, 15, 23, 100_000_000))
        );
        assertThat(
                factory.scalar("time '20:15:23.1234'", NativeTypes.time(2)).get(context),
                is(LocalTime.of(20, 15, 23, 120_000_000))
        );
        assertThat(
                factory.scalar("time '20:15:23.1234'", NativeTypes.time(3)).get(context),
                is(LocalTime.of(20, 15, 23, 123_000_000))
        );
        assertThat(
                factory.scalar("time '20:15:23.1234'", NativeTypes.time(4)).get(context),
                // as of now we don't support precision more than 3 digits
                is(LocalTime.of(20, 15, 23, 123_000_000))
        );
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
    }

    @Test
    @TestFor(ColumnType.DATETIME)
    void datetimeTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.datetime(0);

        assertThat(
                factory.scalar("timestamp '2020-01-01 10:00:00'", resultType).get(context),
                is(LocalDateTime.of(2020, 1, 1, 10, 0, 0))
        );
        assertThat(
                factory.scalar("timestamp '2023-06-15 12:00:00'", resultType).get(context),
                is(LocalDateTime.of(2023, 6, 15, 12, 0, 0))
        );
        assertThat(
                factory.scalar("timestamp '2023-06-15 20:15:23.123'", resultType).get(context),
                is(LocalDateTime.of(2023, 6, 15, 20, 15, 23))
        );
        assertThat(
                factory.scalar("timestamp '2023-06-15 20:15:23.1234'", NativeTypes.datetime(1)).get(context),
                is(LocalDateTime.of(2023, 6, 15, 20, 15, 23, 100_000_000))
        );
        assertThat(
                factory.scalar("timestamp '2023-06-15 20:15:23.1234'", NativeTypes.datetime(2)).get(context),
                is(LocalDateTime.of(2023, 6, 15, 20, 15, 23, 120_000_000))
        );
        assertThat(
                factory.scalar("timestamp '2023-06-15 20:15:23.1234'", NativeTypes.datetime(3)).get(context),
                is(LocalDateTime.of(2023, 6, 15, 20, 15, 23, 123_000_000))
        );
        assertThat(
                factory.scalar("timestamp '2023-06-15 20:15:23.1234'", NativeTypes.datetime(4)).get(context),
                // as of now we don't support precision more than 3 digits
                is(LocalDateTime.of(2023, 6, 15, 20, 15, 23, 123_000_000))
        );
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
    }

    @Test
    @TestFor(ColumnType.TIMESTAMP)
    void timestampTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.timestamp(0);

        List<Instant> values = List.of(
                Instant.parse("2020-01-01T10:00:00Z"),
                Instant.parse("2023-06-15T12:00:00Z")
        );

        IgniteScalar scalar = factory.scalar("CURRENT_TIMESTAMP", resultType);
        for (Instant value : values) {
            assertThat(scalar.get(context(value)), is(value));
        }

        assertThat(factory.scalar("null", resultType).get(context), nullValue());
    }

    @Test
    @TestFor(ColumnType.UUID)
    void uuidTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.UUID;

        assertThat(
                factory.scalar("uuid '12345678-1234-1234-1234-123456789012'", resultType).get(context),
                is(UUID.fromString("12345678-1234-1234-1234-123456789012"))
        );
        assertThat(
                factory.scalar("uuid 'ffffffff-ffff-ffff-ffff-ffffffffffff'", resultType).get(context),
                is(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"))
        );
        assertThat(
                factory.scalar("rand_uuid()", resultType).get(context),
                isA(UUID.class)
        );
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
    }

    @Test
    @TestFor(ColumnType.NULL)
    void nullTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.NULL;

        assertThat(factory.scalar("null", resultType).get(context), nullValue());
    }

    @Test
    @TestFor(ColumnType.STRING)
    void stringTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.stringOf(100);

        assertThat(factory.scalar("'foo'", resultType).get(context), is("foo"));
        assertThat(factory.scalar("'bar'", resultType).get(context), is("bar"));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
    }

    @Test
    @TestFor(ColumnType.BYTE_ARRAY)
    void byteArrayTest() throws Exception {
        EvaluationContext<Object[]> context = context();
        NativeType resultType = NativeTypes.blobOf(100);

        //noinspection NumericCastThatLosesPrecision
        assertThat(
                factory.scalar("x'deadbeaf'", resultType).get(context),
                is(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xAF})
        );
        assertThat(factory.scalar("x'0a141e'", resultType).get(context), is(new byte[] {10, 20, 30}));
        assertThat(factory.scalar("null", resultType).get(context), nullValue());
    }

    @ParameterizedTest
    @CsvSource(delimiterString = ";", value = {
            "val bar baz; Encountered \"bar\" at line 1, column 5",
            "bar#baz; Lexical error at line 1, column 4.  Encountered: \"#\" (35)"
    })
    void parsingErrors(String expression, String expectedMessage) {
        assertThrows(
                ExpressionParsingException.class,
                () -> factory.scalar(expression, NativeTypes.INT32),
                expectedMessage
        );
    }

    @Test
    void validationErrors() {
        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("'42'", NativeTypes.INT32),
                "Expected INTEGER expression but CHAR(2) was provided"
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("42 = '12'", NativeTypes.BOOLEAN),
                "From line 1, column 1 to line 1, column 9: Cannot apply '=' to arguments of type" 
                        + " '<INTEGER> = <CHAR(2)>'. Supported form(s): '<EQUIVALENT_TYPE> = <EQUIVALENT_TYPE>'"
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("EXISTS (SELECT 1 FROM test)", NativeTypes.BOOLEAN),
                "From line 1, column 8 to line 1, column 27: Subqueries are not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("val > 10 AND val = (SELECT id FROM test) AND val < 5", NativeTypes.BOOLEAN),
                "From line 1, column 20 to line 1, column 40: Subqueries are not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("5 < AVG(42)", NativeTypes.BOOLEAN),
                "From line 1, column 5 to line 1, column 11: Aggregates are not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("CURRENT_USER", NativeTypes.stringOf(100)),
                "From line 1, column 1 to line 1, column 12: CURRENT_USER is not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("LOCALTIME", NativeTypes.time(0)),
                "From line 1, column 1 to line 1, column 9: LOCALTIME is not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("LOCALTIMESTAMP", NativeTypes.datetime(0)),
                "From line 1, column 1 to line 1, column 14: LOCALTIMESTAMP is not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("CURRENT_DATE", NativeTypes.DATE),
                "From line 1, column 1 to line 1, column 12: CURRENT_DATE is not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("1 + CAST(? AS BIGINT)", NativeTypes.INT32),
                "From line 1, column 10 to line 1, column 10: Dynamic parameters are not supported in given context."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("'foobarbaz'", NativeTypes.stringOf(3)),
                "Expected VARCHAR(3) expression but CHAR(9) was provided."
        );

        assertThrows(
                ExpressionValidationException.class,
                () -> factory.scalar("row (1)", singleColumnType(NativeTypes.INT32)),
                "Structured types are not supported in given context."
        );
    }

    @Test
    void runtimeErrors() {
        EvaluationContext<Object[]> context = context();

        assertThrows(
                ExpressionEvaluationException.class,
                () -> factory.scalar("42 / 0 > 10", NativeTypes.BOOLEAN).get(context),
                "Division by zero"
        );

        assertThrows(
                ExpressionEvaluationException.class,
                () -> factory.scalar("CAST('abaracadabara' AS BOOLEAN)", NativeTypes.BOOLEAN).get(context),
                "Invalid character for cast: abaracadabara"
        );

        assertThrows(
                ExpressionEvaluationException.class,
                () -> factory.scalar("512", NativeTypes.INT8).get(context),
                "TINYINT out of range"
        );
    }
}
