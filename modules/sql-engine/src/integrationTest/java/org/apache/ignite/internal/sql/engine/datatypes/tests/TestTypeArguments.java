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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.Nullable;

/**
 * Provides method for generating arguments for {@link BaseDataTypeTest data type tests}.
 *
 * @param <T> A storage type of a data type.
 */
public final class TestTypeArguments<T extends Comparable<T>> {

    private final List<Argument<T>> args;

    private final String label;

    @SafeVarargs
    private TestTypeArguments(Argument<T>... args) {
        this(List.of(args), null);
    }

    private TestTypeArguments(List<Argument<T>> args, @Nullable String label) {
        this.args = args;

        if (label == null) {
            StringBuilder sb = new StringBuilder();

            for (var arg : args) {
                if (sb.length() > 0) {
                    sb.append(" ");
                }
                sb.append(arg.typeName);
            }

            sb.append(":");

            for (var arg : args) {
                sb.append(" ");
                sb.append(arg.value);
            }

            this.label = sb.toString();
        } else {
            this.label = label;
        }
    }


    /**
     * Returns an original value of the {@code i-th} argument.
     */
    public T value(int i) {
        return getArg(i).value();
    }

    /**
     * Returns a SQL type name of the {@code i-th} argument.
     */
    public String typeName(int i) {
        Argument<T> arg = getArg(i);
        return arg.typeName();
    }

    /**
     * Returns a java value of the {@code i-th} argument.
     */
    public Object argValue(int i) {
        Argument<T> arg = getArg(i);
        return arg.argValue();
    }

    /**
     * An SQL literal expression of the {@code i-th} argument.
     *
     * @throws UnsupportedOperationException if there is no literal for the {@code i-th} argument.
     */
    public String argLiteral(int i) {
        Argument<T> arg = getArg(i);
        return arg.argLiteral();
    }

    /**
     * Returns an SQL expression that produces a value of i-th argument.
     *
     * <p>Default implementation returns: {@code CAST('value' AS <type>)}.
     */
    public String valueExpr(int i) {
        Argument<T> arg = getArg(i);
        return arg.valueExpr();
    }

    /**
     * Returns string representation of the i-th argument.
     */
    public String stringValue(int i) {
        Argument<T> arg = getArg(i);
        return arg.stringValue();
    }

    /**
     * Creates a copy of these arguments with the given label.
     *
     * @param label A value used by {@link #toString()} method.
     * @return a copy of these arguments with a new label.
     */
    public TestTypeArguments<T> withLabel(String label) {
        return new TestTypeArguments<>(args, label);
    }

    /** {@inheritDoc} **/
    @Override
    public String toString() {
        return label;
    }

    private Argument<T> getArg(int index) {
        return args.get(index);
    }


    /**
     * Creates a stream of arguments for an unary operator.
     *
     * <p>For a sample value {@code V} of type {@code T} and its representations {@code (R1, R_type1)} and {@code (R2, R_type2)}.
     *
     * <p>This method produces the following results:
     * <pre>
     *     value | typeName | argument |
     *     ------+----------+----------+
     *     V     | T        | V        |
     *     V     | R_type1  | R1       |
     *     V     | R_type2  | R2       |
     * </pre>
     */
    public static <T extends Comparable<T>> Stream<TestTypeArguments<T>> unary(DataTypeTestSpec<T> testTypeSpec,
            TestDataSamples<T> samples, T value) {

        List<TestTypeArguments<T>> result = new ArrayList<>();
        Argument<T> argument = createArgument(testTypeSpec, testTypeSpec.typeName(), value, value);
        result.add(new TestTypeArguments<>(argument));

        for (var entry : samples.get(value).entrySet()) {
            String typeName = entry.getKey().getName();
            Argument<T> anotherArgument = createArgument(testTypeSpec, typeName, value, entry.getValue());

            result.add(new TestTypeArguments<>(anotherArgument));
        }

        return result.stream();
    }

    /**
     * Creates a stream of arguments for a binary operator.
     *
     * <p>For sample values {@code V1}, and {@code V2} of type {@code T} and their representations:
     * <ul>
     *     <li>{@code V1}: {@code (R1_1, R_type1)} and {@code (R1_2, R_type2)}</li>
     *     <li>{@code V2}: {@code (R2_1, R_type1)} and {@code (R2_2, R_type2)}</li>
     * </ul>
     *
     * <p>This method produces the following results:
     * <pre>
     *     value(0) | typeName(0) | argument(0) |
     *     ---------+-------------+-------------+
     *     V1       | T           | V1          |
     *     V1       | R_type1     | R1_1        |
     *     V1       | R_type2     | R1_2        |
     *     ---------+-------------+-------------+
     *     value(1) | typeName(1) | argument(1) |
     *     ---------+-------------+-------------+
     *     V2       | T           | V2          |
     *     V2       | R_type1     | R2_1        |
     *     V2       | R_type2     | R2_2        |
     * </pre>
     */
    public static <T extends Comparable<T>> Stream<TestTypeArguments<T>> binary(
            DataTypeTestSpec<T> typeSpec, TestDataSamples<T> samples, T lhs, T rhs) {

        Map<SqlTypeName, Object> value1s = samples.get(lhs);
        Map<SqlTypeName, Object> value2s = samples.get(rhs);

        Argument<T> lhsArg = createArgument(typeSpec, typeSpec.typeName(), lhs, lhs);
        Argument<T> rhsArg = createArgument(typeSpec, typeSpec.typeName(), rhs, rhs);

        List<TestTypeArguments<T>> result = new ArrayList<>();
        result.add(new TestTypeArguments<>(lhsArg, rhsArg));

        for (var typeValue : value2s.entrySet()) {
            Argument<T> rhs1 = createArgument(typeSpec, typeValue.getKey().toString(), rhs, typeValue.getValue());

            result.add(new TestTypeArguments<>(lhsArg, rhs1));
        }

        for (var typeValue : value1s.entrySet()) {
            Argument<T> lhs1 = createArgument(typeSpec, typeValue.getKey().toString(), lhs, typeValue.getValue());

            result.add(new TestTypeArguments<>(lhs1, rhsArg));
        }

        return result.stream();
    }

    /**
     * Creates a stream of arguments for a n-ary operator or an operator that have more than one argument for its right-hand side, eg.
     * {@code IN}.
     *
     * <p>For sample values {@code V1}, {@code V2}, and {@code V3} of type {@code T} and their representations:
     * <ul>
     *     <li>{@code V1}: {@code (R1_1, R_type1)} and {@code (R1_2, R_type2)}</li>
     *     <li>{@code V2}: {@code (R2_1, R_type1)} and {@code (R2_2, R_type2)}</li>
     *     <li>{@code V3}: {@code (R3_1, R_type1)} and {@code (R3_2, R_type2)}</li>
     * </ul>
     *
     * <p>This method produces the following results:
     * <pre>
     *     value(0) | typeName(0) | argument(0) |
     *     ---------+-------------+-------------+
     *     V1       | T           | V1          |
     *     V1       | R_type1     | R1_1        |
     *     V1       | R_type2     | R1_2        |
     *     ---------+-------------+-------------+
     *     value(1) | typeName(1) | argument(1) |
     *     ---------+-------------+-------------+
     *     V2       | T           | V2          |
     *     V2       | R_type1     | R2_1        |
     *     V2       | R_type2     | R2_2        |
     *     ---------+-------------+-------------+
     *     value(2) | typeName(2) | argument(1) |
     *     ---------+-------------+-------------+
     *     V3       | T           | V3          |
     *     V3       | R_type1     | R3_1        |
     *     V3       | R_type2     | R3_2        |
     * </pre>
     */
    @SafeVarargs
    public static <T extends Comparable<T>> Stream<TestTypeArguments<T>> nary(DataTypeTestSpec<T> typeSpec,
            TestDataSamples<T> samples, T lhs, T... rhs) {

        if (rhs.length < 1) {
            throw new IllegalArgumentException("Right-hand side must have at least one argument");
        }

        List<TestTypeArguments<T>> result = new ArrayList<>(rhs.length + 1);

        Argument<T>[] first = (Argument<T>[]) Array.newInstance(Argument.class, rhs.length + 1);
        first[0] = createArgument(typeSpec, typeSpec.typeName(), lhs, lhs);

        for (int i = 1; i <= rhs.length; i++) {
            T rhsValue = rhs[i - 1];
            first[i] = createArgument(typeSpec, typeSpec.typeName(), rhsValue, rhsValue);
        }
        result.add(new TestTypeArguments<>(first));

        for (var entry : samples.get(first[0].value()).entrySet()) {
            Argument[] args = new Argument[rhs.length + 1];
            args[0] = first[0];

            for (int i = 1; i <= rhs.length; i++) {
                T val = rhs[i - 1];
                Object otherValue = samples.get(val, entry.getKey());
                args[i] = createArgument(typeSpec, entry.getKey().getName(), val, otherValue);
            }

            result.add(new TestTypeArguments<T>(args));
        }

        return result.stream();
    }

    private static final class Argument<T extends Comparable<T>> implements Comparable<Argument<T>> {

        private final String typeName;

        private final T value;

        private final Object argument;

        private final String valueExpr;

        private final String stringValue;

        private final String literal;

        Argument(String typeName, T value, Object argument, String valueExpr, String stringValue, @Nullable String literal) {
            this.typeName = typeName;
            this.value = value;
            this.argument = argument;
            this.valueExpr = valueExpr;
            this.stringValue = stringValue;
            this.literal = literal;
        }

        /** Original value. **/
        T value() {
            return value;
        }

        /** SQL type name of this argument. **/
        String typeName() {
            return typeName;
        }

        /** Java value of an argument. */
        Object argValue() {
            return argument;
        }

        /** Returns an SQL expression that produces a value of this argument. **/
        String valueExpr() {
            return valueExpr;
        }

        /** Returns a string representation of this argument. **/
        String stringValue() {
            return stringValue;
        }

        /**
         * Returns this argument as SQL literal.
         *
         * @throws UnsupportedOperationException if there is no literal for this type.
         */
        public String argLiteral() {
            if (literal == null) {
                throw new UnsupportedOperationException("There is no literal expression for type {}. Use testType.hasLiteral()");
            }
            return literal;
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(Argument<T> o) {
            return value.compareTo(o.value);
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Argument<?> argument = (Argument<?>) o;
            return value.equals(argument.value);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return typeName + " " + value;
        }
    }


    private static <T extends Comparable<T>> Argument<T> createArgument(DataTypeTestSpec<T> testTypeSpec,
            String typeName, T value, Object argument) {

        String valueExpr;
        String stringValue;

        if (!typeName.equals(testTypeSpec.typeName())) {
            SqlLiteral sqlLiteral;

            if (argument instanceof Number) {
                if (argument instanceof Long
                        || argument instanceof Integer
                        || argument instanceof Short
                        || argument instanceof Byte) {

                    sqlLiteral = SqlLiteral.createExactNumeric(argument.toString(), SqlParserPos.ZERO);
                } else {
                    sqlLiteral = SqlLiteral.createApproxNumeric(argument.toString(), SqlParserPos.ZERO);
                }
            } else if (argument instanceof String) {
                sqlLiteral = SqlLiteral.createCharString(argument.toString(), "UTF-8", SqlParserPos.ZERO);
            } else if (argument instanceof byte[]) {
                sqlLiteral = SqlLiteral.createBinaryString((byte[]) argument, SqlParserPos.ZERO);
            } else if (argument instanceof Boolean) {
                sqlLiteral = SqlLiteral.createBoolean((Boolean) argument, SqlParserPos.ZERO);
            } else {
                throw new IllegalArgumentException("Unsupported literal: " + value);
            }

            valueExpr = sqlLiteral.toString();
            stringValue = argument.toString();
        } else {
            valueExpr = testTypeSpec.toValueExpr(value);
            stringValue = testTypeSpec.toStringValue(value);
        }

        String literal = testTypeSpec.hasLiterals() ? testTypeSpec.toLiteral(value) : null;

        return new Argument<>(typeName, value, argument, valueExpr, stringValue, literal);
    }
}
