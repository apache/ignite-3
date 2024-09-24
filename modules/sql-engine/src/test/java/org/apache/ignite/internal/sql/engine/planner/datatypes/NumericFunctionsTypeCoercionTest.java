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

package org.apache.ignite.internal.sql.engine.planner.datatypes;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for numeric functions, when operand belongs to the NUMERIC type family.
 */
public class NumericFunctionsTypeCoercionTest extends BaseTypeCoercionTest {

    private static final List<NativeType> INT_TYPES = List.of(NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT64);

    @ParameterizedTest
    @MethodSource("modArgs")
    public void mod(
            TypePair typePair,
            Matcher<RexNode> arg1, Matcher<RexNode> arg2,
            NativeType returnType
    ) throws Exception {
        NativeType type1 = typePair.first();
        NativeType type2 = typePair.second();

        IgniteSchema schema = createSchemaWithTwoColumnTable(type1, type2);

        List<Matcher<RexNode>> args = List.of(arg1, arg2);
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT MOD(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> modArgs() {
        Stream<Arguments> intTypes =
                Arrays.stream(NumericPair.values())
                        .filter(p -> INT_TYPES.contains(p.first()))
                        .filter(p -> INT_TYPES.contains(p.second()))
                        .map(p -> Arguments.of(
                                Types.typePair(p.first(), p.second()),
                                ofTypeWithoutCast(p.first()), ofTypeWithoutCast(p.second()),
                                p.second())
                        );

        Stream<Arguments> approxNumInt = Stream.of(
                Arguments.of(
                        Types.typePair(NativeTypes.FLOAT, NativeTypes.INT8),
                        castTo(Types.DECIMAL_14_7), ofTypeWithoutCast(NativeTypes.INT8),
                        Types.DECIMAL_10_7),

                Arguments.of(
                        Types.typePair(NativeTypes.FLOAT, NativeTypes.INT16),
                        castTo(Types.DECIMAL_14_7), ofTypeWithoutCast(NativeTypes.INT16),
                        Types.DECIMAL_12_7),

                Arguments.of(
                        Types.typePair(NativeTypes.FLOAT, NativeTypes.INT32),
                        castTo(Types.DECIMAL_14_7), ofTypeWithoutCast(NativeTypes.INT32),
                        Types.DECIMAL_14_7),

                Arguments.of(
                        Types.typePair(NativeTypes.FLOAT, NativeTypes.INT64),
                        castTo(Types.DECIMAL_14_7), ofTypeWithoutCast(NativeTypes.INT64),
                        Types.DECIMAL_14_7),

                Arguments.of(
                        Types.typePair(NativeTypes.DOUBLE, NativeTypes.INT8),
                        castTo(Types.DECIMAL_30_15), ofTypeWithoutCast(NativeTypes.INT8),
                        Types.DECIMAL_18_15),

                Arguments.of(
                        Types.typePair(NativeTypes.DOUBLE, NativeTypes.INT16),
                        castTo(Types.DECIMAL_30_15), ofTypeWithoutCast(NativeTypes.INT16),
                        Types.DECIMAL_20_15),

                Arguments.of(
                        Types.typePair(NativeTypes.DOUBLE, NativeTypes.INT32),
                        castTo(Types.DECIMAL_30_15), ofTypeWithoutCast(NativeTypes.INT32),
                        Types.DECIMAL_25_15),

                Arguments.of(
                        Types.typePair(NativeTypes.DOUBLE, NativeTypes.INT64),
                        castTo(Types.DECIMAL_30_15), ofTypeWithoutCast(NativeTypes.INT64),
                        Types.DECIMAL_30_15)
        );

        Stream<Arguments> intApproxTypes = Stream.of(
                Arguments.of(
                        Types.typePair(NativeTypes.INT8, NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.INT8), castTo(Types.DECIMAL_14_7),
                        Types.DECIMAL_10_7),

                Arguments.of(
                        Types.typePair(NativeTypes.INT16, NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.INT16), castTo(Types.DECIMAL_14_7),
                        Types.DECIMAL_12_7),

                Arguments.of(
                        Types.typePair(NativeTypes.INT32, NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.INT32), castTo(Types.DECIMAL_14_7),
                        Types.DECIMAL_14_7),

                Arguments.of(
                        Types.typePair(NativeTypes.INT64, NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.INT64), castTo(Types.DECIMAL_14_7),
                        Types.DECIMAL_14_7),

                Arguments.of(
                        Types.typePair(NativeTypes.INT8, NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.INT8), castTo(Types.DECIMAL_30_15),
                        Types.DECIMAL_18_15),

                Arguments.of(
                        Types.typePair(NativeTypes.INT16, NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.INT16), castTo(Types.DECIMAL_14_7),
                        Types.DECIMAL_12_7),

                Arguments.of(
                        Types.typePair(NativeTypes.INT32, NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.INT32), castTo(Types.DECIMAL_30_15),
                        Types.DECIMAL_25_15),

                Arguments.of(
                        Types.typePair(NativeTypes.INT64, NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.INT64), castTo(Types.DECIMAL_30_15),
                        Types.DECIMAL_30_15)
        );

        return Stream.concat(Stream.concat(intTypes, approxNumInt), intApproxTypes);
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void exp(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        Matcher<RelNode> matcher = new FunctionCallMatcher(List.of(ofTypeWithoutCast(type))).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT EXP(C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> numeric() {
        List<NativeType> numeric = new ArrayList<>();

        for (NumericPair pair : NumericPair.values()) {
            if (!numeric.contains(pair.first())) {
                numeric.add(pair.first());
            }
        }

        return numeric.stream().map(t -> Named.named(t.displayName(), t)).map(Arguments::of);
    }

    @ParameterizedTest
    @EnumSource(NumericPair.class)
    public void power(TypePair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT POWER(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void ln(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT LN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void log10(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT LOG10(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void abs(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(type);

        assertPlan("SELECT ABS(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void rand(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT RAND(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void randInteger1(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.INT32);

        assertPlan("SELECT RAND_INTEGER(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @EnumSource(NumericPair.class)
    public void randInteger2(TypePair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.INT32);

        assertPlan("SELECT RAND_INTEGER(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void acos(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT ACOS(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void asin(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT ASIN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void atan(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT ATAN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @EnumSource(NumericPair.class)
    public void atan2(TypePair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT ATAN2(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void sqrt(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        // SQRT is rewritten to POWER($t0, 0.5:DECIMAL(2, 1))
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type), ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT SQRT(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void cbrt(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT CBRT(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void cos(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT COS(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void cosh(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT COSH(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void cot(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT COT(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void degrees(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT DEGREES(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void radians(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT RADIANS(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void round(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        NativeType returnType;
        if (type instanceof DecimalNativeType) {
            returnType = decimalWithZeroScale((DecimalNativeType) type);
        } else {
            returnType = type;
        }
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT ROUND(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("round2Args")
    public void round2(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(pair.first());

        assertPlan("SELECT ROUND(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<NumericPair> round2Args() {
        List<NativeType> ints = List.of(NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT64);

        return Arrays.stream(NumericPair.values()).filter(p -> ints.contains(p.second()));
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void sign(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(type);

        assertPlan("SELECT SIGN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void sin(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT SIN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void sinh(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT SINH(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void tan(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT TAN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void tanh(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT TANH(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void truncate(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        NativeType returnType;
        if (type instanceof DecimalNativeType) {
            returnType = decimalWithZeroScale((DecimalNativeType) type);
        } else {
            returnType = type;
        }

        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT TRUNCATE(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("truncate2Args")
    public void truncate2(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        NativeType type1 = pair.first();

        NativeType returnType;
        if (type1 instanceof DecimalNativeType) {
            returnType = decimalWithZeroScale((DecimalNativeType) type1);
        } else {
            returnType = type1;
        }

        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT TRUNCATE(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    private static NativeType decimalWithZeroScale(DecimalNativeType decimalType) {
        return NativeTypes.decimalOf(decimalType.precision(), 0);
    }

    private static Stream<NumericPair> truncate2Args() {
        return Arrays.stream(NumericPair.values()).filter(p -> INT_TYPES.contains(p.second()));
    }

    private static class FunctionCallMatcher {

        private final List<Matcher<RexNode>> args;

        // Most of the functions propagate nullability from their arguments,
        // since most of the tests use nullable columns as their arguments,
        // it is better use use the same default.
        private boolean returnTypeNullability = true;

        private FunctionCallMatcher(List<Matcher<RexNode>> args) {
            this.args = args;
        }

        FunctionCallMatcher returnTypeNullability(boolean value) {
            this.returnTypeNullability = value;
            return this;
        }

        Matcher<RelNode> resultWillBe(NativeType returnType) {
            return new TypeSafeDiagnosingMatcher<>() {
                @Override
                protected boolean matchesSafely(RelNode relNode, Description description) {
                    IgniteTableScan tableScan = (IgniteTableScan) relNode;
                    List<RexNode> projects = tableScan.projects();
                    RexCall call = (RexCall) projects.get(0);

                    if (call.getOperands().size() != args.size()) {
                        return false;
                    }
                    assertEquals(args.size(), call.getOperands().size(), "Number of arguments do not match");

                    for (int i = 0; i < args.size(); i++) {
                        Matcher<RexNode> arg = args.get(i);
                        assertThat("Operand#" + i + ". Expected arguments: " + expectedArguments(), call.getOperands().get(i), arg);
                    }

                    RelDataType actualRelType = call.getType();
                    RelDataType expectedRelType = native2relationalType(Commons.typeFactory(), returnType, returnTypeNullability);

                    String message = "Expected return type "
                            + expectedRelType + " but got " + actualRelType
                            + ". Expected arguments: " + expectedArguments();

                    assertEquals(actualRelType, expectedRelType, message);

                    return true;
                }

                @Override
                public void describeTo(Description description) {

                }
            };
        }

        private String expectedArguments() {
            return args.stream().map(Object::toString).collect(Collectors.joining(", "));
        }
    }
}
