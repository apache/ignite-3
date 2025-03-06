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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.provider.Arguments;

/** Base class for testing types coercion. */
public class BaseTypeCoercionTest extends AbstractPlannerTest {
    /**
     * Ensures that object mapping doesn't miss any type pair from {@link NumericPair}.
     */
    static void checkIncludesAllNumericTypePairs(Stream<Arguments> args) {
        checkIncludesAllTypePairs(args, NumericPair.class);
    }

    static void checkIncludesAllTypePairs(Stream<Arguments> args, Class<? extends TypePair> clazz) {
        TypePair[] enums = clazz.getEnumConstants();

        List<TypePair> remainingPairs = new ArrayList<>(Arrays.asList(enums));

        List<TypePair> usedPairs = args.map(Arguments::get)
                .map(arg -> (clazz.cast(arg[0])))
                .collect(Collectors.toList());

        usedPairs.forEach(remainingPairs::remove);

        for (Object numericPair : enums) {
            usedPairs.remove(clazz.cast(numericPair));
        }

        assertThat("There are missing pairs", remainingPairs, Matchers.empty());
        assertThat("There are duplicate pairs. Remove them", usedPairs, Matchers.empty());
    }

    static IgniteSchema createSchemaWithSingleColumnTable(NativeType c1) {
        return createSchema(
                TestBuilders.table()
                        .name("T")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", c1)
                        .build()
        );
    }

    static IgniteSchema createSchemaWithTwoColumnTable(NativeType c1, NativeType c2) {
        return createSchema(
                TestBuilders.table()
                        .name("T")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", c1)
                        .addColumn("C2", c2)
                        .build()
        );
    }

    static IgniteSchema createSchemaWithTwoSingleColumnTable(NativeType c1, NativeType c2) {
        return createSchema(
                TestBuilders.table()
                        .name("T1")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", c1)
                        .build(),
                TestBuilders.table()
                        .name("T2")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C2", c2)
                        .build()
        );
    }

    static Matcher<IgniteRel> operandMatcher(Matcher<RexNode> first, Matcher<RexNode> second) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                RexNode comparison = ((ProjectableFilterableTableScan) actual).projects().get(0);

                assertThat(comparison, instanceOf(RexCall.class));

                RexCall comparisonCall = (RexCall) comparison;

                RexNode leftOperand = comparisonCall.getOperands().get(0);
                RexNode rightOperand = comparisonCall.getOperands().get(1);

                assertThat(leftOperand, first);
                assertThat(rightOperand, second);

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    static Matcher<IgniteRel> operandsWithResultMatcher(Matcher<RexNode> first, Matcher<RexNode> second, Matcher<RexCall> result) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                RexNode comparison = ((ProjectableFilterableTableScan) actual).projects().get(0);

                assertThat(comparison, instanceOf(RexCall.class));

                RexCall comparisonCall = (RexCall) comparison;

                RexNode leftOperand = comparisonCall.getOperands().get(0);
                RexNode rightOperand = comparisonCall.getOperands().get(1);

                assertThat(leftOperand, first);
                assertThat(rightOperand, second);
                assertThat(comparisonCall, result);

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    /**
     * Creates a matcher to verify that given expression has expected return type, but it is not CAST operator.
     *
     * @param type Expected return type.
     * @return A matcher.
     */
    static Matcher<RexNode> ofTypeWithoutCast(NativeType type) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        RelDataType sqlType = native2relationalType(typeFactory, type);

        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                return SqlTypeUtil.equalSansNullability(typeFactory, ((RexNode) actual).getType(), sqlType)
                        && !((RexNode) actual).isA(SqlKind.CAST);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("was ").appendValue(item).appendText(" of type " + ((RexNode) item).getType());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Operand of type {} that is not CAST", sqlType));
            }
        };
    }

    /**
     * Creates a matcher to verify that given expression has expected return type, but it is not CAST operator.
     *
     * @param sqlType Expected return type.
     * @return A matcher.
     */
    static Matcher<RexNode> ofTypeWithoutCast(RelDataType sqlType) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                return SqlTypeUtil.equalSansNullability(typeFactory, ((RexNode) actual).getType(), sqlType)
                        && !((RexNode) actual).isA(SqlKind.CAST);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("was ").appendValue(item).appendText(" of type " + ((RexNode) item).getType());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Operand of type {} that is not CAST", sqlType));
            }
        };
    }

    /**
     * Creates a matcher to verify that given expression is CAST operator with expected return type.
     *
     * @param type Expected return type.
     * @return A matcher.
     */
    static Matcher<RexNode> castTo(NativeType type) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        RelDataType sqlType = native2relationalType(typeFactory, type);

        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof RexCall
                        && ((RexNode) actual).isA(SqlKind.CAST)
                        && SqlTypeUtil.equalSansNullability(typeFactory, ((RexNode) actual).getType(), sqlType);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Operand that is CAST(..):" + sqlType);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("was ").appendValue(item).appendText(" of type " + ((RexNode) item).getType());
            }
        };
    }

    static Matcher<RexCall> ofType(NativeType type) {
        return new BaseMatcher<>() {
            RelDataType expectedType;
            RelDataType relRowType;

            @Override
            public boolean matches(Object item) {
                assert item != null;

                RexNode rexCall = (RexNode) item;

                relRowType = rexCall.getType();
                expectedType = native2relationalType(Commons.typeFactory(), type);

                return SqlTypeUtil.equalSansNullability(relRowType, expectedType);
            }

            @Override
            public void describeTo(Description description) {
                if (expectedType != null && relRowType != null) {
                    description.appendText("Expected type: " + expectedType + ", but found: " + relRowType);
                }
            }
        };
    }

    static TestCaseBuilder forTypePair(TypePair typePair) {
        return new TestCaseBuilder(typePair);
    }

    static TestCaseBuilderEx forTypePairEx(TypePair typePair) {
        return new TestCaseBuilderEx(typePair);
    }

    /**
     * Not really a builder, but provides DSL-like API to describe test case.
     */
    static class TestCaseBuilderEx {
        private final TypePair pair;
        private Matcher<?> firstOpMatcher;
        private Matcher<?> secondOpMatcher;

        private TestCaseBuilderEx(TypePair pair) {
            this.pair = pair;
        }

        TestCaseBuilderEx firstOpMatches(Matcher<?> operandMatcher) {
            firstOpMatcher = operandMatcher;

            return this;
        }

        TestCaseBuilderEx firstOpBeSame() {
            firstOpMatcher = ofTypeWithoutCast(pair.first());

            return this;
        }

        TestCaseBuilderEx secondOpMatches(Matcher<?> operandMatcher) {
            secondOpMatcher = operandMatcher;
            return this;
        }

        TestCaseBuilderEx secondOpBeSame() {
            secondOpMatcher = ofTypeWithoutCast(pair.second());
            return this;
        }

        Arguments resultWillBe(NativeType type) {
            return Arguments.of(pair, firstOpMatcher, secondOpMatcher, ofType(type));
        }

        Arguments checkResult(NativeType type) {
            return Arguments.of(pair, ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()), ofType(type));
        }
    }

    /**
     * Not really a builder, but provides DSL-like API to describe test case.
     */
    public static class TestCaseBuilder {
        private final TypePair pair;
        private Matcher<?> firstOpMatcher;

        private TestCaseBuilder(TypePair pair) {
            this.pair = pair;
        }

        Arguments opMatches(Matcher<?> operandMatcher) {
            firstOpMatcher = operandMatcher;

            return Arguments.of(pair, firstOpMatcher);
        }

        TestCaseBuilder firstOpMatches(Matcher<?> operandMatcher) {
            firstOpMatcher = operandMatcher;

            return this;
        }

        TestCaseBuilder firstOpBeSame() {
            firstOpMatcher = ofTypeWithoutCast(pair.first());

            return this;
        }

        Arguments secondOpMatches(Matcher<?> operandMatcher) {
            return Arguments.of(pair, firstOpMatcher, operandMatcher);
        }

        Arguments secondOpBeSame() {
            return Arguments.of(pair, firstOpMatcher, ofTypeWithoutCast(pair.second()));
        }
    }

    static SingleArgFunctionReturnTypeCheckBuilder forArgumentOfType(NativeType type) {
        return new SingleArgFunctionReturnTypeCheckBuilder(type);
    }

    /**
     * Not really a builder, but provides DSL-like API to describe test case.
     */
    static class SingleArgFunctionReturnTypeCheckBuilder {
        private final NativeType argumentType;

        private SingleArgFunctionReturnTypeCheckBuilder(NativeType type) {
            this.argumentType = type;
        }

        Arguments resultWillBe(NativeType returnType) {
            return Arguments.of(argumentType, returnType);
        }
    }

    /**
     * Generates SQL literal with a random value for given type take into account precision and scale for the passed param.
     *
     * @param type Type to generate literal value.
     * @param closerToBoundForDecimal5 if {@code true} will generate random value in the upper half of positive values for DECIMAL
     *         type with 5  digits in integer part of the number.
     * @return Generated value as string representation of a SQL literal.
     */
    static String generateLiteral(NativeType type, boolean closerToBoundForDecimal5) {
        Object val = SqlTestUtils.generateValueByType(type);
        // We have different behaviour of planner depending on value it can put CAST or not to do it.
        // So we will generate all values which more then Short.MAX_VALUE and CAST will be always putted.
        if (closerToBoundForDecimal5 && type.spec() == NativeTypeSpec.DECIMAL) {
            DecimalNativeType t = ((DecimalNativeType) type);
            // for five-digit we can have value less or more then Short.MaX_VALUE.
            // To get rid of vagueness let's generate always bigger value.
            if (t.precision() - t.scale() == 5) {
                BigDecimal bd = ((BigDecimal) val);
                if (bd.signum() > 0 && bd.intValue() < Short.MAX_VALUE) {
                    val = bd.add(BigDecimal.valueOf(Short.MAX_VALUE));
                } else if (bd.signum() < 0 && bd.intValue() > -Short.MAX_VALUE) {
                    val = bd.subtract(BigDecimal.valueOf(Short.MAX_VALUE));
                }
            }
        }
        return SqlTestUtils.makeLiteral(val, type.spec().asColumnType());
    }

    private static String prevResult = "";

    // It is necessary to have a guarantee that two values in a row will be different.
    // Otherwise, a SQL optimizator can fold complex statement with two values into simple literal.
    static String generateLiteralWithNoRepetition(NativeType type) {
        Object val;
        String valString;
        do {
            val = SqlTestUtils.generateValueByType(type);
            valString = val == null ? "<null>" : val.toString();
        } while (valString.equals(prevResult));
        prevResult = valString;

        return SqlTestUtils.makeLiteral(val, type.spec().asColumnType());
    }
}