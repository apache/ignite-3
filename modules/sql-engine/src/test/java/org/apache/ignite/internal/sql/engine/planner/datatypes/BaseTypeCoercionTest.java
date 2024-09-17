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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.EnumSet;
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
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.sql.ColumnMetadata;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.provider.Arguments;

public class BaseTypeCoercionTest extends AbstractPlannerTest {

    static Stream<Arguments> allNumericPairs() {
        return Arrays.stream(NumericPair.values()).map(Arguments::of);
    }

    static void checkIncludesAllNumericTypePairs(Stream<Arguments> args) {
        EnumSet<NumericPair> remainingPairs = EnumSet.allOf(NumericPair.class);

        List<NumericPair> usedPairs = args.map(Arguments::get)
                .map(arg -> (NumericPair) arg[0])
                .collect(Collectors.toList());

        usedPairs.forEach(remainingPairs::remove);

        for (NumericPair numericPair : NumericPair.values()) {
            usedPairs.remove(numericPair);
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

    /**
     * Creates a matcher to verify that given expression has expected return type, but it is not CAST operator.
     *
     * @param type Expected return type.
     * @return A matcher.
     */
    static Matcher<RexNode> ofTypeWithoutCast(NativeType type) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        RelDataType sqlType = TypeUtils.native2relationalType(typeFactory, type);

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
        RelDataType sqlType = TypeUtils.native2relationalType(typeFactory, type);

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

    private static Matcher<Object> ofType(NativeType type) {
        return new BaseMatcher<>() {
            Object actual;
            NativeType referenceType;
            int precision = 0;
            int scale = 0;
            ColumnMetadata colMeta;

            @Override
            public boolean matches(Object actual) {
                assert actual != null;
                Pair<Object, ColumnMetadata> pair = (Pair<Object, ColumnMetadata>) actual;
                this.actual = pair.getFirst();
                colMeta = pair.getSecond();

                NativeTypeSpec nativeTypeSpec = NativeTypeSpec.fromClass(this.actual.getClass());

                boolean checkPrecisionScale = false;

                int typePrecision = 0;
                int typeScale = 0;

                if (this.actual instanceof BigDecimal) {
                    assert type instanceof DecimalNativeType;
                    typePrecision = ((DecimalNativeType) type).precision();
                    typeScale = ((DecimalNativeType) type).scale();

                    precision = ((BigDecimal) this.actual).precision();
                    scale = ((BigDecimal) this.actual).scale();
                    checkPrecisionScale = true;
                }

                referenceType = type;

                boolean precCheck = checkPrecisionScale ? colMeta.precision() >= precision && colMeta.scale() >= scale : true;

                return precCheck && nativeTypeSpec == type.spec()
                        && typePrecision == precision
                        && typeScale == scale;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Expected : '{}' but found '{}, precision: {}, scale: {}', column meta: {}",
                        referenceType, actual.getClass(), precision, scale, colMeta));
            }
        };
    }

    public static TestCaseBuilder forTypePair(TypePair typePair) {
        return new TestCaseBuilder(typePair);
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

        public Arguments resultWillBe(NativeType type) {
            return Arguments.of(pair, ofType(type));
        }

        Arguments secondOpMatches(Matcher<?> operandMatcher) {
            return Arguments.of(pair, firstOpMatcher, operandMatcher);
        }

        Arguments secondOpBeSame() {
            return Arguments.of(pair, firstOpMatcher, ofTypeWithoutCast(pair.second()));
        }
    }

    public static SingleArgFunctionReturnTypeCheckBuilder forArgumentOfType(NativeType type) {
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
        return SqlTestUtils.makeLiteral(val.toString(), type.spec().asColumnType());
    }
}
