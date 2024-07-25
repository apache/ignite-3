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
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.provider.Arguments;

abstract class BaseTypeCoercionTest extends AbstractPlannerTest {

    static Stream<Arguments> allNumericPairs() {
        return Arrays.stream(NumericPair.values()).map(Arguments::of);
    }

    static void checkIncludesAllNumericTypePairs(Stream<Arguments> args) {
        EnumSet<NumericPair> remainingPairs = EnumSet.allOf(NumericPair.class);

        List<NumericPair> allPairs = args.map(Arguments::get)
                .map(arg -> (NumericPair) arg[0])
                .collect(Collectors.toList());

        allPairs.forEach(remainingPairs::remove);

        EnumSet.allOf(NumericPair.class).forEach(allPairs::remove);

        assertThat("missing pairs", remainingPairs, Matchers.empty());
        assertThat("duplicate pairs", allPairs, Matchers.empty());
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

    static TestCaseBuilder forTypePair(TypePair typePair) {
        return new TestCaseBuilder(typePair);
    }

    /**
     * Not really a builder, but provides DSL-like API to describe test case.
     */
    static class TestCaseBuilder {
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
}
