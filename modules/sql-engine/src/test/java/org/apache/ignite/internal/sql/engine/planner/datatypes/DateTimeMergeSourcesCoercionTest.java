package org.apache.ignite.internal.sql.engine.planner.datatypes;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DatetimePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for MERGE operations, when values belongs to DATETIME types.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which values.
 */
public class DateTimeMergeSourcesCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @MethodSource("argsForMergeWithColumnAsValue")
    public void mergeWithColumnAsValue(
            TypePair pair,
            Matcher<RexNode> matcher
    ) throws Exception {

        IgniteSchema schema = createSchemaWithTwoSingleColumnTable(pair.first(), pair.second());

        assertPlan("MERGE INTO T1 dst USING T2 src ON dst.c1 = src.c2 WHEN MATCHED THEN UPDATE SET c1 = c2", schema,
                mergeOperandMatcher(matcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("argsForMergeWithLiteralValue")
    public void mergeWithLiteralValue(
            TypePair pair,
            Matcher<RexNode> matcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoSingleColumnTable(pair.first(), pair.second());

        String val = timestampLiteral(pair.second());
        assertPlan("MERGE INTO T1 dst USING T2 src ON dst.c1 = src.c2 WHEN MATCHED THEN UPDATE SET c1 = " + val, schema,
                mergeOperandMatcher(matcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("argsDyn")
    public void mergeDynamicParameters(
            TypePair pair,
            Matcher<RexNode> matcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoSingleColumnTable(pair.first(), pair.second());

        Object val = SqlTestUtils.generateValueByType(pair.second());

        assertPlan("MERGE INTO T1 dst USING T2 src ON dst.c1 = src.c2 WHEN MATCHED THEN UPDATE SET c1 = ?", schema,
                mergeOperandMatcher(matcher)::matches, List.of(val));
    }

    /**
     * This test ensures that {@link #argsForMergeWithColumnAsValue()}, {@link #argsForMergeWithLiteralValue()} and {@link #argsDyn()}
     * doesn't miss any type pair from {@link DatetimePair}.
     */
    @Test
    void updateArgsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(argsForMergeWithColumnAsValue(), DatetimePair.class);
        checkIncludesAllTypePairs(argsForMergeWithLiteralValue(), DatetimePair.class);
        checkIncludesAllTypePairs(argsDyn(), DatetimePair.class);
    }

    private static Stream<Arguments> argsForMergeWithColumnAsValue() {
        return Stream.of(
                forTypePair(DatetimePair.DATE_DATE)
                        .opMatches(ofTypeWithoutCast(NativeTypes.DATE)),
                // TIME

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .opMatches(ofTypeWithoutCast(Types.TIME_0)),
                forTypePair(DatetimePair.TIME_0_TIME_1)
                        .opMatches(castTo(Types.TIME_0)),
                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .opMatches(castTo(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_1_TIME_0)
                        .opMatches(castTo(Types.TIME_1)),
                forTypePair(DatetimePair.TIME_1_TIME_1)
                        .opMatches(ofTypeWithoutCast(Types.TIME_1)),
                forTypePair(DatetimePair.TIME_1_TIME_9)
                        .opMatches(castTo(Types.TIME_1)),

                forTypePair(DatetimePair.TIME_9_TIME_0)
                        .opMatches(castTo(Types.TIME_9)),
                forTypePair(DatetimePair.TIME_9_TIME_1)
                        .opMatches(castTo(Types.TIME_9)),
                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .opMatches(ofTypeWithoutCast(Types.TIME_9)),


                // TIMESTAMP 0

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_1)
                        .opMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .opMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1)
                        .opMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_0)),

                // TIMESTAMP 1

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_0)
                        .opMatches(castTo(Types.TIMESTAMP_1)),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_1)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_1)),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_1)),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0)
                        .opMatches(castTo(Types.TIMESTAMP_1)),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1)
                        .opMatches(castTo(Types.TIMESTAMP_1)),
                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_1)),

                // TIMESTAMP 3

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_0)
                        .opMatches(castTo(Types.TIMESTAMP_9)),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_1)
                        .opMatches(castTo(Types.TIMESTAMP_9)),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0)
                        .opMatches(castTo(Types.TIMESTAMP_9)),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1)
                        .opMatches(castTo(Types.TIMESTAMP_9)),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 0

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)),

                // TIMESTAMP LTZ 1

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_1)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_1)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_1)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_1)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_1)),

                // TIMESTAMP LTZ 3

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_9)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_9)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_9)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1)
                        .opMatches(castTo(Types.TIMESTAMP_WLTZ_9)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9))
        );
    }

    private static Stream<Arguments> argsForMergeWithLiteralValue() {
        Map<DatetimePair, Arguments> diff = new EnumMap<>(DatetimePair.class);

        // TIME

        diff.put(DatetimePair.TIME_0_TIME_1, forTypePair(DatetimePair.TIME_0_TIME_1)
                .opMatches(ofTypeWithoutCast(Types.TIME_0)));
        diff.put(DatetimePair.TIME_0_TIME_9, forTypePair(DatetimePair.TIME_0_TIME_9)
                .opMatches(ofTypeWithoutCast(Types.TIME_0)));

        diff.put(DatetimePair.TIME_1_TIME_0, forTypePair(DatetimePair.TIME_1_TIME_0)
                .opMatches(ofTypeWithoutCast(Types.TIME_1)));
        diff.put(DatetimePair.TIME_1_TIME_9, forTypePair(DatetimePair.TIME_1_TIME_9)
                .opMatches(ofTypeWithoutCast(Types.TIME_1)));

        diff.put(DatetimePair.TIME_9_TIME_0, forTypePair(DatetimePair.TIME_9_TIME_0)
                .opMatches(ofTypeWithoutCast(Types.TIME_9)));
        diff.put(DatetimePair.TIME_9_TIME_1, forTypePair(DatetimePair.TIME_9_TIME_1)
                .opMatches(ofTypeWithoutCast(Types.TIME_9)));

        // TIMESTAMP

        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_1, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_1)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_0)));
        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_0)));

        diff.put(DatetimePair.TIMESTAMP_1_TIMESTAMP_0, forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_0)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_1)));
        diff.put(DatetimePair.TIMESTAMP_1_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_9)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_1)));

        diff.put(DatetimePair.TIMESTAMP_9_TIMESTAMP_0, forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_0)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)));
        diff.put(DatetimePair.TIMESTAMP_9_TIMESTAMP_1, forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_1)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)));

        // TIMESTAMP LTZ

        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0, forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0, forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1, forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1)
                .opMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)));

        return argsForMergeWithColumnAsValue().map(v -> diff.getOrDefault(v.get()[0], v));
    }

    private static Stream<Arguments> argsDyn() {
        Map<DatetimePair, Arguments> diff = new EnumMap<>(DatetimePair.class);

        diff.put(DatetimePair.TIME_0_TIME_1, forTypePair(DatetimePair.TIME_0_TIME_1)
                .opMatches(ofTypeWithoutCast(Types.TIME_0)));
        diff.put(DatetimePair.TIME_0_TIME_9, forTypePair(DatetimePair.TIME_0_TIME_9)
                .opMatches(ofTypeWithoutCast(Types.TIME_0)));
        diff.put(DatetimePair.TIME_1_TIME_1, forTypePair(DatetimePair.TIME_1_TIME_1)
                .opMatches(castTo(Types.TIME_1)));
        diff.put(DatetimePair.TIME_9_TIME_9, forTypePair(DatetimePair.TIME_9_TIME_9)
                .opMatches(castTo(Types.TIME_9)));

        diff.put(DatetimePair.TIMESTAMP_0_TIMESTAMP_0, forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                .opMatches(castTo(Types.TIMESTAMP_0)));
        diff.put(DatetimePair.TIMESTAMP_1_TIMESTAMP_1, forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_1)
                .opMatches(castTo(Types.TIMESTAMP_1)));
        diff.put(DatetimePair.TIMESTAMP_9_TIMESTAMP_9, forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                .opMatches(castTo(Types.TIMESTAMP_9)));

        diff.put(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0, forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_0)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1, forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_1)));
        diff.put(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9, forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                .opMatches(castTo(Types.TIMESTAMP_WLTZ_9)));

        return argsForMergeWithColumnAsValue().map(v -> diff.getOrDefault(v.get()[0], v));
    }

    private static Matcher<IgniteRel> mergeOperandMatcher(Matcher<RexNode> matcher) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                RelNode input = ((IgniteTableModify) actual).getInput();

                List<RexNode> operands = null;
                if (input instanceof IgniteProject) {
                    operands = ((IgniteProject) input).getProjects();
                } else if (input instanceof AbstractIgniteJoin) {
                    RexCall condition = ((RexCall) ((AbstractIgniteJoin) input).getCondition());
                    operands = condition.getOperands();
                }

                RexNode operand = Objects.requireNonNull(operands).get(1);
                assertThat(operand, matcher);

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }
}
