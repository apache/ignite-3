package org.apache.ignite.internal.sql.engine.planner.datatypes;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DatetimePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for CASE operator, when operands belong to DATETIME types.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which operand.
 */
public class DateTimeCaseTypeCoercionTest extends BaseTypeCoercionTest {

    private static final IgniteSchema SCHEMA = createSchemaWithTwoColumnTable(NativeTypes.STRING, NativeTypes.STRING);

    private static final NativeType DYNAMIC_PARAM_TIMESTAMP_TYPE = NativeTypes.datetime(6);
    ;
    private static final NativeType DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE = NativeTypes.timestamp(6);

    /** CASE operands from columns. */
    @ParameterizedTest
    @MethodSource("caseArgs")
    public void caseColumnsTypeCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN c1 ELSE c2 END FROM t", schema,
                operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
    }

    private static Stream<Arguments> caseArgs() {
        return Stream.of(

                forTypePair(DatetimePair.DATE_DATE)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_0_TIME_1)
                        .firstOpMatches(castTo(Types.TIME_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(castTo(Types.TIME_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_1_TIME_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_1_TIME_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIME_1)),

                forTypePair(DatetimePair.TIME_1_TIME_9)
                        .firstOpMatches(castTo(Types.TIME_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_9_TIME_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIME_9)),

                forTypePair(DatetimePair.TIME_9_TIME_1)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIME_9)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_0)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_1))
                        .secondOpMatches(castTo(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_1)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_0))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_WLTZ_1)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_1))
                        .secondOpMatches(castTo(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame()
        );

    }

    /** CASE operands from dynamic params. */
    @ParameterizedTest
    @MethodSource("dynamicLiteralArgs")
    public void caseWithDynamicParamsCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        List<Object> params = List.of(
                SqlTestUtils.generateValueByType(typePair.first()),
                SqlTestUtils.generateValueByType(typePair.second())
        );

        assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN ? ELSE ? END FROM t", SCHEMA,
                operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, params);
    }

    private static Stream<Arguments> dynamicLiteralArgs() {
        return Stream.of(

                forTypePair(DatetimePair.DATE_DATE)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_0_TIME_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_1_TIME_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_1_TIME_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_1_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_9_TIME_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIME_9_TIME_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_0)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DEFAULT))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DEFAULT))
        );
    }

    /** CASE operands from literals. */
    @ParameterizedTest
    @MethodSource("literalArgs")
    public void caseWithLiteralsCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        List<Object> params = List.of(
                generateLiteralWithNoRepetition(typePair.first()), generateLiteralWithNoRepetition(typePair.second())
        );

        assertPlan(format("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN {} ELSE {} END FROM t", params.get(0), params.get(1)),
                SCHEMA, operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
    }

    private static Stream<Arguments> literalArgs() {
        return Stream.of(

                forTypePair(DatetimePair.DATE_DATE)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_0_TIME_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_9)),

                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_1_TIME_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_9)),

                forTypePair(DatetimePair.TIME_1_TIME_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_9)),

                forTypePair(DatetimePair.TIME_1_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIME_9_TIME_0)
                        .firstOpBeSame()
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_9)),

                forTypePair(DatetimePair.TIME_9_TIME_1)
                        .firstOpBeSame()
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_9)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_0)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_1))
                        .secondOpMatches(castTo(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_1)
                        .firstOpBeSame()
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_0))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_1)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_1))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1)
                        .firstOpBeSame()
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame()
        );
    }

    Matcher<IgniteRel> operandCaseMatcher(Matcher<RexNode> first, Matcher<RexNode> second) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                RexNode comparison = ((ProjectableFilterableTableScan) actual).projects().get(0);

                assertThat(comparison, instanceOf(RexCall.class));

                RexCall comparisonCall = (RexCall) comparison;

                RexNode firstOperand = comparisonCall.getOperands().get(1);
                RexNode secondOperand = comparisonCall.getOperands().get(2);

                assertThat("first operand: ", firstOperand, first);
                assertThat("second operand: ", secondOperand, second);

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

}
