package org.apache.ignite.internal.sql.engine.planner.datatypes;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;
import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for numeric functions, when operand belong to DATETIME types.
 */
public class DateTimeFunctionsTypeCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @MethodSource("dateTimeTypes")
    public void floor(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType), any(RexNode.class));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(nativeType);

        assertPlan("SELECT FLOOR(C1 TO SECOND) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimeTypes")
    public void ceil(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType), any(RexNode.class));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(nativeType);

        assertPlan("SELECT CEIL(C1 TO SECOND) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<NativeType> dateTimeTypes() {
        return Stream.of(
                Types.TIME_0,
                Types.TIME_1,
                Types.TIME_9,
                Types.TIMESTAMP_0,
                Types.TIMESTAMP_1,
                Types.TIMESTAMP_9,
                Types.TIMESTAMP_WLTZ_0,
                Types.TIMESTAMP_WLTZ_1,
                Types.TIMESTAMP_WLTZ_9
        );
    }

    @Test
    public void lastDay() throws Exception {
        NativeType nativeType = NativeTypes.DATE;
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DATE);

        assertPlan("SELECT LAST_DAY(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void dayName(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.stringOf(2000));

        assertPlan("SELECT DAYNAME(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void monthName(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.stringOf(2000));

        assertPlan("SELECT MONTHNAME(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void dayOfMonth(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT DAYOFMONTH(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void dayOfWeek(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT DAYOFWEEK(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateTimestamps")
    public void dayOfYear(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT DAYOFYEAR(C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<NativeType> dateTimestamps() {
        return Stream.of(
                Types.DATE,
                Types.TIMESTAMP_0,
                Types.TIMESTAMP_1,
                Types.TIMESTAMP_9,
                Types.TIMESTAMP_WLTZ_0,
                Types.TIMESTAMP_WLTZ_1,
                Types.TIMESTAMP_WLTZ_9
        );
    }

    @ParameterizedTest
    @MethodSource("extractTime")
    public void extractTime(String field, NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT EXTRACT(" + field + " FROM C1) FROM T", schema, matcher::matches, List.of());
        assertPlan("SELECT " + field + " (C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> extractTime() {
        List<NativeType> types = List.of(
                Types.TIME_0,
                Types.TIME_1,
                Types.TIME_9
        );

        return Stream.of("HOUR", "MINUTE", "SECOND").flatMap(f -> types.stream().map(t -> Arguments.of(f, t)));
    }

    @ParameterizedTest
    @MethodSource("extractDate")
    public void extractDate(String field, NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT EXTRACT(" + field + " FROM C1) FROM T", schema, matcher::matches, List.of());
        assertPlan("SELECT " + field + " (C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> extractDate() {
        return Stream.of("YEAR", "QUARTER", "MONTH", "WEEK").map(f -> Arguments.of(f, Types.DATE));
    }


    @ParameterizedTest
    @MethodSource("extractTimestamp")
    public void extractTimestamp(String field, NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);
        List<Matcher<RexNode>> args = List.of(any(RexNode.class), ofTypeWithoutCast(nativeType));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT64);

        assertPlan("SELECT EXTRACT(" + field + " FROM C1) FROM T", schema, matcher::matches, List.of());
        assertPlan("SELECT " + field + " (C1) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> extractTimestamp() {
        List<NativeType> types = List.of(
                Types.TIMESTAMP_0,
                Types.TIMESTAMP_1,
                Types.TIMESTAMP_9,
                Types.TIMESTAMP_WLTZ_0,
                Types.TIMESTAMP_WLTZ_1,
                Types.TIMESTAMP_WLTZ_9
        );

        return Stream.of(
                "YEAR", "QUARTER", "MONTH", "WEEK",
                "HOUR", "MINUTE", "SECOND"
        ).flatMap(f -> types.stream().map(t -> Arguments.of(f, t)));
    }

    @ParameterizedTest
    @MethodSource("timestamps")
    public void dateFromTimestamp(NativeType nativeType) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(nativeType);

        if (nativeType.spec() == NativeTypeSpec.DATETIME) {
            List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType));
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DATE);

            assertPlan("SELECT DATE(C1) FROM T", schema, matcher::matches, List.of());
        } else {
            List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(nativeType), any(RexNode.class));
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DATE);

            assertPlan("SELECT DATE(C1, 'UTC') FROM T", schema, matcher::matches, List.of());
        }
    }

    private static Stream<NativeType> timestamps() {
        return Stream.of(
                Types.TIMESTAMP_0,
                Types.TIMESTAMP_1,
                Types.TIMESTAMP_9,
                Types.TIMESTAMP_WLTZ_0,
                Types.TIMESTAMP_WLTZ_1,
                Types.TIMESTAMP_WLTZ_9
        );
    }

    private static class FunctionCallMatcher {

        private final List<Matcher<RexNode>> args;

        // Most of SQL functions propagate nullability from their arguments,
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

        Matcher<RelNode> resultWillBe(Matcher<RexCall> returnType) {
            return new FunctionCallMatcher.ProjectionRexNodeMatcher(new CallMatcher(returnType));
        }

        Matcher<RelNode> resultWillBe(NativeType returnType) {
            return new FunctionCallMatcher.ProjectionRexNodeMatcher(new CallMatcher(returnType));
        }

        Matcher<RelNode> resultWillBe(RelDataType returnType) {
            return new FunctionCallMatcher.ProjectionRexNodeMatcher(new CallMatcher(returnType));
        }

        private String expectedArguments() {
            return args.stream().map(Object::toString).collect(Collectors.joining(", "));
        }

        private class ProjectionRexNodeMatcher extends TypeSafeDiagnosingMatcher<RelNode> {

            private final CallMatcher callMatcher;

            private ProjectionRexNodeMatcher(CallMatcher callMatcher) {
                this.callMatcher = callMatcher;
            }

            @Override
            protected boolean matchesSafely(RelNode relNode, Description description) {
                RexCall call = getRexCall(relNode);
                if (call == null) {
                    return false;
                }

                if (call.getOperands().size() != args.size()) {
                    return false;
                }

                assertEquals(args.size(), call.getOperands().size(), "Number of arguments do not match");

                for (int i = 0; i < args.size(); i++) {
                    Matcher<RexNode> arg = args.get(i);
                    assertThat("Operand#" + i + ". Expected arguments: " + expectedArguments(), call.getOperands().get(i), arg);
                }

                callMatcher.checkCall(call, expectedArguments(), returnTypeNullability);
                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        }

        private static @Nullable RexCall getRexCall(RelNode relNode) {
            if (relNode instanceof ProjectableFilterableTableScan) {
                ProjectableFilterableTableScan scan = (ProjectableFilterableTableScan) relNode;
                List<RexNode> projects = scan.projects();
                return (RexCall) projects.get(0);
            } else if (relNode instanceof IgniteTableFunctionScan) {
                IgniteTableFunctionScan functionScan = (IgniteTableFunctionScan) relNode;
                return (RexCall) functionScan.getCall();
            } else {
                return null;
            }
        }
    }


    private static class CallMatcher {

        private final NativeType nativeType;

        private final RelDataType relDataType;

        private final Matcher<RexCall> callMatcher;

        CallMatcher(NativeType nativeType) {
            this.nativeType = nativeType;
            this.relDataType = null;
            this.callMatcher = null;
        }

        CallMatcher(RelDataType relDataType) {
            this.nativeType = null;
            this.relDataType = relDataType;
            this.callMatcher = null;
        }

        CallMatcher(Matcher<RexCall> callMatcher) {
            this.nativeType = null;
            this.relDataType = null;
            this.callMatcher = callMatcher;
        }

        void checkCall(RexCall call, String expectedArguments, boolean returnTypeNullability) {
            IgniteTypeFactory tf = Commons.typeFactory();
            RelDataType actualRelType = call.getType();
            RelDataType expectedRelType;

            if (nativeType != null || relDataType != null) {

                if (nativeType != null) {
                    expectedRelType = native2relationalType(tf, nativeType, returnTypeNullability);
                } else {
                    expectedRelType = tf.createTypeWithNullability(relDataType, returnTypeNullability);
                }

                String message = "Expected return type "
                        + expectedRelType + " but got " + actualRelType
                        + ". Expected arguments: " + expectedArguments;

                assertEquals(actualRelType, expectedRelType, message);
            } else if (callMatcher != null) {
                assertThat("Return type does not match", call, callMatcher);
            } else {
                throw new IllegalStateException("Not possible");
            }
        }
    }
}
