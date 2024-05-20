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

package org.apache.ignite.internal.sql.engine.planner;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.NativeTypeValues;
import org.apache.ignite.internal.sql.engine.util.StatementChecker;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Type coercion related tests that ensure that the necessary casts are placed where it is necessary.
 */
public class ImplicitCastsTest extends AbstractPlannerTest {
    private static IgniteTable tableWithColumn(String tableName, String columnName, RelDataType columnType) {
        return TestBuilders.table()
                .name(tableName)
                .addColumn("ID", NativeTypes.INT32, false)
                .addColumn(columnName, IgniteTypeFactory.relDataTypeToNative(columnType), false)
                .distribution(IgniteDistributions.single())
                .build();
    }

    private static String castedExpr(String idx, @Nullable RelDataType type) {
        if (type == null) {
            return idx;
        } else {
            return format("CAST({}):{}", idx, type.isNullable() ? type.toString() : type + " NOT NULL");
        }
    }

    private static String castedExprNotNullable(String idx, @Nullable RelDataType type) {
        if (type != null) {
            return castedExpr(idx, TYPE_FACTORY.createTypeWithNullability(type, false));
        } else {
            return castedExpr(idx, null);
        }
    }

    private static Stream<Arguments> filterTypes() {
        return joinColumnTypes().map(args -> {
            Object[] values = args.get();
            ExpectedTypes expectedTypes = (ExpectedTypes) values[2];
            // We use dynamic parameter in conditional expression and dynamic parameters has nullable types.
            // So we need to set nullability.
            if (expectedTypes.rhs != null) {
                RelDataType mayBeNullableRhs = TYPE_FACTORY.createTypeWithNullability(expectedTypes.rhs, true);
                expectedTypes = new ExpectedTypes(expectedTypes.lhs, mayBeNullableRhs);
            }

            return Arguments.of(values[0], values[1], expectedTypes);
        });
    }

    private static Stream<Arguments> joinColumnTypes() {

        List<RelDataType> numericTypes = SqlTypeName.NUMERIC_TYPES.stream()
                // Real/Float got mixed up.
                .filter(t -> t != SqlTypeName.FLOAT)
                .map(TYPE_FACTORY::createSqlType)
                .collect(Collectors.toList());

        List<Arguments> arguments = new ArrayList<>();

        for (RelDataType lhs : numericTypes) {
            for (RelDataType rhs : numericTypes) {
                ExpectedTypes expectedTypes;
                if (lhs.equals(rhs)) {
                    expectedTypes = new ExpectedTypes(null, null);
                } else {
                    List<RelDataType> types = Arrays.asList(lhs, rhs);
                    RelDataType t = TYPE_FACTORY.leastRestrictive(types);
                    if (t == null) {
                        String error = format(
                                "No least restrictive types between {}. This case requires special additional hand coding", types
                        );
                        throw new IllegalArgumentException(error);
                    }
                    expectedTypes = new ExpectedTypes(t.equals(lhs) ? null : t, t.equals(rhs) ? null : t);
                }
                arguments.add(Arguments.of(lhs, rhs, expectedTypes));
            }
        }

        List<Arguments> result = new ArrayList<>(arguments);

        arguments.stream().map(args -> {
            Object[] argsVals = args.get();
            Object lhs = argsVals[1];
            Object rhs = argsVals[0];
            ExpectedTypes expected = (ExpectedTypes) argsVals[2];

            return Arguments.of(lhs, rhs, new ExpectedTypes(expected.rhs, expected.lhs));
        });

        return result.stream();
    }

    /** MergeSort join - casts are pushed down to children. **/
    @ParameterizedTest
    @MethodSource("joinColumnTypes")
    public void testMergeSort(RelDataType lhs, RelDataType rhs, ExpectedTypes expected) throws Exception {
        IgniteSchema igniteSchema = createSchema(
                tableWithColumn("A1", "COL1", lhs),
                tableWithColumn("B1", "COL1", rhs)
        );

        String query = "select A1.*, B1.* from A1 join B1 on A1.col1 = B1.col1";
        assertPlan(query, igniteSchema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                        .and(nodeOrAnyChild(new TableScanWithProjection(expected.lhs)))
                        .and(nodeOrAnyChild(new TableScanWithProjection(expected.rhs)))
        ), "HashJoinConverter", "NestedLoopJoinConverter");
    }

    /** Nested loop join - casts are added to condition operands. **/
    @ParameterizedTest
    @MethodSource("joinColumnTypes")
    public void testNestedLoop(RelDataType lhs, RelDataType rhs, ExpectedTypes expected) throws Exception {
        IgniteSchema igniteSchema = createSchema(
                tableWithColumn("A1", "COL1", lhs),
                tableWithColumn("B1", "COL1", rhs)
        );


        String query = "select A1.*, B1.* from A1 join B1 on A1.col1 != B1.col1";
        assertPlan(query, igniteSchema, isInstanceOf(IgniteNestedLoopJoin.class).and(new NestedLoopWithFilter(expected)));
    }

    /** Filter clause - casts are added to condition operands. **/
    @ParameterizedTest
    @MethodSource("filterTypes")
    public void testFilter(RelDataType lhs, RelDataType rhs, ExpectedTypes expected) throws Exception {
        IgniteSchema igniteSchema = createSchema(
                tableWithColumn("A1", "COL1", lhs)
        );

        List<Object> params = NativeTypeValues.values(rhs, 1);

        String query = format("SELECT * FROM A1 WHERE COL1 > ?", rhs);
        assertPlan(query, igniteSchema, isInstanceOf(IgniteTableScan.class)
                .and(node -> {
                    String actualPredicate = node.condition().toString();

                    // lhs is not null, rhs may be null.
                    String castedLhs = castedExprNotNullable("$t1", expected.lhs);
                    String castedRhs = castedExpr("?0", expected.rhs);
                    String expectedPredicate = format(">({}, {})", castedLhs, castedRhs);

                    return Objects.equals(expectedPredicate, actualPredicate);
                }), params);
    }

    /** Index - implicit casts in index search bounds. **/
    @ParameterizedTest
    @MethodSource("filterTypesForIndex")
    public void testIndex(RelDataType lhs, RelDataType rhs, ExpectedTypes expected, Type indexType) throws Exception {
        UnaryOperator<TableBuilder> tableOp = tableBuilder -> tableBuilder
                .name("A1")
                .addColumn("ID", NativeTypes.INT32, false)
                .addColumn("COL1", IgniteTypeFactory.relDataTypeToNative(lhs), false)
                .distribution(IgniteDistributions.single());

        UnaryOperator<TableBuilder> indexOp;
        switch (indexType) {
            case SORTED:
                indexOp = addSortIndex("COL1");
                break;
            case HASH:
                indexOp = addHashIndex("COL1");
                break;
            default:
                throw new IllegalArgumentException("Unexpected index type " + indexType);
        }

        IgniteSchema igniteSchema = createSchemaFrom(tableOp.andThen(indexOp));

        List<Object> params = NativeTypeValues.values(rhs, 1);

        String query = format("SELECT * FROM A1 WHERE COL1 = ?", rhs);
        assertPlan(query, igniteSchema, isInstanceOf(IgniteIndexScan.class)
                .and(node -> {
                    String actualPredicate = node.condition().toString();

                    // lhs is not null, rhs may be null.
                    String castedLhs = castedExprNotNullable("$t1", expected.lhs);
                    String castedRhs = castedExpr("?0", expected.rhs);
                    String expectedPredicate = format("=({}, {})", castedLhs, castedRhs);

                    return Objects.equals(expectedPredicate, actualPredicate);
                }), params);
    }

    private static Stream<Arguments> filterTypesForIndex() {
        Predicate<Arguments> filter = args -> {
            Object[] vals = args.get();
            RelDataType lhs = (RelDataType) vals[0];
            RelDataType rhs = (RelDataType) vals[1];

            // TODO: https://issues.apache.org/jira/browse/IGNITE-19881
            //       https://issues.apache.org/jira/browse/IGNITE-19882
            //   SearchBounds are not built for types t1 and t2 when
            //   t1 != t2 AND either of them is approx numeric or decimal.
            //   For integral numeric types t1 != t2 search bounds are always built.
            if (SqlTypeUtil.isApproximateNumeric(lhs) || SqlTypeUtil.isApproximateNumeric(rhs)
                    || SqlTypeUtil.isDecimal(lhs) || SqlTypeUtil.isDecimal(rhs)) {

                return SqlTypeUtil.equalSansNullability(lhs, rhs);
            } else {
                return true;
            }
        };

        final class AddIndexType implements Function<Arguments, Arguments> {

            private final Type type;

            private AddIndexType(Type type) {
                this.type = type;
            }

            @Override
            public Arguments apply(Arguments args) {
                Object[] vals = args.get();
                RelDataType lhs = (RelDataType) vals[0];
                RelDataType rhs = (RelDataType) vals[1];
                ExpectedTypes expected = (ExpectedTypes) vals[2];

                return Arguments.of(lhs, rhs, expected, type);
            }
        }

        Stream<Arguments> s1 = filterTypes().filter(filter).map(new AddIndexType(Type.SORTED));
        Stream<Arguments> s2 = filterTypes().filter(filter).map(new AddIndexType(Type.HASH));

        return Stream.concat(s1, s2);
    }

    /** Type coercion in INSERT statement. */
    @TestFactory
    public Stream<DynamicTest> testInsert() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("UPDATE t1 SET c1 = '10'")
                        .project("$t0", "10:BIGINT"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT64, "c2", NativeTypes.INT64)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32)
                        .sql("INSERT INTO t1 (c1, c2) SELECT c1, c2 FROM t2")
                        .project("CAST($t0):BIGINT", "CAST($t1):BIGINT"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.stringOf(4), "c2", NativeTypes.stringOf(4))
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32)
                        .sql("INSERT INTO t1 (c1, c2) SELECT c1, 10 FROM t2")
                        .project("CAST($t0):VARCHAR(4) CHARACTER SET \"UTF-8\"", "_UTF-8'10':VARCHAR(4) CHARACTER SET \"UTF-8\""),

                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT64,
                                "bigint_col", NativeTypes.STRING, "smallint_col", NativeTypes.stringOf(4))
                        .table("t2", "int_col", NativeTypes.INT32,
                                "smallint_col", NativeTypes.INT16, "str_col", NativeTypes.stringOf(4))
                        .sql("INSERT INTO t1 (int_col, bigint_col, smallint_col) SELECT int_col, smallint_col, str_col FROM t2")
                        .project("CAST($t0):BIGINT", "CAST($t1):VARCHAR(65536) CHARACTER SET \"UTF-8\"", "$t2"),

                // DEFAULT is not coerced
                checkStatement()
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t1", (table) -> {
                            return table.name("T1")
                                    .addColumn("INT_COL", NativeTypes.INT32)
                                    .addColumn("STR_COL", NativeTypes.stringOf(4), "0000")
                                    .distribution(IgniteDistributions.single())
                                    .build();
                        })
                        .sql("INSERT INTO t1 VALUES(1, DEFAULT)")
                        .project("1", "_UTF-8'0000'")
        );
    }

    /** Type coercion in UPDATE statement. */
    @TestFactory
    public Stream<DynamicTest> testUpdate() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("UPDATE t1 SET c1 = '1'::INTEGER + 1")
                        .project("$t0", "+(1, 1)"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.stringOf(4))
                        .sql("UPDATE t1 SET c1 = 1")
                        .project("$t0", "_UTF-8'1':VARCHAR(4) CHARACTER SET \"UTF-8\""),

                checkStatement()
                        .table("t1", "id", NativeTypes.INT32, "int_col", NativeTypes.INT32, "str_col", NativeTypes.STRING)
                        .sql("UPDATE t1 SET str_col = 1, int_col = id + 1")
                        .project("$t0", "$t1", "$t2", "_UTF-8'1':VARCHAR(65536) CHARACTER SET \"UTF-8\"", "+($t0, 1)")
        );
    }

    /** Type coercion in MERGE statement. */
    @TestFactory
    public Stream<DynamicTest> testMergeMatchProjection() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT16, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.stringOf(4), "c3", NativeTypes.INT64)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, src.c3)";
                            return sql;
                        })
                        .project("CAST($0):INTEGER", "CAST($1):VARCHAR(4) CHARACTER SET \"UTF-8\"", "CAST($2):BIGINT"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.stringOf(4), "c3", NativeTypes.INT64)
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = src.c2, c3 = src.c3";
                            return sql;
                        })
                        .project("$0", "$1", "$2", "CAST($4):VARCHAR(4) CHARACTER SET \"UTF-8\"", "CAST($5):BIGINT"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32, "c2", NativeTypes.stringOf(4), "c3", NativeTypes.INT32)
                        .table("t2", "c1", NativeTypes.INT32, "c2", NativeTypes.INT32, "c3", NativeTypes.stringOf(3))
                        .sql(() -> {
                            String sql = "MERGE INTO T2 dst USING t1 src ON dst.c1 = src.c1 "
                                    + "WHEN MATCHED THEN UPDATE SET c2 = src.c3 "
                                    + "WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES (src.c1, src.c2, src.c3)";
                            return sql;
                        })
                        .project("$3", "CAST($4):INTEGER", "CAST($5):VARCHAR(3) CHARACTER SET \"UTF-8\"", "$0", "$1", "$2", "$5")
        );
    }

    /** IN expression. */
    @TestFactory
    public Stream<DynamicTest> testInExpression() {
        return Stream.of(
                // literals
                sql("SELECT '1'::int IN ('1'::INTEGER)").project("true"),
                sql("SELECT 1 IN ('1', 2)").project("true"),
                sql("SELECT '1' IN (1, 2)").project("true"),
                sql("SELECT 2 IN ('2'::REAL, 1)").project("true"),

                checkStatement()
                        .table("t", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4), "bigint_col", NativeTypes.INT64)
                        .sql("SELECT int_col IN ('c'::REAL, 1) FROM t")
                        .project("OR(=(CAST($t0):REAL, CAST(_UTF-8'c'):REAL NOT NULL), =(CAST($t0):REAL, 1))"),

                checkStatement()
                        .table("t", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4), "bigint_col", NativeTypes.INT64)
                        .sql("SELECT int_col IN (1, bigint_col) FROM t")
                        .project("OR(=(CAST($t0):BIGINT, 1), =(CAST($t0):BIGINT, $t1))"),

                checkStatement()
                        .table("t", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4), "bigint_col", NativeTypes.INT64)
                        .sql("SELECT str_col IN (1, bigint_col) FROM t")
                        .project("OR(=(CAST($t0):BIGINT, 1), =(CAST($t0):BIGINT, $t1))")
        );
    }

    /** Set operations to least restrictive type. */
    @TestFactory
    public Stream<DynamicTest> testSetOps() {
        return Stream.of(
                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32, "dec2_col", NativeTypes.decimalOf(4, 2))
                        .table("t2", "smallint_col", NativeTypes.INT32, "dec54_col", NativeTypes.decimalOf(8, 4))
                        .sql("SELECT int_col, dec2_col FROM t1 UNION SELECT smallint_col, dec54_col FROM t2")
                        .ok(expectRowType("INTEGER", "DECIMAL(8, 4)")),

                checkStatement()
                        .table("t1", "int1_col", NativeTypes.INT32, "int2_col", NativeTypes.INT32)
                        .table("t2", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4))
                        .sql("SELECT int1_col, int2_col FROM t1 UNION SELECT int_col, str_col FROM t2")
                        .fails("Type mismatch in column 2 of UNION"),

                checkStatement()
                        .table("t", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4))
                        .sql("SELECT int_col, 1 FROM t UNION SELECT str_col, 1 FROM t")
                        .fails("Type mismatch in column 1 of UNION"),

                checkStatement()
                        .table("t", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4))
                        .sql("SELECT str_col, int_col FROM t UNION SELECT int_col, str_col FROM t")
                        .fails("Type mismatch in column 1 of UNION"),

                // See https://issues.apache.org/jira/browse/CALCITE-5130
                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT int_col FROM t1 UNION SELECT '1000'")
                        .fails("Type mismatch in column 1 of UNION"),

                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT '1000' UNION SELECT int_col FROM t1")
                        .fails("Type mismatch in column 1 of UNION")
        );
    }

    private static Consumer<IgniteRel> expectRowType(String... fieldTypes) {
        return (node) -> {
            String actualTypesList = node.getRowType().getFieldList().stream()
                    .map(f -> f.getType().toString()).collect(Collectors.joining(", "));

            String expectedTypeList = String.join(", ", fieldTypes);
            assertEquals(expectedTypeList, actualTypesList, "Row type. Node: " + node);
        };
    }

    /**
     * Custom data types implicit casts.
     */
    @TestFactory
    public Stream<DynamicTest> testCustomTypes() {
        Consumer<StatementChecker> setup = (checker) -> {
            checker.table("t1", "int_col", NativeTypes.INT32, "uuid_col", NativeTypes.UUID, "str_col", NativeTypes.STRING)
                    .table("t2", "int_col", NativeTypes.INT32, "uuid_col", NativeTypes.UUID, "str_col", NativeTypes.STRING);
        };

        return Stream.of(
                checkStatement(setup)
                        .disableRules(DISABLE_KEY_VALUE_MODIFY_RULES)
                        .table("t3", "str_col", NativeTypes.stringOf(36))
                        .sql("INSERT INTO t3 VALUES('1111'::UUID)")
                        .project("CAST(CAST(_UTF-8'1111'):UUID NOT NULL):VARCHAR CHARACTER SET \"UTF-8\" NOT NULL"),

                checkStatement(setup)
                        .table("t3", "str_col", NativeTypes.stringOf(36))
                        .sql("UPDATE t1 SET str_col='1111'::UUID")
                        .project("$t0", "$t1", "$t2",
                                "CAST(CAST(_UTF-8'1111'):UUID NOT NULL):VARCHAR(65536) CHARACTER SET \"UTF-8\" NOT NULL"),

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT uuid_col FROM t2")
                        .ok(),

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT ?", new UUID(0, 0))
                        .ok(),

                // incompatible types.
                // https://issues.apache.org/jira/browse/IGNITE-19503
                /*
                checkStatement(setup)
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("UPDATE t1 SET str_col='1111'::UUID")
                        .fail("!!!!"),

                checkStatement(setup)
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("INSERT INTO t1 VALUES('1111'::UUID)")
                        .fail("!!!!"),
                 */

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT ?", "str")
                        .fails("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT 'str'")
                        .fails("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT 1")
                        .fails("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT 1 UNION SELECT uuid_col FROM t1")
                        .fails("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT str_col FROM t2")
                        .fails("Type mismatch in column 1 of UNION")
        );
    }

    private static final class ExpectedTypes {

        // null means no conversion is necessary.
        final RelDataType lhs;

        final RelDataType rhs;

        ExpectedTypes(@Nullable RelDataType lhs, @Nullable RelDataType rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public String toString() {
            return lhs + " " + rhs;
        }
    }

    static final class TableScanWithProjection implements Predicate<RelNode> {

        @Nullable
        private final RelDataType expected;

        TableScanWithProjection(@Nullable RelDataType expected) {
            this.expected = expected;
        }

        @Override
        public boolean test(RelNode node) {
            if (!(node instanceof IgniteTableScan)) {
                return false;
            }
            IgniteTableScan scan = (IgniteTableScan) node;

            if (expected == null) {
                return scan.projects() == null;
            } else {
                String expectedProjections = format("[$t0, $t1, {}]", castedExpr("$t1", expected));
                String actualProjections;

                if (scan.projects() == null) {
                    actualProjections = null;
                } else {
                    actualProjections = scan.projects().toString();
                }

                return Objects.equals(actualProjections, expectedProjections);
            }
        }
    }

    static final class NestedLoopWithFilter implements Predicate<IgniteNestedLoopJoin> {

        private final ExpectedTypes expected;

        NestedLoopWithFilter(ExpectedTypes expected) {
            this.expected = expected;
        }

        @Override
        public boolean test(IgniteNestedLoopJoin node) {
            String actualCondition = node.getCondition().toString();
            SqlOperator opToUse = SqlStdOperatorTable.NOT_EQUALS;

            String castedLhs = castedExpr("$1", expected.lhs);
            String castedRhs = castedExpr("$3", expected.rhs);
            String expectedCondition = format("{}({}, {})", opToUse.getName(), castedLhs, castedRhs);

            return Objects.equals(actualCondition, expectedCondition);
        }
    }
}
