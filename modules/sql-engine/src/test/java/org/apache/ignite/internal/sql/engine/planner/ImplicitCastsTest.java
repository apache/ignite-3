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

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.NativeTypeValues;
import org.apache.ignite.internal.sql.engine.util.StatementChecker;
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

    private static void addTable(IgniteSchema igniteSchema, String tableName, String columnName, RelDataType columnType) {
        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .add(columnName, columnType)
                .build();

        createTable(igniteSchema, tableName, tableType, IgniteDistributions.single());
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
                // Real/Float get mixed up.
                .filter(t -> t != SqlTypeName.REAL && t != SqlTypeName.FLOAT)
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
        IgniteSchema igniteSchema = new IgniteSchema("PUBLIC");

        addTable(igniteSchema, "A1", "COL1", lhs);
        addTable(igniteSchema, "B1", "COL1", rhs);

        String query = "select A1.*, B1.* from A1 join B1 on A1.col1 = B1.col1";
        assertPlan(query, igniteSchema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                        .and(nodeOrAnyChild(new TableScanWithProjection(expected.lhs)))
                        .and(nodeOrAnyChild(new TableScanWithProjection(expected.rhs)))
        ));
    }

    /** Nested loop join - casts are added to condition operands. **/
    @ParameterizedTest
    @MethodSource("joinColumnTypes")
    public void testNestedLoop(RelDataType lhs, RelDataType rhs, ExpectedTypes expected) throws Exception {
        IgniteSchema igniteSchema = new IgniteSchema("PUBLIC");

        addTable(igniteSchema, "A1", "COL1", lhs);
        addTable(igniteSchema, "B1", "COL1", rhs);

        String query = "select A1.*, B1.* from A1 join B1 on A1.col1 != B1.col1";
        assertPlan(query, igniteSchema, isInstanceOf(IgniteNestedLoopJoin.class).and(new NestedLoopWithFilter(expected)));
    }

    /** Filter clause - casts are added to condition operands. **/
    @ParameterizedTest
    @MethodSource("filterTypes")
    public void testFilter(RelDataType lhs, RelDataType rhs, ExpectedTypes expected) throws Exception {
        IgniteSchema igniteSchema = new IgniteSchema("PUBLIC");

        addTable(igniteSchema, "A1", "COL1", lhs);

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

    /** Type coercion in INSERT statement. */
    @TestFactory
    public Stream<DynamicTest> testInsert() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT64)
                        .sql("UPDATE t1 SET c1 = '10'")
                        .project("$t0", "10:BIGINT"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("UPDATE t1 SET c1 = 'abc'")
                        .project("$t0", "CAST(_UTF-8'abc'):INTEGER NOT NULL"),

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
                        .table("t1", (table) -> {
                            return table.name("T1")
                                    .addColumn("INT_COL", NativeTypes.INT32)
                                    .addColumn("STR_COL", NativeTypes.stringOf(4), "0000")
                                    .distribution(IgniteDistributions.single())
                                    .build();
                        })
                        .sql("INSERT INTO t1 VALUES(1, DEFAULT)")
                        .project("1", "DEFAULT()")
        );
    }

    /** Type coercion in UPDATE statement. */
    @TestFactory
    public Stream<DynamicTest> testUpdate() {
        return Stream.of(
                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("UPDATE t1 SET c1 = '1'")
                        .project("$t0", "1"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.INT32)
                        .sql("UPDATE t1 SET c1 = 'abc'")
                        .project("$t0", "CAST(_UTF-8'abc'):INTEGER NOT NULL"),

                checkStatement()
                        .table("t1", "c1", NativeTypes.stringOf(4))
                        .sql("UPDATE t1 SET c1 = 1")
                        .project("$t0", "_UTF-8'1':VARCHAR(4) CHARACTER SET \"UTF-8\""),

                // If int_col is accessed too early, we get:
                // java.lang.UnsupportedOperationException:
                //  at org.apache.calcite.util.Util.needToImplement(Util.java:1111)
                //  at org.apache.calcite.sql.validate.SqlValidatorImpl.getValidatedNodeType(SqlValidatorImpl.java:1795)
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
                sql("SELECT '1'::int IN ('a')").project("=(1, CAST(_UTF-8'a'):INTEGER NOT NULL)"),
                sql("SELECT 1 IN ('1', 2)").project("true"),
                sql("SELECT '1' IN (1, 2)").project("true"),
                sql("SELECT 2 IN ('c', 1)").project("=(2, CAST(_UTF-8'c'):INTEGER NOT NULL)"),

                checkStatement()
                        .table("t", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4), "bigint_col", NativeTypes.INT64)
                        .sql("SELECT int_col IN ('c', 1) FROM t")
                        .project("OR(=($t0, CAST(_UTF-8'c'):INTEGER NOT NULL), =($t0, 1))"),

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
                        .fail("Type mismatch in column 2 of UNION"),

                checkStatement()
                        .table("t", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4))
                        .sql("SELECT int_col, 1 FROM t UNION SELECT str_col, 1 FROM t")
                        .fail("Type mismatch in column 1 of UNION"),

                checkStatement()
                        .table("t", "int_col", NativeTypes.INT32, "str_col", NativeTypes.stringOf(4))
                        .sql("SELECT str_col, int_col FROM t UNION SELECT int_col, str_col FROM t")
                        .fail("Type mismatch in column 1 of UNION"),

                // See https://issues.apache.org/jira/browse/CALCITE-5130
                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT int_col FROM t1 UNION SELECT '1000'")
                        .fail("Type mismatch in column 1 of UNION"),

                checkStatement()
                        .table("t1", "int_col", NativeTypes.INT32)
                        .sql("SELECT '1000' UNION SELECT int_col FROM t1")
                        .fail("Type mismatch in column 1 of UNION")
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
    public Stream<DynamicTest> testCustomTypeDynamicParams() {
        Consumer<StatementChecker> setup = (checker) -> {
            checker.table("t1", "int_col", NativeTypes.INT32, "uuid_col", NativeTypes.UUID, "str_col", NativeTypes.STRING)
                    .table("t2", "int_col", NativeTypes.INT32, "uuid_col", NativeTypes.UUID, "str_col", NativeTypes.STRING);
        };

        return Stream.of(
                checkStatement(setup)
                        .table("t3", "str_col", NativeTypes.stringOf(36))
                        .sql("INSERT INTO t3 VALUES('1111'::UUID)")
                        .project("CAST(CAST(_UTF-8'1111'):UUID NOT NULL):VARCHAR(36) CHARACTER SET \"UTF-8\" NOT NULL"),

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
                        .fail("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT 'str'")
                        .fail("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT 1")
                        .fail("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT 1 UNION SELECT uuid_col FROM t1")
                        .fail("Type mismatch in column 1 of UNION"),

                checkStatement(setup)
                        .sql("SELECT uuid_col FROM t1 UNION SELECT str_col FROM t2")
                        .fail("Type mismatch in column 1 of UNION")
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
