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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Type coercion related tests that ensure that the necessary casts are placed where it is necessary.
 */
public class ImplicitCastsTest extends AbstractPlannerTest {

    private static final RelDataType INTEGER = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    private static final RelDataType FLOAT = TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT);


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
    public void testFilter(RelDataType lhs, ExpectedTypes expected) throws Exception {
        IgniteSchema igniteSchema = new IgniteSchema("PUBLIC");

        addTable(igniteSchema, "A1", "COL1", lhs);

        assertPlan("SELECT * FROM A1 WHERE COL1 > 1", igniteSchema, isInstanceOf(IgniteTableScan.class)
                .and(node -> {
                    String actualPredicate = node.condition().toString();
                    String expectedPredicate;

                    if (expected.lhs == null) {
                        expectedPredicate = ">($t1, 1)";
                    } else {
                        expectedPredicate = String.format(">(CAST($t1):%s NOT NULL, 1)", lhs);
                    }

                    return expectedPredicate.equals(actualPredicate);
                }));
    }

    private static Stream<Arguments> joinColumnTypes() {

        List<RelDataType> numericTypes = SqlTypeName.NUMERIC_TYPES.stream().map(t -> {
            if (t == SqlTypeName.DECIMAL) {
                return TYPE_FACTORY.createSqlType(t, 10, 2);
            } else {
                return TYPE_FACTORY.createSqlType(t);
            }
        }).collect(Collectors.toList());

        List<Arguments> arguments = new ArrayList<>();

        for (RelDataType lhs : numericTypes) {
            for (RelDataType rhs : numericTypes) {
                ExpectedTypes expectedTypes;
                if (lhs.equals(rhs)) {
                    expectedTypes = new ExpectedTypes(null, null);
                } else {
                    RelDataType t = TYPE_FACTORY.leastRestrictive(Arrays.asList(lhs, rhs));
                    expectedTypes = new ExpectedTypes(t.equals(lhs) ? null : t, t.equals(rhs) ? null : t);
                }
                arguments.add(Arguments.of(lhs, rhs, expectedTypes));
            }
        }

        return arguments.stream();
    }

    private static Stream<Arguments> filterTypes() {
        return Stream.of(
                Arguments.arguments(INTEGER, new ExpectedTypes(null, null)),
                Arguments.arguments(FLOAT, new ExpectedTypes(null, null))
        );
    }

    private static final class ExpectedTypes {
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
                String expectedProjections = String.format("[$t0, $t1, CAST($t1):%s NOT NULL]", expected);
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
            RelDataType expected1 = expected.lhs;
            RelDataType expected2 = expected.rhs;
            SqlOperator opToUse = SqlStdOperatorTable.NOT_EQUALS;
            String expectedCondition;

            if (expected1 != null && expected2 != null) {
                expectedCondition = String.format(
                        "%s(CAST($1):%s NOT NULL, CAST($3):%s NOT NULL)",
                        opToUse.getName(), expected1, expected2);

            } else if (expected1 == null && expected2 == null) {
                expectedCondition = String.format("%s($1, $3)", opToUse.getName());
            } else if (expected1 != null) {
                expectedCondition = String.format("%s(CAST($1):%s NOT NULL, $3)", opToUse.getName(), expected1);
            } else {
                expectedCondition = String.format("%s($1, CAST($3):%s NOT NULL)", opToUse.getName(), expected2);
            }

            return Objects.equals(actualCondition, expectedCondition);
        }
    }

    private static void addTable(IgniteSchema igniteSchema, String tableName, String columnName, RelDataType columnType) {
        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("ID", SqlTypeName.INTEGER)
                .add(columnName, columnType)
                .build();

        createTable(igniteSchema, tableName, tableType, IgniteDistributions.single());
    }
}
