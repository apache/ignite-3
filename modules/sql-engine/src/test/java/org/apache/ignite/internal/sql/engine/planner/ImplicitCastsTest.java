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
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeCoercionRules;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Type coercion related tests that ensure that the necessary casts are placed where it is necessary.
 */
public class ImplicitCastsTest extends AbstractPlannerTest {


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

        // Parameter types are not checked during the validation phase.
        List<Object> params = List.of("anything");

        String query = format("SELECT * FROM A1 WHERE COL1 > CAST(? AS {})", rhs);
        assertPlan(query, igniteSchema, isInstanceOf(IgniteTableScan.class)
                .and(node -> {
                    String actualPredicate = node.condition().toString();

                    // lhs is not null, rhs may be null.
                    String castedLhs = castedExprNotNullable("$t1", expected.lhs);
                    String castedRhs =  castedExpr("?0", expected.rhs);
                    String expectedPredicate = format(">({}, {})",  castedLhs, castedRhs);

                    return Objects.equals(expectedPredicate, actualPredicate);
                }), params);
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
            // We use dynamic parameter in conditional expression and dynamic parameters has nullable types
            // So we need to add nullable flag to types fully match.
            if (expectedTypes.rhs != null && !expectedTypes.rhs.isNullable()) {
                RelDataType nullableRhs = TYPE_FACTORY.createTypeWithNullability(expectedTypes.rhs, true);
                expectedTypes = new ExpectedTypes(expectedTypes.lhs, nullableRhs);
            }

            return Arguments.of(values[0], values[1], expectedTypes);
        });
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

        // IgniteCustomType: test cases for custom data types in join and filter conditions.
        // Implicit casts must be added to the types a custom data type can be converted from.
        IgniteCustomTypeCoercionRules customTypeCoercionRules = TYPE_FACTORY.getCustomTypeCoercionRules();

        for (String customTypeName : TYPE_FACTORY.getCustomTypeSpecs().keySet()) {
            IgniteCustomType customType = TYPE_FACTORY.createCustomType(customTypeName);

            for (SqlTypeName sourceTypeName : customTypeCoercionRules.canCastFrom(customTypeName)) {
                RelDataType sourceType = TYPE_FACTORY.createSqlType(sourceTypeName);

                arguments.add(Arguments.of(sourceType, customType, new ExpectedTypes(customType, null)));
                arguments.add(Arguments.of(customType, sourceType, new ExpectedTypes(null, customType)));
            }
        }

        return arguments.stream();
    }
}
