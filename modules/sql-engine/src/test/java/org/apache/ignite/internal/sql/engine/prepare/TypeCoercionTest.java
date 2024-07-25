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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.framework.TestStatistic;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeCoercionRules;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests type coercion behaviour.
 *
 * @see IgniteTypeCoercion
 */
public class TypeCoercionTest extends AbstractPlannerTest {

    private static final RelDataType VARCHAR = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 20);

    private static final List<RelDataType> NUMERIC_TYPES = SqlTypeName.NUMERIC_TYPES.stream().map(t -> {
        if (t == SqlTypeName.DECIMAL) {
            return TYPE_FACTORY.createSqlType(t, 10, 2);
        } else {
            return TYPE_FACTORY.createSqlType(t);
        }
    }).collect(Collectors.toList());

    @ParameterizedTest
    @MethodSource("booleanToNumeric")
    public void testBooleanToNumeric(TypeCoercionRule rule) {
        var tester = new BinaryOpTypeCoercionTester(rule);
        tester.execute();
    }

    private static Stream<TypeCoercionRule> booleanToNumeric() {
        List<TypeCoercionRule> numericRules = new ArrayList<>();
        RelDataType booleanType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);

        for (RelDataType type : NUMERIC_TYPES) {
            numericRules.add(typeCoercionIsNotSupported(type, booleanType));
            numericRules.add(typeCoercionIsNotSupported(booleanType, type));
        }

        return numericRules.stream();
    }

    @ParameterizedTest
    @MethodSource("varCharToNumeric")
    public void testVarCharToNumeric(TypeCoercionRule rule) {
        var tester = new BinaryOpTypeCoercionTester(rule);
        tester.execute();
    }

    private static Stream<TypeCoercionRule> varCharToNumeric() {
        List<TypeCoercionRule> numericRules = new ArrayList<>();
        RelDataType expectedDecimalType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 32767, 16383);

        for (RelDataType type : NUMERIC_TYPES) {
            if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
                numericRules.add(typeCoercionRule(type, VARCHAR, new ToSpecificType(expectedDecimalType)));
                numericRules.add(typeCoercionRule(VARCHAR, type, new ToSpecificType(expectedDecimalType)));
            } else {
                // cast the right hand side to type
                numericRules.add(typeCoercionRule(type, VARCHAR, new ToSpecificType(type)));
                // cast the left hand side to type
                numericRules.add(typeCoercionRule(VARCHAR, type, new ToSpecificType(type)));
            }
        }

        return numericRules.stream();
    }

    @ParameterizedTest
    @MethodSource("numericCoercionRules")
    public void testTypeCoercionBetweenNumericTypes(TypeCoercionRule rule) {
        var tester = new BinaryOpTypeCoercionTester(rule);
        tester.execute();
    }

    private static Stream<TypeCoercionRule> numericCoercionRules() {
        List<TypeCoercionRule> numericRules = new ArrayList<>();

        for (RelDataType lhs : NUMERIC_TYPES) {
            for (RelDataType rhs : NUMERIC_TYPES) {
                if (lhs.equals(rhs)) {
                    numericRules.add(typeCoercionRule(lhs, rhs, new NoTypeCoercion()));
                } else {
                    numericRules.add(typeCoercionRule(lhs, rhs, new ToLeastRestrictiveType()));
                }
            }
        }

        return numericRules.stream();
    }

    @ParameterizedTest
    @MethodSource("numericToInterval")
    public void testNumericToInterval(TypeCoercionRule rule) {
        var tester = new BinaryOpTypeCoercionTester(rule);
        tester.execute();
    }

    private static Stream<TypeCoercionRule> numericToInterval() {
        List<TypeCoercionRule> numericRules = new ArrayList<>();
        SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.HOUR, SqlParserPos.ZERO);
        RelDataType intervalType = TYPE_FACTORY.createSqlIntervalType(intervalQualifier);

        for (RelDataType type : NUMERIC_TYPES) {
            numericRules.add(typeCoercionIsNotSupported(type, intervalType));
            numericRules.add(typeCoercionIsNotSupported(intervalType, type));
        }

        return numericRules.stream();
    }

    @ParameterizedTest
    @MethodSource("numericToDate")
    public void testNumericToDate(TypeCoercionRule rule) {
        var tester = new BinaryOpTypeCoercionTester(rule);
        tester.execute();
    }

    private static Stream<TypeCoercionRule> numericToDate() {
        List<TypeCoercionRule> numericRules = new ArrayList<>();
        RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);

        for (RelDataType type : NUMERIC_TYPES) {
            numericRules.add(typeCoercionIsNotSupported(type, dateType));
            numericRules.add(typeCoercionIsNotSupported(dateType, type));
        }

        return numericRules.stream();
    }

    @ParameterizedTest
    @MethodSource("varcharToInterval")
    public void testTypeCoercionBetweenIntervalAndVarchar(TypeCoercionRule rule) {
        var tester = new BinaryOpTypeCoercionTester(rule);
        tester.execute();
    }

    private static Stream<TypeCoercionRule> varcharToInterval() {
        List<TypeCoercionRule> numericRules = new ArrayList<>();

        SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.HOUR, SqlParserPos.ZERO);
        RelDataType intervalType = TYPE_FACTORY.createSqlIntervalType(intervalQualifier);

        numericRules.add(typeCoercionRule(VARCHAR, intervalType, new NoTypeCoercion()));
        numericRules.add(typeCoercionRule(intervalType, VARCHAR, new NoTypeCoercion()));

        return numericRules.stream();
    }

    @Test
    public void testCaseWhenExpression() {
        RelDataType decimal = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 11, 1);

        checkExprResult("COALESCE(12.2, 2)", decimal);
        checkExprResult("COALESCE(2, 12.2)", decimal);
    }

    /**
     * SQL 2016, clause 9.5: Mixing types in CASE/COALESCE expressions is illegal.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "COALESCE('b', 2)",
            "COALESCE(2, 'b')",
            "COALESCE(2, COALESCE('b', 2))",
            "COALESCE(2, COALESCE(2, 'b'))",
            "COALESCE('b', COALESCE(2, 3))",
            "CASE WHEN 1=1 THEN 12.2 ELSE 'b' END",
    })
    public void testCaseWhenExpressionWithMixedTypesIsRejected(String expr) {
        checkExprResultFails(expr, "Illegal mixing of types in CASE or COALESCE statement");
    }

    @Test
    public void testNullIf() {
        RelDataType decimal = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 3, 1);
        checkExprResult("NULLIF(12.2, 2)", nullable(decimal));
    }

    @ParameterizedTest
    @MethodSource("varcharToUuid")
    public void testTypeCoercionBetweenUuidAndVarchar(TypeCoercionRule rule) {
        var tester = new BinaryOpTypeCoercionTester(rule);
        tester.execute();
    }

    private static Stream<TypeCoercionRule> varcharToUuid() {
        List<TypeCoercionRule> rules = new ArrayList<>();

        RelDataType uuidType = TYPE_FACTORY.createCustomType(UuidType.NAME);

        rules.add(typeCoercionRule(VARCHAR, uuidType, new ToSpecificType(uuidType)));
        rules.add(typeCoercionRule(uuidType, VARCHAR, new ToSpecificType(uuidType)));

        return rules.stream();
    }

    @ParameterizedTest
    @MethodSource("dataForValuesCoercionCheck")
    public void testCoercionForValues(String query, SqlTypeName rowType, boolean typeChanged) {
        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("C1", rowType, 1)
                .build();

        TestTable testTable = new TestTable("T", tableType, IgniteDistributions.single());

        runTest(query, (planner, insertNode) -> {
            SqlValidator validator = planner.validator();
            validator.validate(insertNode);
            insertNode.accept(new SqlShuttle() {
                @Override
                public SqlNode visit(SqlDataTypeSpec type) {
                    if (typeChanged) {
                        SqlBasicTypeNameSpec typeName = (SqlBasicTypeNameSpec) type.getTypeNameSpec();
                        assertEquals(SqlTypeName.VARCHAR.getName(), typeName.getTypeName().toString());
                        assertEquals(RelDataType.PRECISION_NOT_SPECIFIED, typeName.getPrecision());
                    } else {
                        fail("No type coercion need to be raised.");
                    }
                    return type;
                }
            });
        }, createSchema(testTable));
    }

    private static Stream<Arguments> dataForValuesCoercionCheck() {
        List<Arguments> arguments = new ArrayList<>();
        arguments.add(Arguments.of("INSERT INTO t VALUES (123)", SqlTypeName.CHAR, true));
        arguments.add(Arguments.of("INSERT INTO t VALUES (123 || '1')", SqlTypeName.CHAR, true));
        arguments.add(Arguments.of("INSERT INTO t VALUES ('123')", SqlTypeName.CHAR, false));
        arguments.add(Arguments.of("INSERT INTO t VALUES (123)", SqlTypeName.VARCHAR, true));
        arguments.add(Arguments.of("INSERT INTO t VALUES (123 || '1')", SqlTypeName.VARCHAR, true));
        arguments.add(Arguments.of("INSERT INTO t VALUES ('123')", SqlTypeName.VARCHAR, false));
        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource("commonTypeForBinaryComparison")
    public void testCommonTypeForBinaryComparisonForCustomDataTypes(RelDataType type1, RelDataType type2, RelDataType commonType) {
        runTest("SELECT 1", (planner, ignore) -> {
            SqlValidator validator = planner.validator();
            IgniteTypeCoercion typeCoercion = new IgniteTypeCoercion(TYPE_FACTORY, validator);
            RelDataType actualCommonType = typeCoercion.commonTypeForBinaryComparison(type1, type2);

            assertEquals(commonType, actualCommonType);
        });
    }

    private static Stream<Arguments> commonTypeForBinaryComparison() {
        List<Arguments> arguments = new ArrayList<>();

        // IgniteCustomType: test cases for common type in binary comparison between
        // a custom data type and the types it can be converted from.

        IgniteCustomTypeCoercionRules customTypeCoercionRules = TYPE_FACTORY.getCustomTypeCoercionRules();

        for (String typeName : TYPE_FACTORY.getCustomTypeSpecs().keySet()) {
            IgniteCustomType customType = TYPE_FACTORY.createCustomType(typeName);

            for (SqlTypeName sourceTypeName : customTypeCoercionRules.canCastFrom(typeName)) {
                RelDataType sourceType = TYPE_FACTORY.createSqlType(sourceTypeName);

                arguments.add(Arguments.of(customType, sourceType, customType));
                arguments.add(Arguments.of(sourceType, customType, customType));
            }
        }

        return arguments.stream();
    }

    private final class BinaryOpTypeCoercionTester {

        final TypeCoercionRule rule;

        private BinaryOpTypeCoercionTester(TypeCoercionRule rule) {
            this.rule = rule;
        }

        void execute() {
            if (rule.type == null) {
                expectError(rule);
            } else {
                expectCoercion(rule);
            }
        }

        void expectCoercion(TypeCoercionRule rule) {
            if (rule.type == null) {
                throw new IllegalStateException("rule type is not specified");
            }

            runBinaryOpTypeCoercionTest(rule, (planner, node) -> {
                SqlNode validNode = planner.validate(node);
                SqlSelect sqlSelect = (SqlSelect) validNode;
                String originalExpr = String.format("`A`.`C1` %s `A`.`C2`", rule.operator.getName());

                SqlCall sqlBasicCall = (SqlCall) sqlSelect.getSelectList().get(0);
                boolean coerced = !originalExpr.equals(sqlBasicCall.toString());
                checkBinaryOpTypeCoercionResult(sqlBasicCall, rule.lhs, rule.rhs, rule.operator, coerced, rule.type);
            });
        }

        void expectError(TypeCoercionRule rule) {
            if (rule.type != null) {
                throw new IllegalStateException("rule type is specified. Call expectCoercion instead.");
            }

            runBinaryOpTypeCoercionTest(rule, (planner, node) -> {
                String error = String.format("Values passed to %s operator must have compatible types", rule.operator.getName());

                CalciteContextException e = assertThrows(CalciteContextException.class, () -> planner.validate(node));
                assertThat(e.getMessage(), containsString(error));
            });
        }
    }

    private void runBinaryOpTypeCoercionTest(TypeCoercionRule rule, BiConsumer<IgnitePlanner, SqlNode> testCase) {
        RelDataType tableType = new RelDataTypeFactory.Builder(TYPE_FACTORY)
                .add("C1", rule.lhs)
                .add("C2", rule.rhs)
                .build();

        TestTable testTable = new TestTable("A", tableType, IgniteDistributions.single());

        String dummyQuery = String.format("SELECT c1 %s c2 FROM A", rule.operator.getName());
        runTest(dummyQuery, testCase, createSchema(testTable));
    }

    private void runTest(String query, BiConsumer<IgnitePlanner, SqlNode> testCase) {
        runTest(query, testCase, createSchema());
    }

    private void runTest(String query, BiConsumer<IgnitePlanner, SqlNode> testCase, IgniteSchema schema) {
        PlanningContext planningCtx = plannerCtx(query, schema);

        try (IgnitePlanner planner = planningCtx.planner()) {
            SqlNode node;
            try {
                node = planner.parse(query);
            } catch (SqlParseException e) {
                throw new IllegalStateException("Unable to parse a query: " + query, e);
            }

            testCase.accept(planner, node);
        }
    }

    private static void checkBinaryOpTypeCoercionResult(SqlCall sqlCall, RelDataType lhs, RelDataType rhs,
            SqlOperator operator, boolean coerced, TypeCoercionRuleType ruleType) {

        String originalExpression = sqlCall.toString();
        RelDataType dataType = ruleType.coerceTypes(lhs, rhs);
        RelDataType newLhs = dataType != null && !lhs.equals(dataType) ? dataType : null;
        RelDataType newRhs = dataType != null && !rhs.equals(dataType) ? dataType : null;

        if (newLhs == null && newRhs == null) {
            assertFalse(coerced, "should not have been coerced. Expr: " + sqlCall);
            assertEquals(originalExpression, sqlCall.toString(),
                    "Expression has been modified although type coercion has not been performed");
        } else {
            String expectedExpr;
            if (newLhs != null && newRhs == null) {
                // add cast to the left hand side of the expression
                expectedExpr = String.format("CAST(`A`.`C1` AS %s) %s `A`.`C2`", newLhs, operator.getName());
                assertTrue(coerced, "should have been coerced. Expr: " + sqlCall);
            } else if (newLhs == null) {
                // add cast to the right hand side of the expression
                expectedExpr = String.format("`A`.`C1` %s CAST(`A`.`C2` AS %s)", operator, newRhs);
                assertTrue(coerced, "should have been coerced. Expr: " + sqlCall);
                assertEquals(expectedExpr, sqlCall.toString());
            } else {
                // adds cast to both sides of the expression
                expectedExpr = String.format("CAST(`A`.`C1` AS %s) %s CAST(`A`.`C2` AS %s)", newLhs, operator.getName(), newRhs);
                assertEquals(expectedExpr, sqlCall.toString());
            }

            assertTrue(coerced, "should have been coerced. Expr: " + sqlCall);
            assertEquals(expectedExpr, sqlCall.toString(), "expression with casts");
        }
    }

    private void checkExprResult(String expr, RelDataType expectedType) {
        IgniteSchema igniteSchema = createSchema();
        String query = "SELECT " + expr;
        PlanningContext planningCtx = plannerCtx(query, igniteSchema);

        try (IgnitePlanner planner = planningCtx.planner()) {
            SqlNode sqlNode;
            try {
                sqlNode = planner.parse(query);
            } catch (SqlParseException e) {
                throw new IllegalStateException("Unable to parse a query: " + query, e);
            }
            sqlNode = planner.validate(sqlNode);

            RelDataType actualType = planner.validator().getValidatedNodeType(sqlNode);
            RelDataTypeField firstField = actualType.getFieldList().get(0);
            RelDataType firstFieldType = firstField.getType();

            assertEquals(expectedType, firstFieldType, sqlNode.toString());
        }
    }

    private void checkExprResultFails(String expr, String errorMessage) {
        IgniteSchema igniteSchema = createSchema();
        String query = "SELECT " + expr;
        PlanningContext planningCtx = plannerCtx(query, igniteSchema);

        try (IgnitePlanner planner = planningCtx.planner()) {
            SqlNode sqlNode;
            try {
                sqlNode = planner.parse(query);
            } catch (SqlParseException e) {
                throw new IllegalStateException("Unable to parse a query: " + query, e);
            }

            var err = assertThrows(CalciteContextException.class, () -> planner.validate(sqlNode));
            assertThat(err.getMessage(), containsString(errorMessage));
        }
    }

    /** Type coercion between the given types behaves according the specified {@link TypeCoercionRuleType rule type}. **/
    private static TypeCoercionRule typeCoercionRule(RelDataType lhs, RelDataType rhs, TypeCoercionRuleType typeCoercion) {
        return new TypeCoercionRule(lhs, rhs, SqlStdOperatorTable.EQUALS, typeCoercion);
    }

    /** Type coercion between the given types is not supported and we must throw an exception. **/
    private static TypeCoercionRule typeCoercionIsNotSupported(RelDataType lhs, RelDataType rhs) {
        return new TypeCoercionRule(lhs, rhs, SqlStdOperatorTable.EQUALS, null);
    }

    private static final class TypeCoercionRule {
        final RelDataType lhs;

        final RelDataType rhs;

        final SqlOperator operator;

        final TypeCoercionRuleType type;

        TypeCoercionRule(RelDataType type1, RelDataType type2, SqlOperator operator, @Nullable TypeCoercionRuleType type) {
            this.lhs = type1;
            this.rhs = type2;
            this.operator = operator;
            this.type = type;
        }

        @Override
        public String toString() {
            return lhs + " " + operator.getName() + " " + rhs + " rule: " + type;
        }
    }

    abstract static class TypeCoercionRuleType {
        @Nullable
        abstract RelDataType coerceTypes(RelDataType type1, RelDataType type2);
    }

    /**
     * No casts are added to neither operand.
     */
    static final class NoTypeCoercion extends TypeCoercionRuleType {

        @Override
        @Nullable RelDataType coerceTypes(RelDataType type1, RelDataType type2) {
            return null;
        }

        @Override
        public String toString() {
            return S.toString(NoTypeCoercion.class, this);
        }
    }

    /**
     * Types are coerced to {@link RelDataTypeFactory#leastRestrictive(List) the least restrictive type}.
     */
    static final class ToLeastRestrictiveType extends TypeCoercionRuleType {

        @Override
        @Nullable RelDataType coerceTypes(RelDataType type1, RelDataType type2) {
            return TYPE_FACTORY.leastRestrictive(Arrays.asList(type1, type2));
        }

        @Override
        public String toString() {
            return S.toString(ToLeastRestrictiveType.class, this);
        }
    }

    /**
     * Operands are casted to the specified type.
     */
    static final class ToSpecificType extends TypeCoercionRuleType {

        private final RelDataType type;

        ToSpecificType(RelDataType type) {
            this.type = type;
        }

        @Override
        @Nullable RelDataType coerceTypes(RelDataType type1, RelDataType type2) {
            return type;
        }

        @Override
        public String toString() {
            return S.toString(ToSpecificType.class, this);
        }
    }

    private static RelDataType nullable(RelDataType relDataType) {
        return TYPE_FACTORY.createTypeWithNullability(relDataType, true);
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-15200 Replace with TestTable from test framework.
    // TODO: https://issues.apache.org/jira/browse/IGNITE-17373: Remove when INTERVAL type will be supported natively,
    // or this issue is resolved.

    /** Test table. */
    @Deprecated
    private static class TestTable implements IgniteTable {
        private final String name;

        private final RelProtoDataType protoType;

        private final int id = nextTableId();
        private final IgniteDistribution distribution;

        /** Constructor. */
        private TestTable(String name, RelDataType type, IgniteDistribution distribution) {
            protoType = RelDataTypeImpl.proto(type);
            this.name = name;
            this.distribution = distribution;
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public int version() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet bitSet) {
            RelDataType rowType = protoType.apply(typeFactory);

            if (bitSet != null) {
                RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);
                for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1)) {
                    b.add(rowType.getFieldList().get(i));
                }
                rowType = b.build();
            }

            return rowType;
        }

        @Override
        public TableScan toRel(ToRelContext context, RelOptTable relOptTable) {
            RelOptCluster cluster = context.getCluster();
            List<RelHint> hints = context.getTableHints();

            return IgniteLogicalTableScan.create(cluster, cluster.traitSet(), hints, relOptTable, null, null, null);
        }

        @Override
        public Statistic getStatistic() {
            return new TestStatistic(100.0);
        }

        @Override
        public Schema.TableType getJdbcTableType() {
            throw new AssertionError();
        }

        @Override
        public boolean isRolledUp(String col) {
            return false;
        }

        @Override
        public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
            throw new AssertionError();
        }

        @Override
        public IgniteDistribution distribution() {
            return distribution;
        }

        @Override
        public TableDescriptor descriptor() {
            throw new AssertionError();
        }

        @Override
        public Supplier<PartitionCalculator> partitionCalculator() {
            return null;
        }

        @Override
        public Map<String, IgniteIndex> indexes() {
            return Map.of();
        }

        @Override
        public int partitions() {
            return 1;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public <C> @Nullable C unwrap(Class<C> cls) {
            if (cls.isInstance(this)) {
                return cls.cast(this);
            }
            return null;
        }

        @Override
        public boolean isUpdateAllowed(int colIdx) {
            return false;
        }

        @Override
        public ImmutableIntList keyColumns() {
            throw new AssertionError();
        }

        @Override
        public RelDataType rowTypeForInsert(IgniteTypeFactory factory) {
            return protoType.apply(factory);
        }

        @Override
        public RelDataType rowTypeForUpdate(IgniteTypeFactory factory) {
            return protoType.apply(factory);
        }

        @Override
        public RelDataType rowTypeForDelete(IgniteTypeFactory factory) {
            throw new AssertionError();
        }
    }
}
