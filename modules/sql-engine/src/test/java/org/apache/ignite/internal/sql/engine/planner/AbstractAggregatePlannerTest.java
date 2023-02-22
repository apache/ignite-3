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

import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.junit.jupiter.api.Test;

/**
 * Base class for further planner test implementations.
 */
public abstract class AbstractAggregatePlannerTest extends AbstractPlannerTest {

    private static final Predicate<AggregateCall> NON_NULL_PREDICATE = Objects::nonNull;

    /**
     * Validates a plan for simple query with aggregate.
     */
    @Test
    public void simpleAggregate() throws Exception {
        checkTestCase1("SELECT AVG(val0) FROM test", schema(single()));
        checkTestCase2("SELECT AVG(val0) FROM test", schema(hash()));
    }

    /**
     * Validates a plan for a query with aggregate and groups.
     */
    @Test
    public void groupNoIndex() throws Exception {
        checkTestCase3("SELECT AVG(val0) FROM test GROUP BY grp0", schema(single()));
        checkTestCase4("SELECT AVG(val0) FROM test GROUP BY grp0", schema(hash()));

        checkTestCase3("SELECT AVG(val0) FROM test GROUP BY grp1, grp0", schema(single()));
        checkTestCase4("SELECT AVG(val0) FROM test GROUP BY grp1, grp0", schema(hash()));
    }

    /**
     * Validates a plan uses an index for a query with aggregate and group by first index column.
     */
    @Test
    public void groupWithIndex() throws Exception {
        checkTestCase5("SELECT AVG(val0) FROM test GROUP BY grp0", schema(single(), index("grp0_grp1", 3, 4)));
        checkTestCase6("SELECT AVG(val0) FROM test GROUP BY grp0", schema(hash(), index("grp0_grp1", 3, 4)));
    }

    /**
     * Validates a plan uses an index for a query with aggregate and group by index columns in different order.
     */
    @Test
    public void groupWithIndexCollationPermute() throws Exception {
        checkTestCase5("SELECT AVG(val0) FROM test GROUP BY grp0, grp1", schema(single(), index("grp0_grp1", 3, 4)));
        checkTestCase5("SELECT AVG(val0) FROM test GROUP BY grp1, grp0", schema(single(), index("grp0_grp1", 3, 4)));

        checkTestCase6("SELECT AVG(val0) FROM test GROUP BY grp0, grp0", schema(hash(), index("grp0_grp1", 3, 4)));

        //TODO:https://issues.apache.org/jira/browse/IGNITE-18871 Index should be used here.
        // Assumptions.assumeFalse(this instanceof ColocatedSortAggregatePlannerTest, "wrong plan");
        checkTestCase6("SELECT AVG(val0) FROM test GROUP BY grp1, grp0", schema(hash(), index("grp0_grp1", 3, 4)));
    }

    /**
     * Validates a plan for simple query with DISTINCT aggregates.
     */
    @Test
    public void distinctAggregate() throws Exception {
        IgniteSchema schema = schema(single());

        checkTestCase7("SELECT COUNT(DISTINCT val0) FROM test", schema);
        checkTestCase7("SELECT AVG(DISTINCT val0) FROM test", schema);
        checkTestCase7("SELECT SUM(DISTINCT val0) FROM test", schema);

        // DISTINCT is ignored for MIN and MAX aggregates.
        checkTestCase1("SELECT MIN(DISTINCT val0) FROM test", schema);
        checkTestCase1("SELECT MAX(DISTINCT val0) FROM test", schema);

        schema = schema(hash());

        checkTestCase8("SELECT COUNT(DISTINCT val0) FROM test", schema);
        checkTestCase8("SELECT AVG(DISTINCT val0) FROM test", schema);
        checkTestCase8("SELECT SUM(DISTINCT val0) FROM test", schema);

        // DISTINCT is ignored for MIN and MAX aggregates.
        checkTestCase2("SELECT MIN(DISTINCT val0) FROM test", schema);
        checkTestCase2("SELECT MAX(DISTINCT val0) FROM test", schema);
    }

    /**
     * Validates a plan for a query with DISTINCT and without aggregate functions.
     */
    @Test
    public void groupWithoutAggregate() throws Exception {
        checkTestCase9("SELECT DISTINCT val0, val1 FROM test", schema(single(), index("val0", 1)));
        checkTestCase10("SELECT DISTINCT val0, val1 FROM test", schema(hash(), index("val0", 1)));
    }

    /**
     * Validates a plan uses index for a query with DISTINCT and without functions.
     */
    @Test
    public void groupWithoutAggregateFunctionWithIndex() throws Exception {
        checkTestCase11("SELECT DISTINCT grp0, grp1 FROM test", schema(single(), index("grp0_grp1", 3, 4)));
        checkTestCase12("SELECT DISTINCT grp0, grp1 FROM test", schema(hash(), index("grp0_grp1", 3, 4)));
    }

    /**
     * Validates a plan for a query with DISTINCT aggregates and groups.
     */
    @Test
    public void distinctAggregateWithGroups() throws Exception {
        IgniteSchema schema = schema(single());

        checkTestCase13("SELECT COUNT(DISTINCT val0) FROM test GROUP BY val1, grp0", schema);
        checkTestCase13("SELECT val1, COUNT(DISTINCT val0) as v1 FROM test GROUP BY val1", schema);
        checkTestCase13("SELECT AVG(DISTINCT val0) FROM test GROUP BY val1", schema);
        checkTestCase13("SELECT SUM(DISTINCT val0) FROM test GROUP BY val1", schema);

        // DISTINCT is ignored for MIN and MAX aggregates.
        checkTestCase3("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema);
        checkTestCase3("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema);

        schema = schema(hash());

        checkTestCase14("SELECT COUNT(DISTINCT val0) FROM test GROUP BY val1, grp0", schema);
        checkTestCase14("SELECT val1, COUNT(DISTINCT val0) as v1 FROM test GROUP BY val1", schema);
        checkTestCase14("SELECT AVG(DISTINCT val0) FROM test GROUP BY val1", schema);
        checkTestCase14("SELECT SUM(DISTINCT val0) FROM test GROUP BY val1", schema);

        // DISTINCT is ignored for MIN and MAX aggregates.
        checkTestCase4("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema);
        checkTestCase4("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema);
    }

    /**
     * Validates a plan for a query with aggregate in WHERE clause.
     */
    @Test
    public void aggregateInWhereClause() throws Exception {
        checkTestCase1("SELECT val0 FROM test WHERE VAL1 = (SELECT AVG(val1) FROM test)", schema(single()));
        checkTestCase2("SELECT val0 FROM test WHERE VAL1 = (SELECT AVG(val1) FROM test)", schema(hash()));
    }

    /**
     * Validates a plan for a query with DISTINCT aggregate in WHERE clause.
     */
    @Test
    public void distinctAggregateInWhereClause() throws Exception {
        checkTestCase9("SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test)", schema(single()));
        checkTestCase25("SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test)", schema(hash()));
    }

    /**
     * Validates a plan with merge-sort utilizes index if collation fits.
     */
    @Test
    public void noSortAppendingWithCorrectCollation() throws Exception {
        checkTestCase15(
                "SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)",
                schema(single(), indexByVal0Desc()),
                "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "CorrelateToNestedLoopRule"
        );

        checkTestCase16(
                "SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)",
                schema(hash(), indexByVal0Desc()),
                "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "CorrelateToNestedLoopRule"
        );
    }

    /**
     * Validates SUM aggregate has a correct return type for any numeric column type.
     */
    @Test
    public void sumAggregateTypes() throws Exception {
        checkSumAggretageTypes(single());
        checkSumAggretageTypes(hash());
    }

    //TODO: should we add test with indexes?
    /**
     * Validates a plan for a query with aggregate and with grouping and sorting by the same column set.
     */
    @Test
    public void groupsWithOrderByGroupColumns() throws Exception {
        // Sort order equals to grouping set.
        checkTestCase17("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1", schema(single()),
                TraitUtils.createCollation(List.of(0, 1)));
        checkTestCase18("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1", schema(hash()),
                TraitUtils.createCollation(List.of(0, 1)));

        // Sort order equals to grouping set (permuted collation).
        checkTestCase17("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0", schema(single()),
                TraitUtils.createCollation(List.of(1, 0)));
        checkTestCase18("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0", schema(hash()),
                TraitUtils.createCollation(List.of(1, 0)));
    }

    /**
     * Validates a plan for a query with aggregate and with sorting by subset of group columns.
     */
    @Test
    public void groupsWithOrderBySubsetOfGroupColumns() throws Exception {
        // Sort order is a subset of grouping set.
        checkTestCase17("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0", schema(single()),
                TraitUtils.createCollation(List.of(0, 1)));
        checkTestCase18("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0", schema(hash()),
                TraitUtils.createCollation(List.of(0, 1)));

        // Sort order is a subset of grouping set (permuted collation).
        checkTestCase17("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1", schema(single()),
                TraitUtils.createCollation(List.of(1, 0)));
        checkTestCase18("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1", schema(hash()),
                TraitUtils.createCollation(List.of(1, 0)));
    }

    /**
     * Validates a plan for a query with aggregate, sorting and grouping, when additional sort is required.
     */
    @Test
    public void groupWithOrderByDifferentColumns() throws Exception {
        // Sort order is a superset of grouping set (additional sorting required).
        checkTestCase19("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt", schema(single()),
                TraitUtils.createCollation(List.of(0, 1, 2)));

        // Sort order is not equals to grouping set (additional sorting required).
        checkTestCase20("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY cnt, val1", schema(hash()),
                TraitUtils.createCollation(List.of(2, 1)));
    }

    /**
     * Validates a plan for a query with aggregate, sorting and grouping, when additional sort is required.
     */
    @Test
    public void emptyCollationPassThroughLimit() throws Exception {
        checkTestCase21("SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 1) FROM test", schema(single()));
        checkTestCase22("SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 1) FROM test", schema(hash()));
    }

    /**
     * Validates a plan for a query with EXPAND_DISTINCT_AGG hint.
     */
    @Test
    public void expandDistinctAggregates() throws Exception {
        checkTestCase23("SELECT /*+ EXPAND_DISTINCT_AGG */ SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0",
                schema(single(), index("idx_val0", 3, 1), index("idx_val1", 3, 2)));

        checkTestCase24("SELECT /*+ EXPAND_DISTINCT_AGG */ SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0",
                schema(hash(), index("idx_val0", 3, 1), index("idx_val1", 3, 2)));
    }

    private void checkSumAggretageTypes(IgniteDistribution distribution) throws Exception {
        org.apache.ignite.internal.sql.engine.framework.TestTable table = TestBuilders.table()
                .name("TEST")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("GRP", NativeTypes.INT32)
                .addColumn("VAL_TINYINT", NativeTypes.INT8)
                .addColumn("VAL_SMALLINT", NativeTypes.INT16)
                .addColumn("VAL_INT", NativeTypes.INT32)
                .addColumn("VAL_BIGINT", NativeTypes.INT64)
                .addColumn("VAL_DECIMAL", NativeTypes.decimalOf(DecimalNativeType.DEFAULT_PRECISION, DecimalNativeType.DEFAULT_SCALE))
                .addColumn("VAL_FLOAT", NativeTypes.FLOAT)
                .addColumn("VAL_DOUBLE", NativeTypes.DOUBLE)
                .size(DEFAULT_TBL_SIZE)
                .distribution(distribution)
                .build();

        IgniteSchema schema = new IgniteSchema("PUBLIC");
        schema.addTable(table);

        List<RelDataType> types0 = List.of(
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL,
                        TYPE_FACTORY.getTypeSystem().getMaxNumericPrecision(), 0), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL,
                        TYPE_FACTORY.getTypeSystem().getMaxNumericPrecision(), DecimalNativeType.DEFAULT_SCALE), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), true),
                TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), true));

        assertPlan("SELECT SUM(VAL_TINYINT), SUM(VAL_SMALLINT), SUM(VAL_INT), SUM(VAL_BIGINT), SUM(VAL_DECIMAL), SUM(VAL_FLOAT), "
                        + "SUM(VAL_DOUBLE) FROM test GROUP BY grp",
                schema,
                nodeOrAnyChild(isInstanceOf(SingleRel.class))
                        .and(n -> {
                            List<RelDataType> types = n.getRowType().getFieldList().stream()
                                    .map(RelDataTypeField::getType)
                                    .collect(Collectors.toList());

                            return types.equals(types0);
                        })
        );
    }

    protected abstract void checkTestCase1(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase2(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase3(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase4(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase5(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase6(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase7(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase8(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase9(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase10(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase11(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase12(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase13(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase14(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase15(String sql, IgniteSchema schema, String... additionalRules) throws Exception;

    protected abstract void checkTestCase16(String sql, IgniteSchema schema, String... additionalRules) throws Exception;

    protected abstract void checkTestCase17(String sql, IgniteSchema schema, RelCollation collation) throws Exception;

    protected abstract void checkTestCase18(String sql, IgniteSchema schema, RelCollation collation) throws Exception;

    protected abstract void checkTestCase19(String sql, IgniteSchema schema, RelCollation collation) throws Exception;

    protected abstract void checkTestCase20(String sql, IgniteSchema schema, RelCollation collation) throws Exception;

    protected abstract void checkTestCase21(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase22(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase23(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase24(String sql, IgniteSchema schema) throws Exception;

    protected abstract void checkTestCase25(String sql, IgniteSchema schema) throws Exception;

    /**
     * Rules to disable.
     *
     * @return Rules.
     */
    protected abstract String[] disabledRules();

    protected <T extends RelNode> void assertPlan(
            String sql,
            IgniteSchema schema,
            Predicate<T> predicate
    ) throws Exception {
        assertPlan(sql, Collections.singleton(schema), predicate, List.of(), disabledRules());
    }

    static IgniteSchema schema(IgniteDistribution distribution,
            Consumer<org.apache.ignite.internal.sql.engine.framework.TestTable>... indices) {
        org.apache.ignite.internal.sql.engine.framework.TestTable table = TestBuilders.table()
                .name("TEST")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("VAL0", NativeTypes.INT32)
                .addColumn("VAL1", NativeTypes.INT32)
                .addColumn("GRP0", NativeTypes.INT32)
                .addColumn("GRP1", NativeTypes.INT32)
                .size(DEFAULT_TBL_SIZE)
                .distribution(distribution)
                .build();

        for (Consumer<org.apache.ignite.internal.sql.engine.framework.TestTable> index : indices) {
            index.accept(table);
        }

        IgniteSchema schema = new IgniteSchema("PUBLIC");
        schema.addTable(table);

        return schema;
    }

    private static IgniteDistribution hash() {
        return IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID);
    }

    private static Consumer<org.apache.ignite.internal.sql.engine.framework.TestTable> index(String name, int... cols) {
        return tbl -> tbl.addIndex(createIndex(tbl, name, cols));
    }

    private static Consumer<org.apache.ignite.internal.sql.engine.framework.TestTable> indexByVal0Desc() {
        RelFieldCollation coll = new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING);

        return tbl -> tbl.addIndex(createIndex(tbl, "val0", RelCollations.of(coll)));
    }

    private static IgniteIndex createIndex(org.apache.ignite.internal.sql.engine.framework.TestTable tbl, String name, int... keys) {
        return createIndex(tbl, name, TraitUtils.createCollation(IntStream.of(keys).boxed().collect(Collectors.toList())));
    }

    private static IgniteIndex createIndex(org.apache.ignite.internal.sql.engine.framework.TestTable tbl, String name,
            RelCollation collation) {
        return new IgniteIndex(TestSortedIndex.create(collation, name, tbl));
    }

    <T extends RelNode> Predicate<T> hasAggregate() {
        Predicate<T> mapNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class)
                .and(n -> n.getAggCallList().stream().anyMatch(NON_NULL_PREDICATE));
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class)
                .and(n -> n.getAggregateCalls().stream().anyMatch(NON_NULL_PREDICATE));

        return mapNode.or(reduceNode);
    }

    <T extends RelNode> Predicate<T> hasDistinctAggregate() {
        Predicate<T> mapNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class)
                .and(n -> n.getAggCallList().stream().anyMatch(NON_NULL_PREDICATE.and(AggregateCall::isDistinct)));
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class)
                .and(n -> n.getAggregateCalls().stream().anyMatch(NON_NULL_PREDICATE.and(AggregateCall::isDistinct)));

        return mapNode.or(reduceNode);
    }

    <T extends RelNode> Predicate<T> hasGroups() {
        Predicate<T> aggregateNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class).and(n -> !n.getGroupSets().isEmpty());
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class).and(n -> !n.getGroupSets().isEmpty());

        return aggregateNode.or(reduceNode);
    }
}
