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

import static org.apache.ignite.internal.util.ArrayUtils.concat;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.ArrayUtils;
import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * AggregatePlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class AggregatePlannerTest extends AbstractAggregatePlannerTest {
    /**
     * SingleWithoutIndex.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void singleWithoutIndex(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createBroadcastTable("TEST").addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT AVG(val0) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );

        IgniteColocatedAggregateBase agg = findFirstNode(phys, byClass(algo.colocated));

        assertNotNull(agg, invalidPlanErrorMessage(phys));

        assertThat(
                invalidPlanErrorMessage(phys).get(),
                first(agg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT) {
            assertNotNull(findFirstNode(phys, byClass(IgniteSort.class)));
        }
    }

    /**
     * SingleWithIndex.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void singleWithIndex(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createBroadcastTable("TEST").addIndex("grp0_grp1", 3, 4);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT AVG(val0) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );

        IgniteColocatedAggregateBase agg = findFirstNode(phys, byClass(algo.colocated));

        assertNotNull(agg, invalidPlanErrorMessage(phys));

        assertThat(invalidPlanErrorMessage(phys).get(),
                first(agg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT) {
            assertNotNull(findFirstNode(phys, byClass(IgniteIndexScan.class)));
        }
    }

    @ParameterizedTest
    @EnumSource(AggregateAlgorithm.class)
    public void colocated(AggregateAlgorithm algo) throws Exception {
        IgniteSchema schema = createSchema(
                createTable(
                        "EMP", IgniteDistributions.affinity(1, UUID.randomUUID(), DEFAULT_ZONE_ID),
                        "EMPID", Integer.class,
                        "DEPTID", Integer.class,
                        "NAME", String.class,
                        "SALARY", Integer.class
                ).addIndex("DEPTID", 1),
                createTable(
                        "DEPT", IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID),
                        "DEPTID", Integer.class,
                        "NAME", String.class
                ).addIndex("DEPTID", 0)
        );

        String sql = "SELECT SUM(SALARY) FROM emp GROUP BY deptid";

        assertPlan(sql, schema, hasChildThat(isInstanceOf(algo.colocated)
                        .and(hasDistribution(IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID)))),
                algo.rulesToDisable);

        sql = "SELECT dept.deptid, agg.cnt "
                + "FROM dept "
                + "JOIN (SELECT deptid, COUNT(*) AS cnt FROM emp GROUP BY deptid) AS agg ON dept.deptid = agg.deptid";

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
                        .and(input(0, hasDistribution(IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID))))
                        .and(input(1, hasDistribution(IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID))))),
                algo.rulesToDisable);
    }

    /**
     * Test that aggregate has single distribution output even if parent node accept random distribution inputs.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void distribution(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createAffinityTable("TEST").addIndex("grp0", 3);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT AVG(val0), grp0 FROM test GROUP BY grp0 UNION ALL SELECT val0, grp0 FROM test";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                concat(algo.rulesToDisable, "MapReduceSortAggregateConverterRule",
                        "MapReduceHashAggregateConverterRule")
        );

        IgniteColocatedAggregateBase singleAgg = findFirstNode(phys, byClass(algo.colocated));

        assertEquals(IgniteDistributions.single(), TraitUtils.distribution(singleAgg));

        phys = physicalPlan(
                sql,
                publicSchema,
                concat(algo.rulesToDisable)
        );

        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertEquals(IgniteDistributions.single(), TraitUtils.distribution(rdcAgg));
    }

    /**
     * CollationPermuteSingle.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void collationPermuteSingle() throws Exception {
        TestTable tbl = createAffinityTable("TEST").addIndex("grp0_1", 3, 4);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT MIN(val0) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                AggregateAlgorithm.SORT.rulesToDisable
        );

        IgniteColocatedSortAggregate agg = findFirstNode(phys, byClass(IgniteColocatedSortAggregate.class));

        assertNotNull(agg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertNull(
                findFirstNode(phys, byClass(IgniteSort.class)),
                "Invalid plan\n" + RelOptUtil.toString(phys)
        );
    }

    /**
     * ExpandDistinctAggregates.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void expandDistinctAggregates(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createAffinityTable("TEST")
                .addIndex("idx_val0", 3, 1, 0)
                .addIndex("idx_val1", 3, 2, 0);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT "
                + "/*+ EXPAND_DISTINCT_AGG */ "
                + "SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable);

        // Plan must not contain distinct accumulators.
        assertFalse(
                findNodes(phys, byClass(IgniteAggregate.class)).stream()
                        .anyMatch(n -> ((Aggregate) n).getAggCallList().stream()
                                .anyMatch(AggregateCall::isDistinct)
                        ),
                invalidPlanErrorMessage(phys)
        );

        assertNotNull(
                findFirstNode(phys, byClass(Join.class)),
                "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES)
        );

        // Check the first aggregation step is SELECT DISTINCT (doesn't contain any accumulators)
        assertTrue(
                findNodes(phys, byClass(algo.reduce)).stream()
                        .allMatch(n -> ((IgniteReduceAggregateBase) n).getAggregateCalls().isEmpty()),
                invalidPlanErrorMessage(phys)
        );

        assertTrue(
                findNodes(phys, byClass(algo.map)).stream()
                        .allMatch(n -> ((Aggregate) n).getAggCallList().isEmpty()),
                invalidPlanErrorMessage(phys)
        );

        // Check the second aggregation step contains accumulators.
        assertTrue(
                findNodes(phys, byClass(algo.colocated)).stream()
                        .noneMatch(n -> ((Aggregate) n).getAggCallList().isEmpty()),
                invalidPlanErrorMessage(phys)
        );
    }

    /**
     * MapReduceGroupBy.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void mapReduceGroupBy(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createAffinityTable("TEST");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT AVG(val0) FILTER (WHERE val1 > 10) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );

        IgniteMapAggregateBase mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull(rdcAgg, invalidPlanErrorMessage(phys));
        assertNotNull(mapAgg, invalidPlanErrorMessage(phys));

        assertThat(invalidPlanErrorMessage(phys).get(),
                first(rdcAgg.getAggregateCalls()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        assertThat(invalidPlanErrorMessage(phys).get(),
                first(mapAgg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT) {
            assertNotNull(findFirstNode(phys, byClass(IgniteSort.class)));
        }
    }

    /**
     * Check that map aggregate does not contain distinct accumulator and can be planned at all.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void mapAggregateWithoutDistinctAcc(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createAffinityTable("TEST");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        checkDistinctInMapAggNode("SELECT COUNT(*) FROM test", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT COUNT(DISTINCT val0) FROM test", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT AVG(DISTINCT val0) FROM test", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT SUM(DISTINCT val0) FROM test", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT MIN(DISTINCT val0) FROM test", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT MAX(DISTINCT val0) FROM test", publicSchema, algo);

        checkDistinctInMapAggNode("SELECT COUNT(DISTINCT val0) FROM test GROUP BY val1, grp0", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT val1, COUNT(DISTINCT val0) as v1 FROM test GROUP BY val1", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT AVG(DISTINCT val0) FROM test GROUP BY val1", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT SUM(DISTINCT val0) FROM test GROUP BY val1", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", publicSchema, algo);
        checkDistinctInMapAggNode("SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test)", publicSchema, algo);
    }

    @ParameterizedTest
    @MethodSource("provideAlgoAndDistribution")
    public void singleSumTypes(AggregateAlgorithm algo, IgniteDistribution distr) throws Exception {
        IgniteSchema schema = createSchema(
                createTable(
                        "TEST", distr,
                        "ID", Integer.class,
                        "GRP", Integer.class,
                        "VAL_TINYINT", Byte.class,
                        "VAL_SMALLINT", Short.class,
                        "VAL_INT", Integer.class,
                        "VAL_BIGINT", Long.class,
                        "VAL_DECIMAL", BigDecimal.class,
                        "VAL_FLOAT", Float.class,
                        "VAL_DOUBLE", Double.class
                )
        );

        String sql = "SELECT "
                + "SUM(VAL_TINYINT), "
                + "SUM(VAL_SMALLINT), "
                + "SUM(VAL_INT), "
                + "SUM(VAL_BIGINT), "
                + "SUM(VAL_DECIMAL), "
                + "SUM(VAL_FLOAT), "
                + "SUM(VAL_DOUBLE) "
                + "FROM test GROUP BY grp";

        IgniteRel phys = physicalPlan(
                sql,
                schema,
                algo.rulesToDisable
        );

        checkSplitAndSerialization(phys, schema);

        Class<? extends SingleRel> cls = distr == IgniteDistributions.broadcast() ? algo.colocated : algo.reduce;

        SingleRel agg = findFirstNode(phys, byClass(cls));

        assertNotNull(agg, invalidPlanErrorMessage(phys));

        RelDataType rowTypes = agg.getRowType();

        RelDataTypeFactory tf = phys.getCluster().getTypeFactory();

        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(1).getType());
        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(2).getType());
        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(3).getType());
        assertEquals(tf.createJavaType(BigDecimal.class), rowTypes.getFieldList().get(4).getType());
        assertEquals(tf.createJavaType(BigDecimal.class), rowTypes.getFieldList().get(5).getType());
        assertEquals(tf.createJavaType(Double.class), rowTypes.getFieldList().get(6).getType());
        assertEquals(tf.createJavaType(Double.class), rowTypes.getFieldList().get(7).getType());
    }


    /**
     * CollationPermuteMapReduce.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void collationPermuteMapReduce() throws Exception {
        TestTable tbl = createAffinityTable("TEST").addIndex("grp0_1", 3, 4);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT MIN(val0) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                AggregateAlgorithm.SORT.rulesToDisable
        );

        IgniteReduceSortAggregate agg = findFirstNode(phys, byClass(IgniteReduceSortAggregate.class));

        assertNotNull(agg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertNull(
                findFirstNode(phys, byClass(IgniteSort.class)),
                "Invalid plan\n" + RelOptUtil.toString(phys)
        );
    }

    @Test
    public void testEmptyCollationPassThroughLimit() throws Exception {
        IgniteSchema publicSchema = createSchema(
                createTable("TEST", IgniteDistributions.single(), "A", Integer.class));

        assertPlan("SELECT (SELECT test.a FROM test t ORDER BY 1 LIMIT 1) FROM test", publicSchema,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, hasChildThat(isInstanceOf(IgniteLimit.class)
                                .and(input(isInstanceOf(IgniteSort.class)))))))
        );
    }

    @Test
    public void testCollationPassThrough() throws Exception {
        IgniteSchema publicSchema = createSchema(
                createTable("TEST", IgniteDistributions.single(), "A", Integer.class, "B", Integer.class));

        // Sort order equals to grouping set.
        assertPlan("SELECT a, b, COUNT(*) FROM test GROUP BY a, b ORDER BY a, b", publicSchema,
                isInstanceOf(IgniteAggregate.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                                .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                .and(input(isTableScan("TEST"))))),
                AggregateAlgorithm.SORT.rulesToDisable
        );

        // Sort order equals to grouping set (permuted collation).
        assertPlan("SELECT a, b, COUNT(*) FROM test GROUP BY a, b ORDER BY b, a", publicSchema,
                isInstanceOf(IgniteAggregate.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                                .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(1, 0))))
                                .and(input(isTableScan("TEST"))))),
                AggregateAlgorithm.SORT.rulesToDisable
        );

        // Sort order is a subset of grouping set.
        assertPlan("SELECT a, b, COUNT(*) cnt FROM test GROUP BY a, b ORDER BY a", publicSchema,
                isInstanceOf(IgniteAggregate.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                                .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                .and(input(isTableScan("TEST"))))),
                AggregateAlgorithm.SORT.rulesToDisable
        );

        // Sort order is a subset of grouping set (permuted collation).
        assertPlan("SELECT a, b, COUNT(*) cnt FROM test GROUP BY a, b ORDER BY b", publicSchema,
                isInstanceOf(IgniteAggregate.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                                .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(1, 0))))
                                .and(input(isTableScan("TEST"))))),
                AggregateAlgorithm.SORT.rulesToDisable
        );

        // Sort order is a superset of grouping set (additional sorting required).
        assertPlan("SELECT a, b, COUNT(*) cnt FROM test GROUP BY a, b ORDER BY a, b, cnt", publicSchema,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1, 2))))
                        .and(input(isInstanceOf(IgniteAggregate.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                        .and(input(isTableScan("TEST"))))))),
                AggregateAlgorithm.SORT.rulesToDisable
        );

        // Sort order is not equals to grouping set (additional sorting required).
        assertPlan("SELECT a, b, COUNT(*) cnt FROM test GROUP BY a, b ORDER BY cnt, b", publicSchema,
                isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(2, 1))))
                        .and(input(isInstanceOf(IgniteAggregate.class)
                                .and(input(isInstanceOf(IgniteSort.class)
                                        .and(s -> s.collation().equals(TraitUtils.createCollation(List.of(0, 1))))
                                        .and(input(isTableScan("TEST"))))))),
                AggregateAlgorithm.SORT.rulesToDisable
        );
    }

    /**
     * Test simple query with aggregate and no groups.
     */
    @ParameterizedTest
    @EnumSource
    public void noGroupByAggregate(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createAffinityTable("TEST").addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sqlCount = "SELECT COUNT(*) FROM test";

        IgniteRel phys = physicalPlan(
                sqlCount,
                publicSchema,
                algo.rulesToDisable
        );
        assertNotNull(phys);

        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));
        IgniteMapAggregateBase mapAgg = findFirstNode(phys, byClass(algo.map));

        assertNotNull(rdcAgg, invalidPlanErrorMessage(phys));
        assertNotNull(mapAgg, invalidPlanErrorMessage(phys));

        assertThat(
                invalidPlanErrorMessage(phys).get(),
                first(rdcAgg.getAggregateCalls()).getAggregation(),
                IsInstanceOf.instanceOf(SqlCountAggFunction.class));

        assertThat(
                invalidPlanErrorMessage(phys).get(),
                first(mapAgg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlCountAggFunction.class));
    }

    /**
     * Test subquery with aggregate.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void subqueryWithAggregate(AggregateAlgorithm algo) throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        createTable(publicSchema,
                "EMPS",
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("SALARY", f.createJavaType(Double.class))
                        .build(),
                IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID)
        );

        String sql = "SELECT * FROM emps WHERE emps.salary = (SELECT AVG(emps.salary) FROM emps)";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );
        assertNotNull(phys);

        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));
        IgniteMapAggregateBase mapAgg = findFirstNode(phys, byClass(algo.map));

        assertNotNull(rdcAgg, invalidPlanErrorMessage(phys));
        assertNotNull(mapAgg, invalidPlanErrorMessage(phys));

        assertThat(
                invalidPlanErrorMessage(phys).get(),
                first(rdcAgg.getAggregateCalls()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        assertThat(
                invalidPlanErrorMessage(phys).get(),
                first(mapAgg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));
    }

    /**
     * Check distinct aggregate with no aggregate function.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void mapReduceDistinctWithIndex(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createAffinityTable("TEST").addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT DISTINCT val0, val1 FROM test";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );

        IgniteAggregate mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull(rdcAgg, invalidPlanErrorMessage(phys));
        assertNotNull(mapAgg, invalidPlanErrorMessage(phys));

        assertTrue(nullOrEmpty(rdcAgg.getAggregateCalls()), invalidPlanErrorMessage(phys));
        assertTrue(nullOrEmpty(mapAgg.getAggCallList()), invalidPlanErrorMessage(phys));

        if (algo == AggregateAlgorithm.SORT) {
            assertNotNull(findFirstNode(phys, byClass(IgniteIndexScan.class)));
        }
    }

    /**
     * Checks if already sorted input exist and involved [Map|Reduce]SortAggregate.
     */
    @ParameterizedTest
    @EnumSource
    public void testNoSortAppendingWithCorrectCollation(AggregateAlgorithm algo) throws Exception {
        RelFieldCollation coll = new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING);

        TestTable tbl = createAffinityTable("TEST").addIndex(RelCollations.of(coll), "val0Idx");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sql = "SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                ArrayUtils.concat(
                        algo.rulesToDisable,
                        "NestedLoopJoinConverter",
                        "CorrelatedNestedLoopJoin",
                        "CorrelateToNestedLoopRule"
                )
        );

        if (algo == AggregateAlgorithm.SORT) {
            assertNull(findFirstNode(phys, byClass(IgniteSort.class)), invalidPlanErrorMessage(phys));
            assertNull(findFirstNode(phys, byClass(IgniteTableScan.class)), invalidPlanErrorMessage(phys));
        } else {
            assertNotNull(findFirstNode(phys, byClass(IgniteSort.class)), invalidPlanErrorMessage(phys));
            assertNotNull(findFirstNode(phys, byClass(IgniteTableScan.class)), invalidPlanErrorMessage(phys));
        }
    }

    /**
     * Check that plan does not contain distinct accumulators on map nodes with additional expectations.
     *
     * @param sql Request string.
     * @param publicSchema Schema.
     * @param algo
     * @throws Exception If failed.
     */
    private void checkDistinctInMapAggNode(String sql, IgniteSchema publicSchema, AggregateAlgorithm algo) throws Exception {
        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                concat(algo.rulesToDisable, "ColocatedSortAggregateConverterRule", "ColocatedHashAggregateConverterRule"));

        assertFalse(findNodes(phys, byClass(IgniteMapAggregateBase.class)).isEmpty(),
                invalidPlanErrorMessage(phys));

        assertFalse(findNodes(phys, byClass(IgniteMapAggregateBase.class)).stream()
                        .anyMatch(n -> ((Aggregate) n).getAggCallList().stream()
                                .anyMatch(AggregateCall::isDistinct)
                        ),
                invalidPlanErrorMessage(phys));
    }

    private static Stream<Arguments> provideAlgoAndDistribution() {
        return Stream.of(
                Arguments.of(AggregateAlgorithm.SORT, IgniteDistributions.broadcast()),
                Arguments.of(AggregateAlgorithm.SORT, IgniteDistributions.random()),
                Arguments.of(AggregateAlgorithm.HASH, IgniteDistributions.broadcast()),
                Arguments.of(AggregateAlgorithm.HASH, IgniteDistributions.random())
        );
    }
}
