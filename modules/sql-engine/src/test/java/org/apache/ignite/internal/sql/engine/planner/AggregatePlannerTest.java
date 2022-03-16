/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteSingleAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteSingleHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteSingleSortAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.hamcrest.core.IsInstanceOf;
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
        TestTable tbl = createBroadcastTable().addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );

        IgniteSingleAggregateBase agg = findFirstNode(phys, byClass(algo.single));

        assertNotNull(agg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertThat(
                "Invalid plan\n" + RelOptUtil.toString(phys),
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
        TestTable tbl = createBroadcastTable().addIndex("grp0_grp1", 3, 4);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FILTER(WHERE val1 > 10) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );

        IgniteSingleAggregateBase agg = findFirstNode(phys, byClass(algo.single));

        assertNotNull(agg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertThat(
                "Invalid plan\n" + RelOptUtil.toString(phys),
                first(agg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT) {
            assertNotNull(findFirstNode(phys, byClass(IgniteIndexScan.class)));
        }
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
        TestTable tbl = createAffinityTable();

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FILTER (WHERE val1 > 10) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );

        IgniteMapAggregateBase mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull(rdcAgg, "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES));
        assertNotNull(mapAgg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertThat(
                "Invalid plan\n" + RelOptUtil.toString(phys),
                first(rdcAgg.getAggregateCalls()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        assertThat(
                "Invalid plan\n" + RelOptUtil.toString(phys),
                first(mapAgg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT) {
            assertNotNull(findFirstNode(phys, byClass(IgniteSort.class)));
        }
    }

    /**
     * Test that aggregate has single distribution output even if parent node accept random distibution inputs.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void distribution(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createAffinityTable().addIndex("grp0", 3);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0), grp0 FROM TEST GROUP BY grp0 UNION ALL SELECT val0, grp0 FROM test";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                concat(algo.rulesToDisable, "SortMapReduceAggregateConverterRule",
                        "HashMapReduceAggregateConverterRule")
        );

        IgniteSingleAggregateBase singleAgg = findFirstNode(phys, byClass(algo.single));

        assertEquals(IgniteDistributions.single(), TraitUtils.distribution(singleAgg));

        phys = physicalPlan(
                sql,
                publicSchema,
                concat(algo.rulesToDisable, "SortSingleAggregateConverterRule",
                        "HashSingleAggregateConverterRule")
        );

        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertEquals(IgniteDistributions.single(), TraitUtils.distribution(rdcAgg));
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
        TestTable tbl = createAffinityTable()
                .addIndex("idx_val0", 3, 1, 0)
                .addIndex("idx_val1", 3, 2, 0);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

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
                "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES)
        );

        assertNotNull(
                findFirstNode(phys, byClass(Join.class)),
                "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES)
        );

        // Check the first aggrgation step is SELECT DISTINCT (doesn't contains any accumulators)
        assertTrue(
                findNodes(phys, byClass(algo.reduce)).stream()
                        .allMatch(n -> ((IgniteReduceAggregateBase) n).getAggregateCalls().isEmpty()),
                "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES)
        );

        assertTrue(
                findNodes(phys, byClass(algo.map)).stream()
                        .allMatch(n -> ((Aggregate) n).getAggCallList().isEmpty()),
                "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES)
        );

        // Check the second aggregation step contains accumulators.
        assertTrue(
                findNodes(phys, byClass(algo.single)).stream()
                        .noneMatch(n -> ((Aggregate) n).getAggCallList().isEmpty()),
                "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES)
        );
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

        Class<? extends SingleRel> cls = distr == IgniteDistributions.broadcast() ? algo.single : algo.reduce;

        SingleRel agg = findFirstNode(phys, byClass(cls));

        assertNotNull(agg, "Invalid plan\n" + RelOptUtil.toString(phys));

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

    private static Stream<Arguments> provideAlgoAndDistribution() {
        return Stream.of(
                Arguments.of(AggregateAlgorithm.SORT, IgniteDistributions.broadcast()),
                Arguments.of(AggregateAlgorithm.SORT, IgniteDistributions.random()),
                Arguments.of(AggregateAlgorithm.HASH, IgniteDistributions.broadcast()),
                Arguments.of(AggregateAlgorithm.HASH, IgniteDistributions.random())
        );
    }

    enum AggregateAlgorithm {
        SORT(
                IgniteSingleSortAggregate.class,
                IgniteMapSortAggregate.class,
                IgniteReduceSortAggregate.class,
                "HashSingleAggregateConverterRule", "HashMapReduceAggregateConverterRule"
        ),

        HASH(
                IgniteSingleHashAggregate.class,
                IgniteMapHashAggregate.class,
                IgniteReduceHashAggregate.class,
                "SortSingleAggregateConverterRule", "SortMapReduceAggregateConverterRule"
        );

        public final Class<? extends IgniteSingleAggregateBase> single;

        public final Class<? extends IgniteMapAggregateBase> map;

        public final Class<? extends IgniteReduceAggregateBase> reduce;

        public final String[] rulesToDisable;

        AggregateAlgorithm(
                Class<? extends IgniteSingleAggregateBase> single,
                Class<? extends IgniteMapAggregateBase> map,
                Class<? extends IgniteReduceAggregateBase> reduce,
                String... rulesToDisable) {
            this.single = single;
            this.map = map;
            this.reduce = reduce;
            this.rulesToDisable = rulesToDisable;
        }
    }
}
