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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteSingleAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteSingleHashAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * AggregateDistinctPlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class AggregateDistinctPlannerTest extends AbstractAggregatePlannerTest {
    /**
     * MapReduceDistinctWithIndex.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void mapReduceDistinctWithIndex(AggregateAlgorithm algo) throws Exception {
        TestTable tbl = createAffinityTable().addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT DISTINCT val0, val1 FROM test";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                algo.rulesToDisable
        );

        IgniteAggregate mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull(rdcAgg, "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES));
        assertNotNull(mapAgg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertTrue(nullOrEmpty(rdcAgg.getAggregateCalls()), "Invalid plan\n" + RelOptUtil.toString(phys));
        assertTrue(nullOrEmpty(mapAgg.getAggCallList()), "Invalid plan\n" + RelOptUtil.toString(phys));

        // TODO: uncomment after IGNITE-17748"
        // if (algo == AggregateAlgorithm.SORT) {
        //     assertNotNull(findFirstNode(phys, byClass(IgniteIndexScan.class)));
        // }
    }

    enum AggregateAlgorithm {
        // TODO: uncomment after IGNITE-17748
        // SORT(
        //         IgniteSingleSortAggregate.class,
        //         IgniteMapSortAggregate.class,
        //         IgniteReduceSortAggregate.class,
        //         "HashSingleAggregateConverterRule",
        //         "HashMapReduceAggregateConverterRule",
        //         "SortSingleAggregateConverterRule"
        // ),

        HASH(
                IgniteSingleHashAggregate.class,
                IgniteMapHashAggregate.class,
                IgniteReduceHashAggregate.class,
                "SortSingleAggregateConverterRule",
                "SortMapReduceAggregateConverterRule",
                "HashSingleAggregateConverterRule"
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
