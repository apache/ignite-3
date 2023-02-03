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

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.Test;

/**
 * HashAggregatePlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class HashAggregatePlannerTest extends AbstractAggregatePlannerTest {
    /**
     * SubqueryWithAggregate.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void subqueryWithAggregate() throws Exception {
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

        String sql = "SELECT /*+ DISABLE_RULE('MapReduceSortAggregateConverterRule', 'ColocatedHashAggregateConverterRule', "
                + "'ColocatedSortAggregateConverterRule') */ * FROM emps WHERE emps.salary = (SELECT AVG(emps.salary) FROM emps)";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema
        );

        assertNotNull(phys);

        IgniteReduceHashAggregate rdcAgg = findFirstNode(phys, byClass(IgniteReduceHashAggregate.class));
        IgniteMapHashAggregate mapAgg = findFirstNode(phys, byClass(IgniteMapHashAggregate.class));

        assertNotNull(rdcAgg, "Invalid plan\n" + RelOptUtil.toString(phys));
        assertNotNull(mapAgg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertThat(
                "Invalid plan\n" + RelOptUtil.toString(phys),
                first(rdcAgg.getAggregateCalls()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        assertThat(
                "Invalid plan\n" + RelOptUtil.toString(phys),
                first(mapAgg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlAvgAggFunction.class));
    }

    /**
     * NoGroupByAggregate.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void noGroupByAggregate() throws Exception {
        TestTable tbl = createAffinityTable("TEST").addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(tbl);

        String sqlCount = "SELECT /*+ DISABLE_RULE('MapReduceSortAggregateConverterRule', 'ColocatedHashAggregateConverterRule', "
                + "'ColocatedSortAggregateConverterRule') */ COUNT(*) FROM test";

        IgniteRel phys = physicalPlan(
                sqlCount,
                publicSchema
        );

        IgniteMapHashAggregate mapAgg = findFirstNode(phys, byClass(IgniteMapHashAggregate.class));
        IgniteReduceHashAggregate rdcAgg = findFirstNode(phys, byClass(IgniteReduceHashAggregate.class));

        assertNotNull(rdcAgg, "Invalid plan\n" + RelOptUtil.toString(phys));
        assertNotNull(mapAgg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertThat(
                "Invalid plan\n" + RelOptUtil.toString(phys),
                first(rdcAgg.getAggregateCalls()).getAggregation(),
                IsInstanceOf.instanceOf(SqlCountAggFunction.class));

        assertThat(
                "Invalid plan\n" + RelOptUtil.toString(phys),
                first(mapAgg.getAggCallList()).getAggregation(),
                IsInstanceOf.instanceOf(SqlCountAggFunction.class));
    }
}
