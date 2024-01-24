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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule.JoinConditionPushRule;
import org.apache.calcite.rel.rules.FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests correctness applying of JOIN_COMMUTE* rules.
 */
public class JoinCommutePlannerTest extends AbstractPlannerTest {
    private static IgniteSchema publicSchema;

    /**
     * Set up tests.
     */
    @BeforeAll
    public static void init() {
        publicSchema = createSchema(
                TestBuilders.table()
                        .name("HUGE")
                        .addColumn("ID", NativeTypes.INT32)
                        .distribution(IgniteDistributions.affinity(0, nextTableId(), DEFAULT_ZONE_ID))
                        .size(1_000)
                        .build(),
                TestBuilders.table()
                        .name("SMALL")
                        .addColumn("ID", NativeTypes.INT32)
                        .distribution(IgniteDistributions.affinity(0, nextTableId(), DEFAULT_ZONE_ID))
                        .size(10)
                        .build()
        );
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16334")
    public void testEnforceJoinOrderHint() throws Exception {
        String sqlJoinCommuteWithNoHint = "SELECT COUNT(*) FROM SMALL s, HUGE h, HUGE h1 WHERE h.id = s.id and h1.id=s.id";
        String sqlJoinCommuteWithHint =
                "SELECT /*+ ENFORCE_JOIN_ORDER */ COUNT(*) FROM SMALL s, HUGE h, HUGE h1 WHERE h.id = s.id and h1.id=s.id";
        String sqlJoinCommuteOuterWithNoHint = "SELECT COUNT(*) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";
        String sqlJoinCommuteOuterWithHint = "SELECT /*+ ENFORCE_JOIN_ORDER */ COUNT(*) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";

        JoinConditionPushRule joinConditionPushRule = JoinConditionPushRuleConfig.DEFAULT.toRule();
        JoinCommuteRule joinCommuteRule = JoinCommuteRule.Config.DEFAULT.toRule();

        RuleAttemptListener listener = new RuleAttemptListener();

        physicalPlan(sqlJoinCommuteWithNoHint, publicSchema, listener);
        assertTrue(listener.isApplied(joinCommuteRule));
        assertTrue(listener.isApplied(joinConditionPushRule));

        listener.reset();
        physicalPlan(sqlJoinCommuteOuterWithNoHint, publicSchema, listener);
        assertTrue(listener.isApplied(joinCommuteRule));
        assertTrue(listener.isApplied(joinConditionPushRule));

        listener.reset();
        physicalPlan(sqlJoinCommuteWithHint, publicSchema, listener);
        assertFalse(listener.isApplied(joinCommuteRule));
        assertTrue(listener.isApplied(joinConditionPushRule));

        listener.reset();
        physicalPlan(sqlJoinCommuteOuterWithHint, publicSchema, listener);
        assertFalse(listener.isApplied(joinCommuteRule));
        assertTrue(listener.isApplied(joinConditionPushRule));
    }

    @Test
    public void testOuterCommute() throws Exception {
        // Use aggregates that are the same for both MAP and REDUCE phases.
        String sql = "SELECT SUM(s.id), SUM(h.id) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin");

        assertNotNull(phys);

        IgniteNestedLoopJoin join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));

        IgniteProject proj = findFirstNode(phys, byClass(IgniteProject.class));

        assertNotNull(proj);

        assertEquals(JoinRelType.LEFT, join.getJoinType());

        PlanningContext ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin");

        RelOptPlanner pl = ctx.cluster().getPlanner();

        RelOptCost costWithCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        assertNotNull(costWithCommute);

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));

        proj = findFirstNode(phys, byClass(IgniteProject.class));

        assertNull(proj);

        // no commute
        assertEquals(JoinRelType.RIGHT, join.getJoinType());

        ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        pl = ctx.cluster().getPlanner();

        RelOptCost costWithoutCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertTrue(costWithCommute.isLt(costWithoutCommute));
    }

    @Test
    public void testInnerCommute() throws Exception {
        // Use aggregates that are the same for both MAP and REDUCE phases.
        String sql = "SELECT SUM(s.id), SUM(h.id) FROM SMALL s JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin");

        assertNotNull(phys);

        IgniteNestedLoopJoin join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));
        assertNotNull(join);

        IgniteProject proj = findFirstNode(phys, byClass(IgniteProject.class));
        assertNotNull(proj);

        IgniteTableScan rightScan = findFirstNode(join.getRight(), byClass(IgniteTableScan.class));
        assertNotNull(rightScan);

        IgniteTableScan leftScan = findFirstNode(join.getLeft(), byClass(IgniteTableScan.class));
        assertNotNull(leftScan);

        List<String> rightSchemaWithName = rightScan.getTable().getQualifiedName();

        assertEquals(2, rightSchemaWithName.size());

        assertEquals(rightSchemaWithName.get(1), "SMALL");

        List<String> leftSchemaWithName = leftScan.getTable().getQualifiedName();

        assertEquals(leftSchemaWithName.get(1), "HUGE");

        assertEquals(JoinRelType.INNER, join.getJoinType());

        PlanningContext ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin");

        RelOptPlanner pl = ctx.cluster().getPlanner();

        RelOptCost costWithCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());
        assertNotNull(costWithCommute);

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));
        proj = findFirstNode(phys, byClass(IgniteProject.class));

        rightScan = findFirstNode(join.getRight(), byClass(IgniteTableScan.class));
        leftScan = findFirstNode(join.getLeft(), byClass(IgniteTableScan.class));

        assertNotNull(join);
        assertNull(proj);
        assertNotNull(rightScan);
        assertNotNull(leftScan);

        rightSchemaWithName = rightScan.getTable().getQualifiedName();

        assertEquals(2, rightSchemaWithName.size());
        // no commute
        assertEquals(rightSchemaWithName.get(1), "HUGE");

        leftSchemaWithName = leftScan.getTable().getQualifiedName();

        assertEquals(leftSchemaWithName.get(1), "SMALL");

        // no commute
        assertEquals(JoinRelType.INNER, join.getJoinType());

        ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        pl = ctx.cluster().getPlanner();

        RelOptCost costWithoutCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertTrue(costWithCommute.isLt(costWithoutCommute));
    }

    /**
     * The test verifies that queries with a considerable number of joins can be planned for a
     * reasonable amount of time, thus all assertions ensure that the planer returns anything but
     * null. The "reasonable amount of time" here is a timeout of the test. With enabled
     * {@link CoreRules#JOIN_COMMUTE}, optimization of joining with a few dozens of tables will
     * take an eternity. Thus, if the test finished before it was killed, it can be considered
     * a success.
     */
    @Test
    public void commuteIsDisabledForBigJoinsOfTables() throws Exception {
        int joinSize = 50;

        IgniteTable[] tables = new IgniteTable[joinSize];

        for (int i = 0; i < joinSize; i++) {
            tables[i] = TestBuilders.table()
                    .name("T" + (i + 1))
                    .addColumn("ID", NativeTypes.INT32)
                    .addColumn("AFF", NativeTypes.INT32)
                    .addColumn("VAL", NativeTypes.stringOf(128))
                    .distribution(IgniteDistributions.hash(List.of(1)))
                    .build();
        }

        IgniteSchema schema = createSchema(tables);

        String tableList = IntStream.range(1, joinSize + 1)
                .mapToObj(i -> "t" + i) // t1, t2, t3...
                .collect(Collectors.joining(", "));

        String predicateList = IntStream.range(1, joinSize)
                .mapToObj(i -> format("t{}.id = t{}.{}", i, i + 1, i % 3 == 0 ? "aff" : "id"))
                .collect(Collectors.joining(" AND "));

        String queryWithBigJoin = format("SELECT t1.val FROM {} WHERE {}", tableList, predicateList);

        {
            IgniteRel root = physicalPlan(queryWithBigJoin, schema);

            assertNotNull(root);
        }

        {
            IgniteRel root = physicalPlan(format("SELECT ({}) as c", queryWithBigJoin), schema);

            assertNotNull(root);
        }

        {
            IgniteRel root = physicalPlan(format("SELECT 1 FROM t1 WHERE ({}) like 'foo%'", queryWithBigJoin), schema);

            assertNotNull(root);
        }
    }

    /**
     * The same as {@link #commuteIsDisabledForBigJoinsOfTables()}, but with table functions as source of data.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18937")
    public void commuteIsDisabledForBigJoinsOfTableFunctions() throws Exception {
        int joinSize = 50;

        IgniteSchema schema = createSchema();

        String tableList = IntStream.range(1, joinSize + 1)
                .mapToObj(i -> format("table(system_range(0, 10)) t{}(x)", i))
                .collect(Collectors.joining(", "));

        String predicateList = IntStream.range(1, joinSize)
                .mapToObj(i -> format("t{}.x = t{}.x", i, i + 1))
                .collect(Collectors.joining(" AND "));

        String queryWithBigJoin = format("SELECT t1.x FROM {} WHERE {}", tableList, predicateList);

        {
            IgniteRel root = physicalPlan(queryWithBigJoin, schema);

            assertNotNull(root);
        }

        {
            IgniteRel root = physicalPlan(format("SELECT ({}) as c", queryWithBigJoin), schema);

            assertNotNull(root);
        }

        {
            IgniteRel root = physicalPlan(format("SELECT 1 FROM table(system_range(0, 1)) WHERE ({}) > 0", queryWithBigJoin), schema);

            assertNotNull(root);
        }
    }
}
