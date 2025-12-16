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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlannerPhase;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/** Tests to verify correlated subquery planning. */
public class CorrelatedSubqueryPlannerTest extends AbstractPlannerTest {
    /**
     * Test verifies the row type is consistent for correlation variable and node that actually puts this variable into a context.
     *
     * <p>In this particular test the row type of the left input of CNLJ node should
     * match the row type correlated variable in the filter was created with.
     *
     * @throws Exception In case of any unexpected error.
     */
    @Test
    public void test() throws Exception {
        IgniteSchema schema = createSchema(createTestTable("A", "B", "C", "D", "E"));

        String sql = ""
                + "SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)\n"
                + "  FROM t1\n"
                + " WHERE (a>e-2 AND a<e+2)\n"
                + "    OR c>d\n"
                + " ORDER BY 1;";

        IgniteRel rel = physicalPlan(sql, schema, "FilterTableScanMergeRule");

        IgniteFilter filter = findFirstNode(rel, byClass(IgniteFilter.class)
                .and(f -> RexUtils.hasCorrelation(((Filter) f).getCondition())));

        RexFieldAccess fieldAccess = findFirst(filter.getCondition(), RexFieldAccess.class);

        assertNotNull(fieldAccess);
        assertEquals(
                RexCorrelVariable.class,
                fieldAccess.getReferenceExpr().getClass()
        );

        IgniteCorrelatedNestedLoopJoin join = findFirstNode(rel, byClass(IgniteCorrelatedNestedLoopJoin.class));

        assertEquals(
                fieldAccess.getReferenceExpr().getType(),
                join.getLeft().getRowType()
        );
    }

    /**
     * Test verifies resolving of collisions in the left hand of correlates.
     */
    @Test
    public void testCorrelatesCollisionsLeftHand() throws Exception {
        IgniteSchema schema = createSchema(
                createTestTable("A", "B", "C", "D")
        );

        String sql = "SELECT * FROM t1 as cor WHERE "
                + "EXISTS (SELECT 1 FROM t1 WHERE t1.b = cor.a) AND "
                + "EXISTS (SELECT 1 FROM t1 WHERE t1.c = cor.a) AND "
                + "EXISTS (SELECT 1 FROM t1 WHERE t1.d = cor.a)";

        PlanningContext ctx = plannerCtx(sql, schema);

        try (IgnitePlanner planner = ctx.planner()) {
            RelNode rel = convertSubQueries(planner, ctx);

            List<LogicalCorrelate> correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(3, correlates.size());

            // There are collisions by correlation id.
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(1).getCorrelationId());
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(2).getCorrelationId());

            rel = planner.replaceCorrelatesCollisions(rel);

            correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(3, correlates.size());

            // There are no collisions by correlation id.
            assertNotEquals(correlates.get(0).getCorrelationId(), correlates.get(1).getCorrelationId());
            assertNotEquals(correlates.get(0).getCorrelationId(), correlates.get(2).getCorrelationId());
            assertNotEquals(correlates.get(1).getCorrelationId(), correlates.get(2).getCorrelationId());

            List<LogicalFilter> filters = findNodes(rel, byClass(LogicalFilter.class)
                    .and(f -> RexUtils.hasCorrelation(((Filter) f).getCondition())));

            assertEquals(3, filters.size());

            // Filters match correlates in reverse order (we find outer correlate first, but inner filter first).
            assertEquals(Set.of(correlates.get(0).getCorrelationId()),
                    RexUtils.extractCorrelationIds(filters.get(2).getCondition()));

            assertEquals(Set.of(correlates.get(1).getCorrelationId()),
                    RexUtils.extractCorrelationIds(filters.get(1).getCondition()));

            assertEquals(Set.of(correlates.get(2).getCorrelationId()),
                    RexUtils.extractCorrelationIds(filters.get(0).getCondition()));
        }
    }

    /**
     * Test verifies resolving of collisions in the right hand of correlates.
     */
    @Test
    public void testCorrelatesCollisionsRightHand() throws Exception {
        IgniteSchema schema = createSchema(
                createTestTable("A")
        );

        String sql = "SELECT (SELECT (SELECT (SELECT cor.a))) FROM t1 as cor";

        PlanningContext ctx = plannerCtx(sql, schema);

        try (IgnitePlanner planner = ctx.planner()) {
            RelNode rel = convertSubQueries(planner, ctx);

            List<LogicalCorrelate> correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(3, correlates.size());

            // There are collisions by correlation id.
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(1).getCorrelationId());
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(2).getCorrelationId());

            rel = planner.replaceCorrelatesCollisions(rel);

            correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(1, correlates.size());
        }
    }

    /**
     * Test verifies resolving of collisions in right and left hands of correlates.
     */
    @Test
    public void testCorrelatesCollisionsMixed() throws Exception {
        IgniteSchema schema = createSchema(
                createTestTable("A", "B", "C")
        );

        String sql = "SELECT * FROM t1 as cor WHERE "
                + "EXISTS (SELECT 1 FROM t1 WHERE t1.b = (SELECT cor.a)) AND "
                + "EXISTS (SELECT 1 FROM t1 WHERE t1.c = (SELECT cor.a))";

        PlanningContext ctx = plannerCtx(sql, schema);

        try (IgnitePlanner planner = ctx.planner()) {
            RelNode rel = convertSubQueries(planner, ctx);

            List<LogicalCorrelate> correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(2, correlates.size());

            // There are collisions by correlation id.
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(1).getCorrelationId());

            // Cannot decorrelate further
            RelNode replaced = planner.replaceCorrelatesCollisions(rel);
            assertSame(rel, replaced);

            correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(2, correlates.size());

            // There are no collisions by correlation id.
            assertNotEquals(correlates.get(0).getCorrelationId(), correlates.get(1).getCorrelationId());

            List<LogicalProject> projects = findNodes(rel, byClass(LogicalProject.class)
                    .and(f -> RexUtils.hasCorrelation(((Project) f).getProjects())));

            assertEquals(2, projects.size());

            assertEquals(Set.of(correlates.get(0).getCorrelationId()),
                    RexUtils.extractCorrelationIds(projects.get(1).getProjects()));

            assertEquals(Set.of(correlates.get(1).getCorrelationId()),
                    RexUtils.extractCorrelationIds(projects.get(0).getProjects()));
        }
    }

    private RelNode convertSubQueries(IgnitePlanner planner, PlanningContext ctx) throws Exception {
        // Parse and validate.
        SqlNode sqlNode = planner.validate(planner.parse(ctx.query()));

        // Create original logical plan.
        RelNode rel = planner.rel(sqlNode).rel;

        // Convert sub-queries to correlates.
        return planner.transform(PlannerPhase.HEP_SUBQUERIES_TO_CORRELATES, rel.getTraitSet(), rel);
    }

    /** Creates test table with columns of given name and INT32 type. */
    static IgniteTable createTestTable(String... columns) {
        assert columns.length > 0;

        TableBuilder tableBuilder = TestBuilders.table()
                .name("T1")
                .distribution(IgniteDistributions.single());

        for (String column : columns) {
            tableBuilder.addColumn(column, NativeTypes.INT32);
        }

        return tableBuilder.build();
    }
}

