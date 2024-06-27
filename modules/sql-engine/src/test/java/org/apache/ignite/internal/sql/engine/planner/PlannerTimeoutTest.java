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

import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoTimeoutException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.framework.PredefinedSchemaManager;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test planner timeout.
 */
public class PlannerTimeoutTest extends AbstractPlannerTest {

    @Test
    public void testPlannerTimeout() throws Exception {
        long plannerTimeout = 1L;
        IgniteSchema schema = createSchema(createTestTable("T1"));
        SqlOperationContext ctx = operationContext();

        PrepareService prepareService = new PrepareServiceImpl("test", 0,
                CaffeineCacheFactory.INSTANCE, null, plannerTimeout, 1, new MetricManagerImpl(), new PredefinedSchemaManager(schema));
        prepareService.start();
        try {
            ParserService parserService = new ParserServiceImpl();

            ParsedResult parsedResult = parserService.parse("SELECT * FROM T1 t, T1 t1, T1 t2, T1 t3");

            SqlTestUtils.assertThrowsSqlException(
                    EXECUTION_CANCELLED_ERR,
                    "Planning of a query aborted due to planner timeout threshold is reached",
                    () -> await(prepareService.prepareAsync(parsedResult, ctx)));
        } finally {
            prepareService.stop();
        }
    }

    @Test
    public void testLongPlanningTimeout() {
        final long plannerTimeout = 500;

        IgniteSchema schema = createSchema(
                createTestTable("T1"),
                createTestTable("T2")
        );

        String sql = "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A";

        PlanningContext ctx = PlanningContext.builder()
                .plannerTimeout(plannerTimeout)
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(createRootSchema(List.of(schema)).getSubSchema(schema.getName()))
                        .build())
                .query(sql)
                .build();

        AtomicReference<IgniteRel> plan = new AtomicReference<>();
        AtomicReference<RelOptPlanner.CannotPlanException> plannerError = new AtomicReference<>();

        assertTimeoutPreemptively(Duration.ofMillis(10 * plannerTimeout), () -> {
            try (IgnitePlanner planner = ctx.planner()) {
                plan.set(physicalPlan(planner, ctx.query()));

                VolcanoPlanner volcanoPlanner = (VolcanoPlanner) ctx.cluster().getPlanner();

                assertNotNull(volcanoPlanner);

                assertThrowsWithCause(volcanoPlanner::checkCancel, VolcanoTimeoutException.class);
            } catch (RelOptPlanner.CannotPlanException e) {
                plannerError.set(e);
            } catch (Exception e) {
                throw new RuntimeException("Planning failed", e);
            }
        });

        assertTrue(plan.get() != null || plannerError.get() != null);

        if (plan.get() != null) {
            new RelVisitor() {
                @Override
                public void visit(
                        RelNode node,
                        int ordinal,
                        RelNode parent
                ) {
                    assertNotNull(node.getTraitSet().getTrait(IgniteConvention.INSTANCE.getTraitDef()));
                    super.visit(node, ordinal, parent);
                }
            }.go(plan.get());
        }
    }

    private static IgniteTable createTestTable(String tableName) {
        IgniteTable igniteTable = TestBuilders.table()
                .name(tableName)
                .addColumn("A", NativeTypes.INT32)
                .addColumn("B", NativeTypes.INT32)
                .distribution(someAffinity())
                .size(DEFAULT_TBL_SIZE)
                .build();

        // Create a proxy.
        IgniteTable spyTable = Mockito.spy(igniteTable);

        // Override and slowdown a method, which is called by Planner, to emulate long planning.
        Mockito.doAnswer(inv -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // Call original method.
            return igniteTable.getRowType(inv.getArgument(0), inv.getArgument(1));
        }).when(spyTable).getRowType(any(), any());

        return spyTable;
    }
}

