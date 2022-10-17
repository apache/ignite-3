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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoTimeoutException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

/**
 * Test planner timeout.
 */
public class PlannerTimeoutTest extends AbstractPlannerTest {
    private static final long PLANNER_TIMEOUT = 500;

    @Test
    public void testLongPlanningTimeout() throws Exception {
        IgniteSchema schema = createSchema(
                createTestTable("T1", "A", Integer.class, "B", Integer.class),
                createTestTable("T2", "A", Integer.class, "B", Integer.class)
        );

        String sql = "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A";

        PlanningContext ctx = PlanningContext.builder()
                .parentContext(baseQueryContext(Collections.singletonList(schema), null))
                .plannerTimeout(PLANNER_TIMEOUT)
                .query(sql)
                .build();

        AtomicReference<IgniteRel> plan = new AtomicReference<>();
        AtomicReference<RelOptPlanner.CannotPlanException> plannerError = new AtomicReference<>();

        assertTimeoutPreemptively(Duration.ofMillis(10 * PLANNER_TIMEOUT), () -> {
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

    @NotNull
    private static TestTable createTestTable(String name, Object... cols) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(TYPE_FACTORY);

        for (int i = 0; i < cols.length; i += 2) {
            b.add((String) cols[i], TYPE_FACTORY.createJavaType((Class<?>) cols[i + 1]));
        }

        return new TestTable(name, b.build(), DEFAULT_TBL_SIZE) {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet bitSet) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.getRowType(typeFactory, bitSet);
            }
        };
    }
}

