/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.RexUtils;
import org.junit.jupiter.api.Test;

/** Tests to verify correlated subquery planning. */
public class CorrelatedSubqueryPlannerTest extends AbstractPlannerTest {
    /**
     * Test verifies the row type is consistent for correlation variable and
     * node that actually puts this variable into a context.
     *
     * <p>In this particular test the row type of the left input of CNLJ node should
     * match the row type correlated variable in the filter was created with.
     *
     * @throws Exception In case of any unexpected error.
     */
    @Test
    public void test() throws Exception {
        IgniteSchema schema = createSchema(
                createTable("T1", IgniteDistributions.single(), "A", Integer.class,
                        "B", Integer.class, "C", Integer.class, "D", Integer.class, "E", Integer.class)
        );

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
}

