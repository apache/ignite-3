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

import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * Tests IgniteProjectCorrelateTransposeRule.
 */
public class ProjectCorrelateTransposePlannerTest extends AbstractPlannerTest {
    @Test
    public void testProjectCorrelateTranspose() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                tableA("T0"),
                tableA("T1"));

        String sql = "select t0.id "
                + "from t0 "
                + "where exists (select * from t1 where t0.jid = t1.jid);";

        Predicate<RelNode> check =
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(0, isTableScan("T0").and(n -> n.requiredColumns() != null))));

        assertPlan(sql, publicSchema, check);
    }

    /** Test works of system_range function with correlate. */
    @Test
    public void testSystemRangeFunctionWithCorrelate() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(tableA("T0"));
        String sql = "SELECT t.jid FROM t0 t WHERE t.jid < 5 AND EXISTS "
                + "(SELECT x FROM table(system_range(t.jid, t.jid)) WHERE mod(x, 2) = 0)";

        Predicate<IgniteCorrelatedNestedLoopJoin> check =
                isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(0, isTableScan("T0").and(n -> n.requiredColumns() != null)));

        assertPlan(sql, publicSchema, check);
    }

    private static UnaryOperator<TableBuilder> tableA(String tableName) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("JID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(IgniteDistributions.broadcast());
    }
}
