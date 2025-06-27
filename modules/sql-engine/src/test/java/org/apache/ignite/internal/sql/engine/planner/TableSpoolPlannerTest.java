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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.UnaryOperator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableSpool;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Table spool test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-18689")
public class TableSpoolPlannerTest extends AbstractPlannerTest {
    /**
     * TableSpoolDistributed.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void tableSpoolDistributed() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                createTestTable("T0"),
                createTestTable("T1")
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid > t1.jid";

        IgniteRel phys = physicalPlan(sql, publicSchema,
                "HashJoinConverter", "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeRule");

        assertNotNull(phys);

        IgniteTableSpool tblSpool = findFirstNode(phys, byClass(IgniteTableSpool.class));

        assertNotNull(tblSpool, "Invalid plan:\n" + RelOptUtil.toString(phys));

        checkSplitAndSerialization(phys, publicSchema);
    }

    private static UnaryOperator<TableBuilder> createTestTable(String tableName) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("JID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(someAffinity());
    }
}
