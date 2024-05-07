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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * HashIndexSpoolPlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-18689")
public class HashIndexSpoolPlannerTest extends AbstractPlannerTest {
    /**
     * Check equi-join on not colocated fields. CorrelatedNestedLoopJoinTest is applicable for this case only with IndexSpool.
     */
    @Test
    public void testSingleKey() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                tableA("T0"),
                tableA("T1").andThen(addHashIndex("JID", "ID"))
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToSortedIndexSpoolRule", "HashJoinConverter"
        );

        System.out.println("+++\n" + RelOptUtil.toString(phys));

        IgniteHashIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteHashIndexSpool.class));

        List<RexNode> searchRow = idxSpool.searchRow();

        assertNotNull(searchRow);
        assertEquals(3, searchRow.size());

        assertNull(searchRow.get(0));
        assertTrue(searchRow.get(1) instanceof RexFieldAccess);
        assertNull(searchRow.get(2));
    }

    @Test
    public void testMultipleKeys() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                tableB("T0"),
                tableB("T1").andThen(addHashIndex("JID", "ID"))
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid0 = t1.jid0 and t0.jid1 = t1.jid1";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToSortedIndexSpoolRule", "HashJoinConverter"
        );

        IgniteHashIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteHashIndexSpool.class));

        List<RexNode> searchRow = idxSpool.searchRow();

        assertNotNull(searchRow);
        assertEquals(4, searchRow.size());

        assertNull(searchRow.get(0));
        assertTrue(searchRow.get(1) instanceof RexFieldAccess);
        assertTrue(searchRow.get(2) instanceof RexFieldAccess);
        assertNull(searchRow.get(3));
    }

    /**
     * Check equi-join on not colocated fields without indexes.
     */
    @Test
    public void testSourceWithoutCollation() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                tableA("T0"),
                tableA("T1")
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "HashJoinConverter"
        );

        IgniteHashIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteHashIndexSpool.class));

        List<RexNode> searchRow = idxSpool.searchRow();

        assertNotNull(searchRow);
        assertEquals(3, searchRow.size());

        assertNull(searchRow.get(0));
        assertTrue(searchRow.get(1) instanceof RexFieldAccess);
        assertNull(searchRow.get(2));
    }

    private static UnaryOperator<TableBuilder> tableA(String tableName) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("JID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(someAffinity());
    }

    private static UnaryOperator<TableBuilder> tableB(String tableName) {
        return tableBuilder -> tableBuilder
                .name(tableName)
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("JID0", NativeTypes.INT32)
                .addColumn("JID1", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(someAffinity());
    }
}
