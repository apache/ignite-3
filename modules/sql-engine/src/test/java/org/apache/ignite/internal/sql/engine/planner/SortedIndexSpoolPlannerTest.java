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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * SortedIndexSpoolPlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-18689")
public class SortedIndexSpoolPlannerTest extends AbstractPlannerTest {
    /**
     * Check equi-join on not colocated fields. CorrelatedNestedLoopJoinTest is applicable for this case only with IndexSpool.
     */
    @Test
    public void testNotColocatedEqJoin() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                tableA("T0").andThen(addSortIndex("JID", "ID")),
                tableA("T1").andThen(addSortIndex("JID", "ID"))
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "HashJoinConverter", "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );

        IgniteSortedIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteSortedIndexSpool.class));

        List<SearchBounds> searchBounds = idxSpool.searchBounds();

        assertNotNull(searchBounds, "Invalid plan\n" + RelOptUtil.toString(phys));
        assertEquals(2, searchBounds.size());

        assertTrue(searchBounds.get(0) instanceof ExactBounds);
        assertTrue(((ExactBounds) searchBounds.get(0)).bound() instanceof RexFieldAccess);
        assertNull(searchBounds.get(1));
    }

    /**
     * Check case when exists index (collation) isn't applied not for whole join condition but may be used by part of condition.
     */
    @Test
    public void testPartialIndexForCondition() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                tableB("T0"),
                tableB("T1").andThen(addSortIndex("JID2", "JID1"))
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid0 = t1.jid0 and t0.jid1 = t1.jid1";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "HashJoinConverter", "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );

        System.out.println("+++ \n" + RelOptUtil.toString(phys));

        IgniteSortedIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteSortedIndexSpool.class));

        List<SearchBounds> searchBounds = idxSpool.searchBounds();

        assertNotNull(searchBounds, "Invalid plan\n" + RelOptUtil.toString(phys));
        assertEquals(2, searchBounds.size());

        assertTrue(searchBounds.get(0) instanceof ExactBounds);
        assertTrue(((ExactBounds) searchBounds.get(0)).bound() instanceof RexFieldAccess);
        assertTrue(searchBounds.get(1) instanceof ExactBounds);
        assertTrue(((ExactBounds) searchBounds.get(1)).bound() instanceof RexFieldAccess);
    }

    /**
     * Check colocated fields with DESC ordering.
     */
    @Test
    public void testDescFields() throws Exception {
        IgniteSchema publicSchema = createSchemaFrom(
                tableA("T0").andThen(setSize(10)),
                tableA("T1").andThen(setSize(100)).andThen(index("jid", Collation.DESC_NULLS_LAST))
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t1.jid < t0.jid";

        assertPlan(sql, publicSchema,
                isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteSortedIndexSpool.class)
                                .and(spool -> {
                                    List<SearchBounds> searchBounds = spool.searchBounds();

                                    // Condition is LESS_THEN, but we have DESC field and condition should be in lower bound
                                    // instead of upper bound.
                                    assertNotNull(searchBounds);
                                    assertEquals(1, searchBounds.size());

                                    assertTrue(searchBounds.get(0) instanceof RangeBounds);
                                    RangeBounds fld1Bounds = (RangeBounds) searchBounds.get(0);
                                    assertTrue(fld1Bounds.lowerBound() instanceof RexFieldAccess);
                                    assertFalse(fld1Bounds.lowerInclude());
                                    // NULLS LAST in collation, so nulls can be skipped by upper bound.
                                    assertEquals("$NULL_BOUND()", fld1Bounds.upperBound().toString());
                                    assertFalse(fld1Bounds.upperInclude());

                                    return true;
                                })
                                .and(hasChildThat(isIndexScan("T1", "idx_jid")))
                        )),
                "HashJoinConverter", "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );
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

    private static UnaryOperator<TableBuilder> index(String column, Collation collation) {
        return tableBuilder -> tableBuilder.sortedIndex()
                .name("idx_" + column)
                .addColumn(column, collation)
                .end();
    }
}
