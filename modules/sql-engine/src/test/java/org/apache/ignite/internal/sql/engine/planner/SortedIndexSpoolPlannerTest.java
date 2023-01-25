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
import java.util.UUID;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
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
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        createTable(publicSchema,
                "T0",
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("JID", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build(),
                IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID)
        ).addIndex("t0_jid_idx", 1, 0);

        createTable(publicSchema,
                "T1",
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("JID", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build(),
                IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID)
        ).addIndex("t1_jid_idx", 1, 0);

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
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
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        createTable(publicSchema,
                "T0",
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("JID0", f.createJavaType(Integer.class))
                        .add("JID1", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build(),
                IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID)
        );

        createTable(publicSchema,
                "T1",
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("JID0", f.createJavaType(Integer.class))
                        .add("JID1", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build(),
                IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID)
        ).addIndex("t1_jid0_idx", 2, 1);

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid0 = t1.jid0 and t0.jid1 = t1.jid1";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
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
        IgniteSchema publicSchema = createSchema(
                createTable("T0", 10, IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID),
                        "ID", Integer.class, "JID", Integer.class, "VAL", String.class)
                        .addIndex("t0_jid_idx", 1),
                createTable("T1", 100, IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID),
                        "ID", Integer.class, "JID", Integer.class, "VAL", String.class)
                        .addIndex(RelCollations.of(TraitUtils.createFieldCollation(1, ColumnCollation.DESC_NULLS_LAST)), "t1_jid_idx")
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
                                    assertTrue(((RexLiteral) fld1Bounds.upperBound()).isNull());
                                    assertFalse(fld1Bounds.upperInclude());

                                    return true;
                                })
                                .and(hasChildThat(isIndexScan("T1", "t1_jid_idx")))
                        )),
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );
    }
}
