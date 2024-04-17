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

import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify join colocation.
 */
public class JoinColocationPlannerTest extends AbstractPlannerTest {
    /**
     * Join of the same tables with a simple affinity is expected to be colocated.
     */
    @Test
    public void joinSameTableSimpleAffMergeJoin() throws Exception {
        IgniteTable tbl = simpleTable("TEST_TBL", DEFAULT_TBL_SIZE);

        IgniteSchema schema = createSchema(tbl);

        String sql = "select count(*) "
                + "from TEST_TBL t1 "
                + "join TEST_TBL t2 on t1.id = t2.id";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");

        IgniteMergeJoin join = findFirstNode(phys, byClass(IgniteMergeJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().function().affinity(), is(true));
        assertThat(invalidPlanMsg, join.getLeft(), instanceOf(IgniteIndexScan.class));
        assertThat(invalidPlanMsg, join.getRight(), instanceOf(IgniteIndexScan.class));
    }

    /**
     * Join of the same tables with a simple affinity is expected to be colocated.
     */
    @Test
    public void joinSameTableSimpleAffHashJoin() throws Exception {
        IgniteTable tbl = simpleTable("TEST_TBL", DEFAULT_TBL_SIZE);

        IgniteSchema schema = createSchema(tbl);

        String sql = "select count(*) "
                + "from TEST_TBL t1 "
                + "join TEST_TBL t2 on t1.id = t2.id";

        // Only hash join
        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "MergeJoinConverter");

        AbstractIgniteJoin join = findFirstNode(phys, byClass(AbstractIgniteJoin.class));
        List<RelNode> joinNodes = findNodes(phys, byClass(AbstractIgniteJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, joinNodes.size(), equalTo(1));
        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().function().affinity(), is(true));
    }

    /**
     * Hash join need to preserve left collation.
     */
    @Test
    public void hashJoinCheckLeftCollationsPropagation() throws Exception {
        IgniteTable tbl1 = simpleTable("TEST_TBL", DEFAULT_TBL_SIZE);
        IgniteTable tbl2 = complexTbl("TEST_TBL_CMPLX");

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select t1.ID, t2.ID1 "
                + "from TEST_TBL_CMPLX t2 "
                + "join TEST_TBL t1 on t1.id = t2.id1 "
                + "order by t2.ID1 NULLS LAST, t2.ID2 NULLS LAST";

        // Only hash join
        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter",
                "CorrelatedNestedLoopJoin", "MergeJoinConverter", "JoinCommuteRule");

        AbstractIgniteJoin join = findFirstNode(phys, byClass(AbstractIgniteJoin.class));
        List<RelNode> joinNodes = findNodes(phys, byClass(AbstractIgniteJoin.class));
        List<RelNode> sortNodes = findNodes(phys, byClass(IgniteSort.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, sortNodes.size(), equalTo(0));
        assertThat(invalidPlanMsg, joinNodes.size(), equalTo(1));
        assertThat(invalidPlanMsg, join, notNullValue());
    }

    /**
     * Hash join erase right collation.
     */
    @Test
    public void hashJoinCheckRightCollations() throws Exception {
        IgniteTable tbl1 = simpleTable("TEST_TBL", DEFAULT_TBL_SIZE);
        IgniteTable tbl2 = complexTbl("TEST_TBL_CMPLX");

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select t1.ID, t2.ID1 "
                + "from TEST_TBL t1 "
                + "join TEST_TBL_CMPLX t2 on t1.id = t2.id1 "
                + "order by t2.ID1 NULLS LAST, t2.ID2 NULLS LAST";

        // Only hash join
        IgniteRel phys = physicalPlan(sql, schema, "NestedLoopJoinConverter",
                "CorrelatedNestedLoopJoin", "MergeJoinConverter", "JoinCommuteRule");

        AbstractIgniteJoin join = findFirstNode(phys, byClass(AbstractIgniteJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, sortOnTopOfJoin(phys), notNullValue());
    }

    private static @Nullable IgniteSort sortOnTopOfJoin(IgniteRel root) {
        List<IgniteSort> sortNodes = findNodes(root, byClass(IgniteSort.class)
                .and(node -> node.getInputs().size() == 1 && node.getInput(0) instanceof Join));

        if (sortNodes.size() > 1) {
            throw new AssertionError("Unexpected count of sort nodes: exp<=1, act=" + sortNodes.size());
        }

        return sortNodes.isEmpty() ? null : sortNodes.get(0);
    }

    /**
     * Join of the same tables with a complex affinity is expected to be colocated.
     */
    @Test
    public void joinSameTableComplexAff() throws Exception {
        IgniteTable tbl = complexTbl("TEST_TBL");

        IgniteSchema schema = createSchema(tbl);

        String sql = "select count(*) "
                + "from TEST_TBL t1 "
                + "join TEST_TBL t2 on t1.id1 = t2.id1 and t1.id2 = t2.id2";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");

        IgniteMergeJoin join = findFirstNode(phys, byClass(IgniteMergeJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().function().affinity(), is(true));
        assertThat(invalidPlanMsg, join.getLeft(), instanceOf(IgniteIndexScan.class));
        assertThat(invalidPlanMsg, join.getRight(), instanceOf(IgniteIndexScan.class));
    }

    /**
     * Re-hashing based on simple affinity.
     *
     * <p>The smaller table should be sent to the bigger one.
     */
    @Test
    public void joinComplexToSimpleAff() throws Exception {
        IgniteTable complexTbl = complexTbl("COMPLEX_TBL", 2 * DEFAULT_TBL_SIZE,
                IgniteDistributions.affinity(ImmutableIntList.of(0, 1), nextTableId(), DEFAULT_ZONE_ID));

        IgniteTable simpleTbl = simpleTable("SIMPLE_TBL", DEFAULT_TBL_SIZE);

        IgniteSchema schema = createSchema(complexTbl, simpleTbl);

        String sql = "select count(*) "
                + "from COMPLEX_TBL t1 "
                + "join SIMPLE_TBL t2 on t1.id1 = t2.id and t1.id2 = t2.id2";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasDistribution(complexTbl.distribution()))
                .and(input(0, isInstanceOf(IgniteIndexScan.class)
                        .and(scan -> complexTbl.equals(scan.getTable().unwrap(IgniteTable.class)))
                ))
                .and(input(1, isInstanceOf(IgniteSort.class)
                        .and(input(isInstanceOf(IgniteExchange.class)
                                .and(input(isInstanceOf(IgniteTableScan.class)
                                        .and(scan -> simpleTbl.equals(scan.getTable().unwrap(IgniteTable.class)))
                                ))
                        ))
                ))
        ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
    }

    /**
     * Re-hashing for complex affinity.
     *
     * <p>The smaller table should be sent to the bigger one.
     */
    @Test
    public void joinComplexToComplexAffWithDifferentOrder() throws Exception {
        IgniteTable complexTblDirect = complexTbl(
                "COMPLEX_TBL_DIRECT",
                2 * DEFAULT_TBL_SIZE,
                IgniteDistributions.affinity(ImmutableIntList.of(0, 1), nextTableId(), DEFAULT_ZONE_ID));

        IgniteTable complexTblIndirect = complexTbl(
                "COMPLEX_TBL_INDIRECT",
                DEFAULT_TBL_SIZE,
                IgniteDistributions.affinity(ImmutableIntList.of(1, 0), nextTableId(), DEFAULT_ZONE_ID));

        IgniteSchema schema = createSchema(complexTblDirect, complexTblIndirect);

        String sql = "select count(*) "
                + "from COMPLEX_TBL_DIRECT t1 "
                + "join COMPLEX_TBL_INDIRECT t2 on t1.id1 = t2.id1 and t1.id2 = t2.id2";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasDistribution(single()))
                .and(input(0, isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteIndexScan.class)
                                .and(scan -> complexTblDirect.equals(scan.getTable().unwrap(IgniteTable.class)))
                        ))
                ))
                .and(input(1, isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteIndexScan.class)
                                .and(scan -> complexTblIndirect.equals(scan.getTable().unwrap(IgniteTable.class)))
                        ))
                ))
        ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
    }

    private static IgniteTable simpleTable(String tableName, int size) {
        return TestBuilders.table()
                .name(tableName)
                .size(size)
                .distribution(someAffinity())
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("ID2", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .sortedIndex()
                .name("PK")
                .addColumn("ID", Collation.ASC_NULLS_LAST)
                .end()
                .build();
    }

    private static IgniteTable complexTbl(String tableName) {
        return complexTbl(tableName, DEFAULT_TBL_SIZE,
                IgniteDistributions.affinity(ImmutableIntList.of(0, 1), nextTableId(), DEFAULT_ZONE_ID));
    }

    private static IgniteTable complexTbl(String tableName, int size, IgniteDistribution distribution) {
        return TestBuilders.table()
                .name(tableName)
                .size(size)
                .distribution(distribution)
                .addColumn("ID1", NativeTypes.INT32)
                .addColumn("ID2", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .sortedIndex()
                .name("PK")
                .addColumn("ID1", Collation.ASC_NULLS_LAST)
                .addColumn("ID2", Collation.ASC_NULLS_LAST)
                .end()
                .build();
    }
}
