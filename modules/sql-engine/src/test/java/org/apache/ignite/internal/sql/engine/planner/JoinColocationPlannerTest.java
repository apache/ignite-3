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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
        assertThat(invalidPlanMsg, join.distribution().isTableDistribution(), is(true));
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
        assertThat(invalidPlanMsg, join.distribution().isTableDistribution(), is(true));
    }

    /**
     * Join of the same tables with a complex affinity is expected to be colocated.
     */
    @ParameterizedTest(name = "DISABLED: {0}")
    @ValueSource(strings = {"HashJoinConverter", "MergeJoinConverter"})
    public void joinSameTableComplexAff(String disabledRule) throws Exception {
        IgniteTable tbl = complexTbl("TEST_TBL");

        IgniteSchema schema = createSchema(tbl);

        String sql = "select count(*) "
                + "from TEST_TBL t1 "
                + "join TEST_TBL t2 on t1.id1 = t2.id1 and t1.id2 = t2.id2";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", disabledRule);

        AbstractIgniteJoin join = findFirstNode(phys, byClass(AbstractIgniteJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().isTableDistribution(), is(true));

        if (!"MergeJoinConverter".equals(disabledRule)) {
            assertThat(invalidPlanMsg, join.getLeft(), instanceOf(IgniteIndexScan.class));
            assertThat(invalidPlanMsg, join.getRight(), instanceOf(IgniteIndexScan.class));
        }
    }

    /**
     * Join of the same tables with a complex affinity is expected to be colocated.
     */
    @ParameterizedTest(name = "DISABLED: {0}")
    @ValueSource(strings = {"HashJoinConverter", "MergeJoinConverter"})
    public void joinDiffTablesComplexAff(String disabledRule) throws Exception {
        IgniteTable tbl1 = complexTbl("TEST_TBL1");
        IgniteTable tbl2 = complexTbl("TEST_TBL2");

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select count(*) "
                + "from TEST_TBL1 t1 "
                + "join TEST_TBL2 t2 on t1.id1 = t2.id1 and t1.id2 = t2.id2";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", disabledRule);

        AbstractIgniteJoin join = findFirstNode(phys, byClass(AbstractIgniteJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().isTableDistribution(), is(true));

        if (!"MergeJoinConverter".equals(disabledRule)) {
            assertThat(invalidPlanMsg, join.getLeft(), instanceOf(IgniteIndexScan.class));
            assertThat(invalidPlanMsg, join.getRight(), instanceOf(IgniteIndexScan.class));
        }
    }

    /**
     * Join of the same tables with a complex affinity is expected to be colocated.
     */
    @ParameterizedTest(name = "DISABLED: {0}")
    @ValueSource(strings = {"HashJoinConverter", "MergeJoinConverter"})
    public void joinDiffTablesNotSupersetComplexAff(String disabledRule) throws Exception {
        IgniteTable tbl1 = complexTbl("TEST_TBL1");
        IgniteTable tbl2 = complexTbl("TEST_TBL2");

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select count(*) "
                + "from TEST_TBL1 t1 "
                + "join TEST_TBL2 t2 on t1.id1 = t2.id1";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", disabledRule);

        AbstractIgniteJoin join = findFirstNode(phys, byClass(AbstractIgniteJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().isTableDistribution(), is(false));
    }

    /**
     * Re-hashing based on simple affinity.
     *
     * <p>The smaller table should be sent to the bigger one.
     */
    @Test
    public void joinComplexToSimpleAff() throws Exception {
        int tableId = nextTableId();
        IgniteTable complexTbl = complexTbl("COMPLEX_TBL", 2 * DEFAULT_TBL_SIZE,
                TestBuilders.affinity(ImmutableIntList.of(0, 1), tableId, DEFAULT_ZONE_ID));

        IgniteTable simpleTbl = simpleTable("SIMPLE_TBL", DEFAULT_TBL_SIZE);

        IgniteSchema schema = createSchema(complexTbl, simpleTbl);

        String sql = "select count(*) "
                + "from COMPLEX_TBL t1 "
                + "join SIMPLE_TBL t2 on t1.id1 = t2.id and t1.id2 = t2.id2";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasDistribution(complexTbl.distribution())
                        // This is projection of complexTbl distribution on the right side of join.
                        // That is, for this equi-join, distribution might be either equal to one on the left side
                        // or to its counterpart derived as projection of distribution keys of left side through
                        // join pairs on right side.
                        .or(hasDistribution(TestBuilders.affinity(ImmutableIntList.of(2, 3), tableId, DEFAULT_ZONE_ID))))
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

    // Test cases in this test may emit multiple equivalent plan.
    // It's not deterministic because for equivalent options the order
    // sometimes determined by the order of entries in hash-based collection
    // (like HashMap). Let's repeat this test a few times to cover as much as
    // possible. This test is relatively fast.
    @RepeatedTest(10)
    void joinOutputDistribution() throws Exception {
        int bigTableId = nextTableId();
        IgniteTable bigTable = table(
                "BIG_TBL", 2 * DEFAULT_TBL_SIZE, bigTableId,
                "ID", "C1", "C2", "C3"
        );
        int smallTableId = nextTableId();
        IgniteTable smallTable = table(
                "SMALL_TBL", DEFAULT_TBL_SIZE, smallTableId,
                "ID", "C1", "C2", "C3", "C4"
        );

        IgniteSchema schema = createSchema(bigTable, smallTable);

        {
            // Case: Inner join, big table on the left.
            // Expected output distribution either original from big table, or projection
            // of big table distribution to right side.
            String sql = "SELECT /*+ enforce_join_order */ "
                    + " COUNT(bt.id + bt.c1 + bt.c2 + bt.c3 + st.id + st.c1 + st.c2 + st.c3 + st.c4) "
                    + "FROM big_tbl bt "
                    + "JOIN small_tbl st ON bt.id = st.c4";

            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasDistribution(bigTable.distribution())
                            .or(hasDistribution(TestBuilders.affinity(ImmutableIntList.of(
                                    4 /* index of C4 column in small_table */
                                            + bigTable.descriptor().rowTypeSansHidden().getFieldCount()
                            ), bigTableId, DEFAULT_ZONE_ID)))
                    )
            ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
        }

        {
            // Case: Inner join, big table on the right.
            // Expected output distribution either distribution of big table shifted
            // by the size of small table, or projection of big table distribution to
            // left side.
            String sql = "SELECT /*+ enforce_join_order */ "
                    + " COUNT(bt.id + bt.c1 + bt.c2 + bt.c3 + st.id + st.c1 + st.c2 + st.c3 + st.c4) "
                    + "FROM small_tbl st "
                    + "JOIN big_tbl bt ON bt.id = st.c4";

            TargetMapping offsetMapping = Commons.targetOffsetMapping(
                    bigTable.descriptor().rowTypeSansHidden().getFieldCount(),
                    smallTable.descriptor().rowTypeSansHidden().getFieldCount()
            );

            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasDistribution(bigTable.distribution().apply(offsetMapping))
                            .or(hasDistribution(TestBuilders.affinity(ImmutableIntList.of(
                                    4 /* index of C4 column in small_table */
                            ), bigTableId, DEFAULT_ZONE_ID)))
                    )
            ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
        }

        {
            // Case: Left join, big table on the left.
            // Expected output distribution is original from big table only. Projection
            // of big table distribution is not expected since left join may emit NULLS on right side.
            String sql = "SELECT /*+ enforce_join_order */ "
                    + " COUNT(bt.id + bt.c1 + bt.c2 + bt.c3 + st.id + st.c1 + st.c2 + st.c3 + st.c4) "
                    + "FROM big_tbl bt "
                    + "LEFT JOIN small_tbl st ON bt.id = st.c4";

            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasDistribution(bigTable.distribution()))
            ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
        }

        {
            // Case: Left join, big table on the right.
            // Expected output distribution is projection of big table distribution to
            // left side only. Distribution of big table shifted by the size of small
            // table is not expected since left join may emit NULLS on right side.
            String sql = "SELECT /*+ enforce_join_order */ "
                    + " COUNT(bt.id + bt.c1 + bt.c2 + bt.c3 + st.id + st.c1 + st.c2 + st.c3 + st.c4) "
                    + "FROM small_tbl st "
                    + "LEFT JOIN big_tbl bt ON bt.id = st.c4";

            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasDistribution(TestBuilders.affinity(ImmutableIntList.of(
                                    4 /* index of C4 column in small_table */
                            ), bigTableId, DEFAULT_ZONE_ID))
                    )
            ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
        }

        {
            // Case: Right join, big table on the left.
            // Expected output distribution is projection of big table distribution to
            // right side only. Distribution of big table is not expected since right
            // join may emit NULLS on left side.
            String sql = "SELECT /*+ enforce_join_order */ "
                    + " COUNT(bt.id + bt.c1 + bt.c2 + bt.c3 + st.id + st.c1 + st.c2 + st.c3 + st.c4) "
                    + "FROM big_tbl bt "
                    + "RIGHT JOIN small_tbl st ON bt.id = st.c4";

            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasDistribution(TestBuilders.affinity(ImmutableIntList.of(
                                    4 /* index of C4 column in small_table */
                                            + bigTable.descriptor().rowTypeSansHidden().getFieldCount()
                            ), bigTableId, DEFAULT_ZONE_ID))
                    )
            ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
        }

        {
            // Case: Right join, big table on the right.
            // Here we expect smaller table to be sent to bigger table.
            // Hence, expected distribution is distribution of big table shifted
            // by the size of small table only. Projection of big table distribution is not
            // expected since right join may emit NULLS on left side.
            String sql = "SELECT /*+ enforce_join_order */ "
                    + " COUNT(bt.id + bt.c1 + bt.c2 + bt.c3 + st.id + st.c1 + st.c2 + st.c3 + st.c4) "
                    + "FROM small_tbl st "
                    + "RIGHT JOIN big_tbl bt ON bt.id = st.c4";

            TargetMapping offsetMapping = Commons.targetOffsetMapping(
                    bigTable.descriptor().rowTypeSansHidden().getFieldCount(),
                    smallTable.descriptor().rowTypeSansHidden().getFieldCount()
            );

            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasDistribution(bigTable.distribution().apply(offsetMapping)))
            ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
        }

        {
            // Case: Outer join, big table on the left.
            // Expected output distribution is random since outer join may emit NULLS on both sides.
            String sql = "SELECT /*+ enforce_join_order */ "
                    + " COUNT(bt.id + bt.c1 + bt.c2 + bt.c3 + st.id + st.c1 + st.c2 + st.c3 + st.c4) "
                    + "FROM big_tbl bt "
                    + "FULL OUTER JOIN small_tbl st ON bt.id = st.c4";

            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasDistribution(IgniteDistributions.random()))
            ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
        }

        {
            // Case: Outer join, big table on the right.
            // Expected output distribution is random since outer join may emit NULLS on both sides.
            String sql = "SELECT /*+ enforce_join_order */ "
                    + " COUNT(bt.id + bt.c1 + bt.c2 + bt.c3 + st.id + st.c1 + st.c2 + st.c3 + st.c4) "
                    + "FROM small_tbl st "
                    + "FULL OUTER JOIN big_tbl bt ON bt.id = st.c4";

            assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasDistribution(IgniteDistributions.random()))
            ), "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter");
        }
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
                TestBuilders.affinity(ImmutableIntList.of(0, 1), nextTableId(), DEFAULT_ZONE_ID));

        IgniteTable complexTblIndirect = complexTbl(
                "COMPLEX_TBL_INDIRECT",
                DEFAULT_TBL_SIZE,
                TestBuilders.affinity(ImmutableIntList.of(1, 0), nextTableId(), DEFAULT_ZONE_ID));

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

    static IgniteTable simpleTable(String tableName, int size) {
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

    static IgniteTable simpleTableHashPk(String tableName, int size) {
        return TestBuilders.table()
                .name(tableName)
                .size(size)
                .distribution(someAffinity())
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("ID2", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.STRING)
                .hashIndex()
                .name("PK")
                .addColumn("ID")
                .end()
                .build();
    }

    static IgniteTable complexTbl(String tableName) {
        return complexTbl(tableName, DEFAULT_TBL_SIZE,
                TestBuilders.affinity(ImmutableIntList.of(0, 1), nextTableId(), DEFAULT_ZONE_ID));
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

    private static IgniteTable table(String tableName, int size, int tableId, String... columnNames) {
        TableBuilder builder = TestBuilders.table()
                .name(tableName)
                .size(size)
                .distribution(TestBuilders.affinity(ImmutableIntList.of(0), tableId, DEFAULT_ZONE_ID))
                .sortedIndex()
                .name("PK")
                .addColumn(columnNames[0], Collation.ASC_NULLS_LAST)
                .end();

        for (String name : columnNames) {
            builder.addColumn(name, NativeTypes.INT32);
        }

        return builder.build();
    }
}
