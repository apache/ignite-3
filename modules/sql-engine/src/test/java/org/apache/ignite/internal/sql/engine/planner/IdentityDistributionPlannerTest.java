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

import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify join colocation when using
 * {@link org.apache.ignite.internal.sql.engine.trait.DistributionFunction.IdentityDistribution} function.
 */
public class IdentityDistributionPlannerTest extends AbstractPlannerTest {

    /**
     * Join of the tables with same affinity is expected to be colocated.
     */
    @Test
    public void joinSameTableIdentity() throws Exception {
        IgniteTable tbl1 = simpleTable("TEST_TBL1", DEFAULT_TBL_SIZE, IgniteDistributions.identity(0));
        IgniteTable tbl2 = simpleTable("TEST_TBL2", DEFAULT_TBL_SIZE, IgniteDistributions.identity(0));

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select count(*) "
                + "from TEST_TBL1 t1 "
                + "join TEST_TBL2 t2 on t1.id = t2.id";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                        .and(hasDistribution(IgniteDistributions.identity(0))
                                // This is projection of complexTbl distribution on the right side of join.
                                // That is, for this equi-join, distribution might be either equal to one on the left side
                                // or to its counterpart derived as projection of distribution keys of left side through
                                // join pairs on right side.
                                .or(hasDistribution(IgniteDistributions.identity(1))))
                        .and(input(0, isInstanceOf(IgniteIndexScan.class)))
                        .and(input(1, isInstanceOf(IgniteIndexScan.class)))
                ))
        ));
    }

    /**
     * Re-hashing based on affinity.
     */
    @Test
    public void joinAffinityTableWithIdentityTable() throws Exception {
        IgniteDistribution affinityDistribution = TestBuilders.affinity(0, nextTableId(), 0);

        IgniteTable tbl1 = simpleTable("TEST_TBL1", 2 * DEFAULT_TBL_SIZE, IgniteDistributions.identity(0));
        IgniteTable tbl2 = simpleTable("TEST_TBL2", DEFAULT_TBL_SIZE, affinityDistribution);

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select count(*) "
                + "from TEST_TBL1 t1 "
                + "join TEST_TBL2 t2 on t1.id = t2.id";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasDistribution(single()))
                .and(input(0, isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteIndexScan.class)))
                ))
                .and(input(1, isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteIndexScan.class)))
                ))
        ));
    }

    /**
     * Re-hashing based on identity function.
     */
    @Test
    public void joinIndentityTableWithAffinityTable() throws Exception {
        IgniteDistribution affinityDistribution = TestBuilders.affinity(0, nextTableId(), 0);

        IgniteTable tbl1 = simpleTable("TEST_TBL1", DEFAULT_TBL_SIZE, IgniteDistributions.identity(0));
        IgniteTable tbl2 = simpleTable("TEST_TBL2", 2 * DEFAULT_TBL_SIZE, affinityDistribution);

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select count(*) "
                + "from TEST_TBL1 t1 "
                + "join TEST_TBL2 t2 on t1.id = t2.id";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasDistribution(single()))
                .and(input(0, isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteIndexScan.class)))
                ))
                .and(input(1, isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteIndexScan.class)))
                ))
        ));
    }

    /**
     * Join of the tables with same affinity is expected to be colocated.
     */
    @Test
    public void joinTableWithDifferentIdentity() throws Exception {
        IgniteTable tbl1 = complexTable("TEST_TBL1", DEFAULT_TBL_SIZE, IgniteDistributions.identity(0));
        IgniteTable tbl2 = complexTable("TEST_TBL2", DEFAULT_TBL_SIZE, IgniteDistributions.identity(1));

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select count(*) "
                + "from TEST_TBL1 t1 "
                + "join TEST_TBL2 t2 on t1.id1 = t2.id2";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                        .and(hasDistribution(IgniteDistributions.identity(0))
                                // This is projection of complexTbl distribution on the right side of join.
                                // That is, for this equi-join, distribution might be either equal to one on the left side
                                // or to its counterpart derived as projection of distribution keys of left side through
                                // join pairs on right side.
                                .or(hasDistribution(IgniteDistributions.identity(1))))
                        .and(input(0, isInstanceOf(IgniteIndexScan.class)))
                        .and(input(1, isInstanceOf(IgniteIndexScan.class)))
                ))
        ));
    }

    /**
     * Join of the tables with identity and broadcast distribution is expected to be colocated.
     */
    @Test
    public void joinTableWithIdentityAndBroadcastDistributions() throws Exception {
        IgniteTable tbl1 = complexTable("TEST_TBL1", DEFAULT_TBL_SIZE, IgniteDistributions.identity(0));
        IgniteTable tbl2 = complexTable("TEST_TBL2", DEFAULT_TBL_SIZE, IgniteDistributions.broadcast());

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select count(*) "
                + "from TEST_TBL1 t1 "
                + "join TEST_TBL2 t2 on t1.id1 = t2.id1";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                        .and(hasDistribution(IgniteDistributions.identity(0))
                                // This is projection of complexTbl distribution on the right side of join.
                                // That is, for this equi-join, distribution might be either equal to one on the left side
                                // or to its counterpart derived as projection of distribution keys of left side through
                                // join pairs on right side.
                                .or(hasDistribution(IgniteDistributions.identity(1))))
                        .and(input(0, isInstanceOf(IgniteIndexScan.class)))
                        .and(input(1, isInstanceOf(IgniteTrimExchange.class)
                                .and(input(isInstanceOf(IgniteIndexScan.class)))
                        ))
                ))
        ));
    }

    private static IgniteTable simpleTable(String tableName, int size, IgniteDistribution distribution) {
        return TestBuilders.table()
                .name(tableName)
                .size(size)
                .distribution(distribution)
                .addColumn("ID", NativeTypes.STRING)
                .addColumn("VAL", NativeTypes.STRING)
                .sortedIndex()
                .name("PK")
                .addColumn("ID", Collation.ASC_NULLS_LAST)
                .end()
                .build();
    }

    private static IgniteTable complexTable(String tableName, int size, IgniteDistribution distribution) {
        return TestBuilders.table()
                .name(tableName)
                .size(size)
                .distribution(distribution)
                .addColumn("ID1", NativeTypes.STRING)
                .addColumn("ID2", NativeTypes.STRING)
                .addColumn("VAL", NativeTypes.STRING)
                .sortedIndex()
                .name("IDX1")
                .addColumn("ID1", Collation.ASC_NULLS_LAST)
                .end()
                .sortedIndex()
                .name("IDX2")
                .addColumn("ID2", Collation.ASC_NULLS_LAST)
                .end()
                .build();
    }
}
