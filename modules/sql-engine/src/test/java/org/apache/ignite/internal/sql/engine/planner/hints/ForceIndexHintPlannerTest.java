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

package org.apache.ignite.internal.sql.engine.planner.hints;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.function.UnaryOperator;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Planner test for force index hint.
 */
public class ForceIndexHintPlannerTest extends AbstractPlannerTest {
    private static IgniteSchema SCHEMA;

    private static final String TBL1 = "TBL1";

    private static final String TBL2 = "TBL2";

    @BeforeAll
    public static void setup() {
        SCHEMA = createSchemaFrom(
                createSimpleTable(TBL1, 100)
                        .andThen(addHashIndex("ID"))
                        .andThen(addSortIndex("VAL1"))
                        .andThen(addSortIndex("VAL2", "VAL3"))
                        .andThen(addSortIndex("VAL3")),
                createSimpleTable(TBL2, 100_000)
                        .andThen(addHashIndex("ID"))
                        .andThen(addSortIndex("VAL1"))
                        .andThen(addSortIndex("VAL2"))
                        .andThen(addSortIndex("VAL3"))
        );
    }

    @Test
    public void testWrongIndexName() throws Exception {
        var sql = "SELECT /*+ FORCE_INDEX({}) */ * FROM TBL1 WHERE id = 0 AND val1 = 'v'";

        assertNoCertainIndex(format(sql, "'tbl1_idx_id'"), TBL1, "TBL1_IDX_ID");
        assertNoCertainIndex(format(sql, "\"tbl1_idx_id\""), TBL1, "TBL1_IDX_ID");
        assertNoCertainIndex(format(sql, "'unexisting', 'tbl1_idx_id'"), TBL1, "UNEXISTING");
        assertNoCertainIndex(format(sql, "\"unexisting\", \"tbl1_idx_id\""), TBL1, "UNEXISTING");
    }

    @Test
    public void testSingleTable() throws Exception {
        var sql = "SELECT /*+ FORCE_INDEX({}) */ * FROM TBL1 WHERE val2 = 'v' AND val3 = 'v'";

        assertCertainIndex(format(sql, ""), TBL1, "IDX_VAL2_VAL3");
        assertCertainIndex(format(sql, "unexisting, idx_val3"), TBL1, "IDX_VAL3");
        assertCertainIndex(format(sql, "UNEXISTING, IDX_VAL3"), TBL1, "IDX_VAL3");
        assertCertainIndex(format(sql, "'UNEXISTING', 'IDX_VAL3'"), TBL1, "IDX_VAL3");
        assertCertainIndex(format(sql, "\"UNEXISTING\", \"IDX_VAL3\""), TBL1, "IDX_VAL3");

        assertPlan(format(sql, "IDX_VAL2_VAL3, IDX_VAL3"), SCHEMA, nodeOrAnyChild(isIndexScan(TBL1, "IDX_VAL2_VAL3")
                .or(isIndexScan(TBL1, "IDX_VAL3"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX_VAL2_VAL3), FORCE_INDEX(IDX_VAL3) */ * FROM TBL1 WHERE val2 = 'v' AND val3 = 'v'", SCHEMA,
                nodeOrAnyChild(isIndexScan(TBL1, "IDX_VAL2_VAL3").or(isIndexScan(TBL1, "IDX_VAL3"))));
    }

    @Test
    public void testSubquery() throws Exception {
        var sql1 = "SELECT /*+ FORCE_INDEX({}) */ * FROM TBL1 t2, (SELECT * FROM TBL1 WHERE val2='v' AND val3='v') t1 WHERE t2.val2='v'";

        assertCertainIndex(format(sql1, ""), TBL1, "IDX_VAL2_VAL3");
        assertCertainIndex(format(sql1, "IDX_VAL2_VAL3"), TBL1, "IDX_VAL2_VAL3");
        assertCertainIndex(format(sql1, "IDX_VAL3"), TBL1, "IDX_VAL3");

        var sql2 = "SELECT /*+ FORCE_INDEX({}) */ * FROM TBL2 t2 WHERE t2.val2 = (SELECT val2 from TBL1 WHERE val2='v' AND val3='v')";

        assertCertainIndex(format(sql2, ""), TBL1, "IDX_VAL2_VAL3");
        assertCertainIndex(format(sql2, "IDX_VAL2_VAL3"), TBL1, "IDX_VAL2_VAL3");
        assertCertainIndex(format(sql2, "IDX_VAL3"), TBL1, "IDX_VAL3");
    }

    @Test
    public void testJoins() throws Exception {
        var sql1 = "SELECT /*+ FORCE_INDEX({}) */ * FROM TBL2 t2 INNER JOIN TBL1 t1 on t1.val2=t2.val2 AND t1.val3=t2.val3";

        assertCertainIndex(format(sql1, "IDX_VAL2_VAL3"), TBL1, "IDX_VAL2_VAL3");
        assertCertainIndex(format(sql1, "IDX_VAL3"), TBL1, "IDX_VAL3");

        var sql2 = "SELECT /*+ FORCE_INDEX({}) */ * FROM TBL1 t1, TBL2 t2 WHERE t1.val2=t2.val2 AND t1.val3=t2.val3";

        assertCertainIndex(format(sql2, "IDX_VAL2_VAL3"), TBL1, "IDX_VAL2_VAL3");
        assertCertainIndex(format(sql2, "IDX_VAL3"), TBL1, "IDX_VAL3");
    }

    @Test
    public void testOrderBy() throws Exception {
        assertCertainIndex("SELECT /*+ FORCE_INDEX(IDX_VAL1) */ val2, val3 FROM TBL1 ORDER by val2,val1,val3", TBL1, "IDX_VAL1");
        assertCertainIndex("SELECT /*+ FORCE_INDEX(IDX_VAL2_VAL3) */ val2, val3 FROM TBL1 ORDER by val2,val1,val3", TBL1,
                "IDX_VAL2_VAL3");
        assertCertainIndex("SELECT /*+ FORCE_INDEX(IDX_VAL3) */ val2, val3 FROM TBL1 ORDER by val2,val1,val3", TBL1, "IDX_VAL3");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SUM", "AVG", "MIN", "MAX"})
    public void testAggregates(String op) throws Exception {
        var sql = "SELECT /*+ FORCE_INDEX({}) */ {}(val1) FROM TBL1 where val1=1 group by val2";

        assertCertainIndex(format(sql, "IDX_VAL1", op), TBL1, "IDX_VAL1");
        assertCertainIndex(format(sql, "IDX_VAL2_VAL3", op), TBL1, "IDX_VAL2_VAL3");
        assertPlan(format(sql, "IDX_VAL1, IDX_VAL2_VAL3", op), SCHEMA, nodeOrAnyChild(isIndexScan(TBL1, "IDX_VAL1")
                .or(nodeOrAnyChild(isIndexScan(TBL1, "IDX_VAL2_VAL3")))));
    }

    @Test
    public void testWithMultipleIndexHints() throws Exception {
        // TODO IGNITE-21404 Replace assertThrowsWithCause with assertThrowsSqlException.
        IgniteTestUtils.assertThrowsWithCause(
                () -> physicalPlan("SELECT /*+ FORCE_INDEX(IDX_VAL3), NO_INDEX */ * FROM TBL1 WHERE val2='v' AND val3='v'", SCHEMA),
                SqlException.class,
                "Indexes [IDX_VAL3] of table 'TBL1' has already been forced to use by other hints before."
        );

        IgniteTestUtils.assertThrowsWithCause(
                () -> physicalPlan("SELECT /*+ NO_INDEX, FORCE_INDEX(IDX_VAL3) */ * FROM TBL1 WHERE val2='v' AND val3='v'", SCHEMA),
                SqlException.class,
                "Index 'IDX_VAL3' of table 'TBL1' has already been disabled in other hints."
        );

        assertCertainIndex("SELECT /*+ NO_INDEX(IDX_VAL1), FORCE_INDEX(IDX_VAL3), NO_INDEX(IDX_VAL2_VAL3) */ * FROM TBL1 WHERE  val1='v'"
                + " AND val2='v' AND val3='v'", TBL1, "IDX_VAL3");

        IgniteTestUtils.assertThrowsWithCause(
                () -> physicalPlan("SELECT /*+ NO_INDEX(IDX_VAL1), FORCE_INDEX(IDX_VAL1) */ * FROM TBL1 WHERE val1='v'", SCHEMA),
                SqlException.class,
                "Index 'IDX_VAL1' of table 'TBL1' has already been disabled in other hints."
        );
    }

    private static UnaryOperator<TableBuilder> createSimpleTable(String name, int sz) {
        return t -> t.name(name)
                .size(sz)
                .distribution(IgniteDistributions.single())
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("VAL1", NativeTypes.STRING)
                .addColumn("VAL2", NativeTypes.STRING)
                .addColumn("VAL3", NativeTypes.STRING);
    }

    private void assertCertainIndex(String sql, String tblName, String idxName) throws Exception {
        assertPlan(sql, SCHEMA, nodeOrAnyChild(isIndexScan(tblName, idxName)));
    }

    private void assertNoCertainIndex(String sql, String tblName, String idxName) throws Exception {
        assertPlan(sql, SCHEMA, nodeOrAnyChild(isIndexScan(tblName, idxName)).negate());
    }
}
