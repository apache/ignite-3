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

import static java.lang.String.format;

import java.util.function.UnaryOperator;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Planner test for index hints.
 */
public class NoIndexHintPlannerTest extends AbstractPlannerTest {
    private static IgniteSchema SCHEMA;

    private static final String TBL1 = "TBL1";

    private static final String TBL2 = "TBL2";

    @BeforeAll
    public static void setup() {
        SCHEMA = createSchemaFrom(
                createSimpleTable(TBL1, 100)
                        .andThen(addUniqueHashIndex("TBL1_IDX", "ID"))
                        .andThen(addUniqueSortIndex("TBL1_IDX", "VAL1"))
                        .andThen(addUniqueSortIndex("TBL1_IDX", "VAL2", "VAL3"))
                        .andThen(addUniqueSortIndex("TBL1_IDX", "VAL3")),
                createSimpleTable(TBL2, 100_000)
                        .andThen(addUniqueHashIndex("TBL2_IDX", "ID"))
                        .andThen(addUniqueSortIndex("TBL2_IDX", "VAL1"))
                        .andThen(addUniqueSortIndex("TBL2_IDX", "VAL2"))
                        .andThen(addUniqueSortIndex("TBL2_IDX", "VAL3"))
        );
    }

    @Test
    public void testWrongIndexName() throws Exception {
        assertCertainIndex("SELECT /*+ NO_INDEX('tbl1_idx_id') */ * FROM TBL1 WHERE id = 0", TBL1, "TBL1_IDX_ID");
        assertCertainIndex("SELECT /*+ NO_INDEX(\"tbl1_idx_id\") */ * FROM TBL1 WHERE id = 0", TBL1, "TBL1_IDX_ID");
        assertCertainIndex("SELECT /*+ NO_INDEX('unexisting', 'tbl1_idx_id') */ * FROM TBL1 WHERE val1='v'", TBL1, "TBL1_IDX_VAL1");
        assertCertainIndex("SELECT /*+ NO_INDEX(\"unexisting\", \"tbl1_idx_id\") */ * FROM TBL1 WHERE val1='v'", TBL1, "TBL1_IDX_VAL1");
    }

    @Test
    public void testSingleTable() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 WHERE id = 0");
        assertNoCertainIndex("SELECT /*+ NO_INDEX(unexisting, tbl1_idx_id) */ * FROM TBL1 WHERE id = 0", TBL1, "TBL1_IDX_ID");
        assertNoCertainIndex("SELECT /*+ NO_INDEX(UNEXISTING, TBL1_IDX_ID) */ * FROM TBL1 WHERE id = 0", TBL1, "TBL1_IDX_ID");
        assertNoCertainIndex("SELECT /*+ NO_INDEX('UNEXISTING', 'TBL1_IDX_ID') */ * FROM TBL1 WHERE id = 0", TBL1, "TBL1_IDX_ID");
        assertNoCertainIndex("SELECT /*+ NO_INDEX(\"UNEXISTING\", \"TBL1_IDX_ID\") */ * FROM TBL1 WHERE id = 0", TBL1, "TBL1_IDX_ID");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL1, TBL1_IDX_VAL2_VAL3, TBL1_IDX_VAL3) */ * FROM TBL1 WHERE val1='v' " +
                "and val2='v' and val3='v'");
    }

    @Test
    public void testMultipleTables() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where t2.val3=t1.val3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL2_VAL3) */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
                "val2='v') t2 WHERE t1.val2='v'", TBL1, "TBL1_IDX_VAL2_VAL3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
                "val3='v') t2 WHERE t1.val2='v'", TBL2, "TBL2_IDX_VAL3");

        assertNoCertainIndex("SELECT * FROM TBL1 t1, (select /*+ NO_INDEX(TBL2_IDX_VAL3) */ * FROM TBL2 WHERE " +
                "val3='v') t2 WHERE t1.val2='v'", TBL2, "TBL2_IDX_VAL3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL3) */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where " +
                "t2.val3=t1.val3", TBL1, "TBL1_IDX_VAL3");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL3,TBL2_IDX_VAL3) */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where " +
                "t2.val3=t1.val3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL3) */ t1.val1, t2.val2 FROM TBL1 t1 JOIN TBL2 t2 on " +
                "t1.val3=t2.val3", TBL1, "TBL1_IDX_VAL3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ t1.val1, t2.val2 FROM TBL1 t1 JOIN TBL2 t2 on " +
                "t1.val3=t2.val3", TBL2, "TBL2_IDX_VAL3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL2_IDX_VAL2) */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where t1.val2='v' " +
                "and t2.val2=t1.val2", TBL2, "TBL2_IDX_VAL2");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL2_VAL3) */ * FROM TBL1 t1 WHERE t1.val2 = " +
                "(SELECT val2 from TBL2 WHERE val3='v')", TBL1, "TBL1_IDX_VAL2_VAL3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ * FROM TBL1 t1 WHERE t1.val2 = " +
                "(SELECT val2 from TBL2 WHERE val3='v')", TBL2, "TBL2_IDX_VAL3");

        assertNoCertainIndex("SELECT * FROM TBL1 t1 WHERE t1.val2 = " +
                "(SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ val2 from TBL2 WHERE val3='v')", TBL2, "TBL2_IDX_VAL3");

        assertCertainIndex("SELECT t2.val3 FROM TBL2 t2 WHERE t2.val2 = " +
                "(SELECT /*+ NO_INDEX(TBL2_IDX_VAL2) */ t1.val2 from TBL1 t1 WHERE t1.val3='v')", TBL2, "TBL2_IDX_VAL2");

        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 t1 WHERE t1.val3 = " +
                "(SELECT val2 from TBL2 WHERE val3='v')");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL3, TBL2_IDX_VAL3) */ * FROM TBL1 t1 WHERE t1.val3 = " +
                "(SELECT val2 from TBL2 WHERE val3='v')");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL3), NO_INDEX(TBL2_IDX_VAL3) */ * FROM TBL1 t1 WHERE t1.val3 = " +
                "(SELECT val2 from TBL2 WHERE val3='v')");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1_IDX_VAL3) */ * FROM TBL1 t1 WHERE t1.val3 = " +
                "(SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ val2 from TBL2 WHERE val3='v')");
    }

    @Test
    public void testOrderBy() throws Exception {
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX_VAL2_VAL3) */ val3 FROM TBL1 order by val2, val3", TBL1, "IDX_VAL2_VAL3");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SUM", "AVG", "MIN", "MAX"})
    public void testAggregates(String op) throws Exception {
        assertNoAnyIndex(format("SELECT /*+ NO_INDEX */ %s(val1) FROM TBL2 group by val3", op));

        assertNoCertainIndex(format("SELECT /*+ NO_INDEX(IDX_VAL3) */ %s(val1) FROM TBL2 group by val3", op), TBL2, "IDX_VAL3");
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNION", "UNION ALL", "INTERSECT", "EXCEPT"})
    public void testSetOperators(String op) throws Exception {
        assertPlan(format("SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                        "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", op), SCHEMA,
                nodeOrAnyChild(isIndexScan(TBL2, "TBL2_IDX_VAL3")).and(nodeOrAnyChild(isIndexScan(TBL1, "TBL1_IDX_VAL2_VAL3"))));

        assertPlan(format("SELECT /*+ NO_INDEX(TBL1_IDX_VAL2_VAL3) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                        "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", op), SCHEMA,
                nodeOrAnyChild(isIndexScan(TBL2, "TBL2_IDX_VAL3")).and(nodeOrAnyChild(isIndexScan(TBL1, "TBL1_IDX_VAL2_VAL3")).negate()));

        assertPlan(format("SELECT /*+ NO_INDEX */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                        "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", op), SCHEMA,
                nodeOrAnyChild(isIndexScan(TBL2, "TBL2_IDX_VAL3")).and(nodeOrAnyChild(isIndexScan(TBL1, "TBL1_IDX_VAL2_VAL3")).negate()));

        assertPlan(format("SELECT t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                        "SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ t2.* FROM TBL2 t2 where t2.val3='v'", op), SCHEMA,
                nodeOrAnyChild(isIndexScan(TBL2, "TBL2_IDX_VAL3")).negate().and(nodeOrAnyChild(isIndexScan(TBL1, "TBL1_IDX_VAL2_VAL3"))));

        assertPlan(format("SELECT t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                        "SELECT /*+ NO_INDEX */ t2.* FROM TBL2 t2 where t2.val3='v'", op), SCHEMA,
                nodeOrAnyChild(isIndexScan(TBL2, "TBL2_IDX_VAL3")).negate().and(nodeOrAnyChild(isIndexScan(TBL1, "TBL1_IDX_VAL2_VAL3"))));

        assertNoCertainIndex(format("SELECT /*+ NO_INDEX(TBL1_IDX_VAL2_VAL3) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ t2.* FROM TBL2 t2 where t2.val3='v'", op), TBL1, "TBL1_IDX_VAL2_VAL3");

        assertNoCertainIndex(format("SELECT /*+ NO_INDEX(TBL1_IDX_VAL2_VAL3) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT /*+ NO_INDEX(TBL2_IDX_VAL3) */ t2.* FROM TBL2 t2 where t2.val3='v'", op), TBL2, "TBL2_IDX_VAL3");
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

    private void assertNoAnyIndex(String sql) throws Exception {
        assertPlan(sql, SCHEMA, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)).negate());
    }

    private void assertNoCertainIndex(String sql, String tblName, String idxName) throws Exception {
        assertPlan(sql, SCHEMA, nodeOrAnyChild(isIndexScan(tblName, idxName)).negate());
    }

    private void assertCertainIndex(String sql, String tblName, String idxName) throws Exception {
        assertPlan(sql, SCHEMA, nodeOrAnyChild(isIndexScan(tblName, idxName)));
    }
}
