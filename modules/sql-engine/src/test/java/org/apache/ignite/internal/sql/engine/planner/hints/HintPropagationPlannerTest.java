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

import java.util.function.UnaryOperator;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Planner tests for index/no_index hints propagation.
 */
public class HintPropagationPlannerTest extends AbstractPlannerTest {
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
    public void usingDifferentIndexesForSameTable() throws Exception {
        var sql = "SELECT * FROM"
                + " (SELECT t1.val1, t1.val2, t1.val3 FROM tbl1 /*+ NO_INDEX */ as t1"
                + " LEFT JOIN tbl2 /*+ FORCE_INDEX(idx_val3) */ as t2 ON (t1.val2 = t2.val2 AND t2.val2 > 'x')) as t"
                + " LEFT JOIN tbl2 /*+ FORCE_INDEX(idx_val2) */ as t3 ON (t.val3 = t3.val3 AND t3.val3 < 'a')";

        assertPlan(sql, SCHEMA, isInstanceOf(AbstractIgniteJoin.class)
                .and(input(0, nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)
                        .and(input(0, nodeOrAnyChild(isInstanceOf(IgniteTableScan.class))))
                        .and(input(1, nodeOrAnyChild(isIndexScan(TBL2, "IDX_VAL3")
                                .and(scan -> scan.searchBounds() == null)
                                .and(scan -> ">($t0, _UTF-8'x')".equals(scan.condition().toString()))
                        )))
                )))
                .and(input(1, nodeOrAnyChild(isIndexScan(TBL2, "IDX_VAL2")
                        .and(scan -> scan.searchBounds() == null)
                        .and(scan -> "<($t3, _UTF-8'a')".equals(scan.condition().toString()))
                )))
        );

        sql = "SELECT * FROM"
                + " (SELECT /*+ NO_INDEX */ t1.val1, t1.val2, t1.val3 FROM tbl1 as t1 LEFT JOIN tbl2 as t2 ON (t1.val2 = t2.val2)) as t"
                + " LEFT JOIN tbl2 /*+ FORCE_INDEX(idx_val2) */ as t3 ON (t.val3 = t3.val3)";

        assertPlan(sql, SCHEMA, isInstanceOf(AbstractIgniteJoin.class)
                .and(input(0, nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)
                        .and(input(0, nodeOrAnyChild(isInstanceOf(IgniteTableScan.class))))
                        .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableScan.class))))
                )))
                .and(input(1, nodeOrAnyChild(isIndexScan(TBL2, "IDX_VAL2"))))
        );
    }

    @Test
    public void testHintOverriding() throws Exception {
        var sql = "SELECT /*+ NO_INDEX */ * FROM"
                + " (SELECT t1.val1, t1.val2, t1.val3 FROM tbl1 as t1 LEFT JOIN tbl2 as t2 ON (t1.val2 = t2.val2)) as t"
                + " LEFT JOIN tbl2 /*+ FORCE_INDEX(idx_val2_val3) */ as t3 ON (t.val3 = t3.val3)";

        assertPlan(sql, SCHEMA, isInstanceOf(AbstractIgniteJoin.class)
                .and(input(0, nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)
                        .and(input(0, nodeOrAnyChild(isInstanceOf(IgniteTableScan.class))))
                        .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableScan.class))))
                )))
                .and(input(1, nodeOrAnyChild(isIndexScan(TBL2, "idx_val3"))))
        );

        sql = "SELECT /*+ FORCE_INDEX(idx_val3) */ * FROM"
                + " (SELECT t1.val1, t1.val2, t1.val3 FROM tbl1 /*+ NO_INDEX */ as t1 "
                + " LEFT JOIN tbl2 as t2 ON (t1.val2 = t2.val2)) as t"
                + " LEFT JOIN tbl2 /*+ FORCE_INDEX(idx_val2) */as t3 ON (t.val3 = t3.val3)";

        assertPlan(sql, SCHEMA, isInstanceOf(AbstractIgniteJoin.class)
                .and(input(0, nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)
                        .and(input(0, nodeOrAnyChild(isInstanceOf(IgniteTableScan.class))))
                        .and(input(1, nodeOrAnyChild(isIndexScan(TBL2, "IDX_VAL3"))))
                )))
                .and(input(1, nodeOrAnyChild(isIndexScan(TBL2, "IDX_VAL2"))))
        );
    }

    private static UnaryOperator<TableBuilder> createSimpleTable(String name, int sz) {
        return t -> t.name(name)
                .size(sz)
                .distribution(IgniteDistributions.single())
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("VAL1", NativeTypes.INT32)
                .addColumn("VAL2", NativeTypes.STRING)
                .addColumn("VAL3", NativeTypes.STRING);
    }
}
