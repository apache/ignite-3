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
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeAll;
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
                createSimpleTable(TBL1, 10)
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

    private static UnaryOperator<TableBuilder> createSimpleTable(String name, int sz) {
        return t -> t.name(name)
                .size(sz)
                .distribution(IgniteDistributions.single())
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("VAL1", NativeTypes.STRING)
                .addColumn("VAL2", NativeTypes.STRING)
                .addColumn("VAL3", NativeTypes.STRING);
    }

    @ParameterizedTest
    @ValueSource(strings = {"SUM", "AVG", "MIN", "MAX"})
    public void testAggregates(String op) throws Exception {
        assertCertainIndex(format("SELECT /*+ FORCE_INDEX(TBL1_IDX_VAL2_VAL3) */ {}(val1) FROM TBL1 where val1=1 group by val2", op), TBL1,
                "TBL1_IDX_VAL2_VAL3");

        assertCertainIndex(format("SELECT /*+ FORCE_INDEX(TBL1_IDX_VAL2_VAL3) */ {}(val1) FROM TBL1 group by val2", op), TBL1,
                "TBL1_IDX_VAL2_VAL3");

        assertCertainIndex(format("SELECT /*+ FORCE_INDEX(TBL1_IDX_VAL1) */ {}(val1) FROM TBL1 where val1=1 group by val2", op), TBL1,
                "TBL1_IDX_VAL1");

        assertCertainIndex(format("SELECT /*+ FORCE_INDEX(TBL1_IDX_VAL2_VAL3) */ {}(val1) FROM TBL1 where val1=1 group by val2", op), TBL1,
                "TBL1_IDX_VAL2_VAL3");

        assertPlan(format("SELECT /*+ FORCE_INDEX(TBL1_IDX_VAL1, TBL2_IDX_VAL2) */ {}(val1) FROM TBL1 where val1=1 group by val2", op),
                SCHEMA, nodeOrAnyChild(isIndexScan(TBL1, "TBL1_IDX_VAL2")).or(nodeOrAnyChild(isIndexScan(TBL1, "TBL1_IDX_VAL1"))));
    }

    private void assertCertainIndex(String sql, String tblName, String idxName) throws Exception {
        assertPlan(sql, SCHEMA, nodeOrAnyChild(isIndexScan(tblName, idxName)));
    }
}
