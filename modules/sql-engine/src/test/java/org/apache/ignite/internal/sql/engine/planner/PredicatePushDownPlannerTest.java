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

import org.apache.calcite.rel.core.Join;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * Set of tests to verify predicate push down optimization.
 */
@SuppressWarnings("ConcatenationWithEmptyString")
public class PredicatePushDownPlannerTest extends AbstractPlannerTest {
    @Test
    protected void predicatePushedUnderCorrelate() throws Exception {
        IgniteSchema schema = createSchema(
                createTable("T")
        );

        String sql = ""
                + " SELECT /*+ disable_decorrelation */ * "
                + "   FROM t ot"
                + "  WHERE ot.c2 > 10"
                + "    AND EXISTS ("
                + "         SELECT *"
                + "           FROM t it"
                + "          WHERE ot.c1 = it.c1"
                + "            AND it.c2 > it.c3)";

        assertPlan(sql, schema, isInstanceOf(Join.class)
                .and(hasChildThat(isInstanceOf(ProjectableFilterableTableScan.class)
                        .and(scan -> scan.condition().toString().contains(">($t1, 10)")))));

    }

    private static IgniteTable createTable(String tableName) {
        return TestBuilders.table()
                .name(tableName)
                .addColumn("C1", NativeTypes.INT32)
                .addColumn("C2", NativeTypes.INT32)
                .addColumn("C3", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();
    }
}
