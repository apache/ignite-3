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

import static org.apache.ignite.internal.sql.engine.framework.DataProvider.fromCollection;
import static org.apache.ignite.internal.sql.engine.framework.TestBuilders.tableScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.nodeRowCount;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests to check row count estimation for select relation.
 */
public class SelectRowCountEstimationTest extends BaseRowsProcessedEstimationTest {
    private static final int TABLE_REAL_SIZE = 73_049;

    private static final TestCluster CLUSTER = TestBuilders.cluster()
            .nodes("N1")
            .defaultAssignmentsProvider(tableName -> (partNum, includeBackups) -> IntStream.range(0, partNum)
                    .mapToObj(part -> List.of("N1"))
                    .collect(Collectors.toList())
            )
            .defaultDataProvider(tableName -> tableScan(fromCollection(List.of())))
            .build();

    private static final TestNode NODE = CLUSTER.node("N1");

    @BeforeAll
    static void startCluster() {
        CLUSTER.start();

        for (TpcTable table : List.of(TpcdsTables.CATALOG_SALES, TpcdsTables.CATALOG_RETURNS, TpcdsTables.DATE_DIM)) {
            NODE.initSchema(table.ddlScript());
        }

        CLUSTER.setTableSize("CATALOG_SALES", TABLE_REAL_SIZE);
    }

    @AfterAll
    static void stopCluster() throws Exception {
        CLUSTER.stop();
    }

    @Test
    void testPredicatesSelectivity() {
        double selectivityFactor = 0.5;
        assertQuery(NODE, "SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        assertQuery(NODE, "SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10 OR CS_SOLD_DATE_SK < 5")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE)))
                .check();

        String query = appendEqConditions(2);
        selectivityFactor = 0.56;
        assertQuery(NODE, query)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        query = appendEqConditions(3);
        selectivityFactor = 0.76;
        assertQuery(NODE, query)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        query = appendEqConditions(20);
        selectivityFactor = 1.0;
        assertQuery(NODE, query)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.25;
        assertQuery(NODE, "SELECT * FROM CATALOG_SALES WHERE (CS_SOLD_DATE_SK > 10 AND CS_SOLD_DATE_SK < 20) OR "
                + "(CS_SOLD_DATE_SK > 0 and CS_SOLD_DATE_SK < 5)")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.33;
        assertQuery(NODE, "SELECT * FROM CATALOG_SALES WHERE (CS_SOLD_DATE_SK > 10 AND CS_SOLD_DATE_SK < 20) OR "
                + "(CS_SOLD_DATE_SK = 0)")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.5;
        assertQuery(NODE, "SELECT * FROM CATALOG_SALES WHERE SQRT(CS_SOLD_DATE_SK) > 10")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.9;
        assertQuery(NODE, "SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10 OR CS_SOLD_DATE_SK IS NOT NULL")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.33;
        assertQuery(NODE, "SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK = 10 OR LENGTH('12') = 2")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();
    }

    private static String appendEqConditions(int count) {
        IgniteStringBuilder query = new IgniteStringBuilder("SELECT * FROM CATALOG_SALES WHERE");

        for (int i = 0; i < count; ++i) {
            if (i != 0) {
                query.app(" OR ");
            }

            query.app(" CS_SOLD_DATE_SK = " + (2 * i));
        }

        return query.toString();
    }


}
