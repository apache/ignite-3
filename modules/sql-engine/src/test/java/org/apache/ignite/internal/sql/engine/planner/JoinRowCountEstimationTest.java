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
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests to check row count estimation for join relation.
 */
@SuppressWarnings("ConcatenationWithEmptyString")
public class JoinRowCountEstimationTest extends BaseRowsProcessedEstimationTest {
    private static final int CATALOG_SALES_SIZE = 1_441_548;
    private static final int CATALOG_RETURNS_SIZE = 144_067;
    private static final int DATE_DIM_SIZE = 73_049;

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

        CLUSTER.setTableSize("CATALOG_SALES", CATALOG_SALES_SIZE);
        CLUSTER.setTableSize("CATALOG_RETURNS", CATALOG_RETURNS_SIZE);
        CLUSTER.setTableSize("DATE_DIM", DATE_DIM_SIZE);
    }

    @AfterAll
    static void stopCluster() throws Exception {
        CLUSTER.stop();
    }

    @Test
    void joinByPrimaryKeys() {
        assertQuery(NODE, ""
                + "SELECT *"
                + "  FROM catalog_sales"
                + "      ,catalog_returns"
                + "  WHERE cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE)))
                .check();

        // Defined by IgniteMdSelectivity.guessSelectivity.
        double isNotNullPredicateFactor = 0.9;
        assertQuery(NODE, ""
                + "SELECT *"
                + "  FROM catalog_sales"
                + "      ,catalog_returns"
                + "  WHERE cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cs_promo_sk IS NOT NULL"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE * isNotNullPredicateFactor)))
                .check();
    }

    @Test
    void joinByForeignKey() {
        assertQuery(NODE, ""
                + "SELECT *"
                + "  FROM catalog_returns"
                + "      ,date_dim"
                + "  WHERE cr_returned_date_sk = d_date_sk"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE)))
                .check();

        // Defined by IgniteMdSelectivity.guessSelectivity.
        double greaterPredicateFactor = 0.5;
        assertQuery(NODE, ""
                + "SELECT *"
                + "  FROM date_dim"
                + "      ,catalog_returns"
                + "  WHERE cr_returned_date_sk = d_date_sk"
                + "    AND d_moy > 6"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE * greaterPredicateFactor)))
                .check();
    }
}
