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
import static org.apache.ignite.internal.sql.engine.metadata.IgniteMdSelectivity.COMPARISON_SELECTIVITY;
import static org.apache.ignite.internal.sql.engine.metadata.IgniteMdSelectivity.IS_NOT_NULL_SELECTIVITY;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.nodeRowCount;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests to check row count estimation for join relation.
 */
@SuppressWarnings("ConcatenationWithEmptyString")
public class JoinRowCountEstimationTest extends BaseRowsProcessedEstimationTest {
    private static final int CATALOG_SALES_SIZE = 1_441_548;
    private static final int CATALOG_RETURNS_SIZE = 144_067;
    private static final int DATE_DIM_SIZE = 73_049;

    private static final String SELECT = "SELECT /*+ DISABLE_RULE('JoinCommuteRule') */ * ";

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

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16493 Decorrelation after subquery rewrite")
    @Test
    void joinByPrimaryKeysSemi() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_sales"
                + " WHERE EXISTS ("
                + "     SELECT 1 FROM catalog_returns "
                + "         WHERE cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number"
                + ")"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE)))
                .check();
    }

    @Test
    void joinByPrimaryKeys() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_sales"
                + "      ,catalog_returns"
                + "  WHERE cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_sales"
                + "      ,catalog_returns"
                + "  WHERE cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cs_promo_sk IS NOT NULL"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE * IS_NOT_NULL_SELECTIVITY)))
                .check();
    }

    @Test
    void joinByPrimaryKeysLeft() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_sales LEFT JOIN catalog_returns ON"
                + "  cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_SALES_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_sales LEFT JOIN catalog_returns"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cs_promo_sk IS NOT NULL"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_SALES_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_sales LEFT JOIN catalog_returns"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cr_order_number > 1"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_SALES_SIZE)))
                .check();
    }

    @Test
    void joinByPrimaryKeysRight() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns RIGHT JOIN catalog_sales ON"
                + "  cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_SALES_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns RIGHT JOIN catalog_sales"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cs_promo_sk IS NOT NULL"
        )
                .matches(nodeRowCount("NestedLoopJoin", CoreMatchers.is(CATALOG_SALES_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns RIGHT JOIN catalog_sales"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cr_order_number > 1"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_SALES_SIZE)))
                .check();
    }

    @Test
    void joinByPrimaryKeysFull() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns FULL OUTER JOIN catalog_sales ON"
                + "  cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual((int) ((CATALOG_SALES_SIZE + CATALOG_RETURNS_SIZE)
                        * (1.0 - (0.15 * 0.15))))))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns FULL OUTER JOIN catalog_sales"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cs_promo_sk IS NOT NULL"
        )
                .matches(nodeRowCount("NestedLoopJoin", approximatelyEqual((int) ((CATALOG_SALES_SIZE + CATALOG_RETURNS_SIZE)
                        * (1.0 - (0.15 * 0.15 * 0.9))))))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns FULL OUTER JOIN catalog_sales"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cr_order_number > 1"
        )
                .matches(nodeRowCount("NestedLoopJoin", approximatelyEqual((int) ((CATALOG_SALES_SIZE + CATALOG_RETURNS_SIZE)
                        * (1.0 - (0.15 * 0.15 * 0.5))))))
                .check();
    }

    @Test
    void joinByForeignKey() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns"
                + "      ,date_dim"
                + "  WHERE cr_returned_date_sk = d_date_sk"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE)))
                .check();

        // Defined by IgniteMdSelectivity.guessSelectivity.
        assertQuery(NODE, ""
                + SELECT
                + "  FROM date_dim"
                + "      ,catalog_returns"
                + "  WHERE cr_returned_date_sk = d_date_sk"
                + "    AND d_moy > 6"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE * COMPARISON_SELECTIVITY)))
                .check();
    }

    @Test
    void joinByForeignKeyLeft() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns LEFT JOIN date_dim ON"
                + "  cr_returned_date_sk = d_date_sk"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM date_dim LEFT JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns LEFT JOIN date_dim ON"
                + "  cr_returned_date_sk = d_date_sk"
                + "     AND cr_return_quantity > 6"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)))
                .check();
    }

    @Test
    void joinByForeignKeyRight() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM catalog_returns RIGHT JOIN date_dim ON"
                + "  cr_returned_date_sk = d_date_sk"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM date_dim RIGHT JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk"
        )
                .matches(nodeRowCount("HashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM date_dim RIGHT JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk"
                + "     AND cr_return_quantity > 6"
        )
                .matches(nodeRowCount("NestedLoopJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)))
                .check();
    }

    @Test
    void joinByForeignKeyFull() {
        assertQuery(NODE, ""
                + SELECT
                + "  FROM date_dim FULL OUTER JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk"
        )
                .matches(nodeRowCount("HashJoin", approximatelyEqual((int) ((DATE_DIM_SIZE + CATALOG_RETURNS_SIZE)
                        * (1.0 - (0.15))))))
                .check();

        assertQuery(NODE, ""
                + SELECT
                + "  FROM date_dim FULL OUTER JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk"
                + "     AND cr_return_quantity > 6"
        )
                .matches(nodeRowCount("NestedLoopJoin", approximatelyEqual((int) ((DATE_DIM_SIZE + CATALOG_RETURNS_SIZE)
                        * (1.0 - (0.15 * 0.5))))))
                .check();
    }
}
