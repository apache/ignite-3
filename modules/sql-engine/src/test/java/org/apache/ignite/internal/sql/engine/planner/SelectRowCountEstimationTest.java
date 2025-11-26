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
import static org.apache.ignite.internal.sql.engine.metadata.IgniteMdSelectivity.EQ_SELECTIVITY;
import static org.apache.ignite.internal.sql.engine.metadata.IgniteMdSelectivity.IS_NOT_NULL_SELECTIVITY;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.nodeRowCount;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

        for (TpcTable table : List.of(TpcdsTables.CATALOG_SALES)) {
            NODE.initSchema(table.ddlScript());
        }

        CLUSTER.setTableSize("CATALOG_SALES", TABLE_REAL_SIZE);
    }

    @AfterAll
    static void stopCluster() throws Exception {
        CLUSTER.stop();
    }

    @ParameterizedTest
    @MethodSource("pkSelectivity")
    void testConditionWithPk(String sql, double selectivityFactor) {
        assertQuery(NODE, sql)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .disableRules("TableScanToKeyValueGetRule", "LogicalIndexScanConverterRule")
                .check();
    }

    @ParameterizedTest
    @MethodSource("pkSelectivity")
    void testConditionWithPkNoUnion(String sql, double selectivityFactor) {
        assertQuery(NODE, sql)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .disableRules("TableScanToKeyValueGetRule", "LogicalIndexScanConverterRule", "ScanLogicalOrToUnionRule")
                .check();
    }

    private static Stream<Arguments> pkSelectivity() {
        return Stream.of(
                // partial pk
                Arguments.of("SELECT CS_SOLD_DATE_SK, CS_ITEM_SK FROM CATALOG_SALES WHERE CS_ITEM_SK = 1", EQ_SELECTIVITY),
                // partial pk
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_ITEM_SK = 1", EQ_SELECTIVITY),
                // full pk, eq operations
                Arguments.of("SELECT CS_SOLD_DATE_SK FROM CATALOG_SALES WHERE CS_ITEM_SK = 1 AND CS_ORDER_NUMBER = 1",
                        1.0 / TABLE_REAL_SIZE),
                Arguments.of("SELECT CS_SOLD_DATE_SK FROM CATALOG_SALES WHERE CS_ITEM_SK = 1+ROUND(1/CS_SHIP_DATE_SK) "
                                + "AND CS_ORDER_NUMBER = 1+1", 1.0 / TABLE_REAL_SIZE),
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_ITEM_SK = 1 AND CS_ORDER_NUMBER = 1", 1.0 / TABLE_REAL_SIZE),
                // full pk, comp operations
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_ITEM_SK = 1 AND CS_ORDER_NUMBER > 1",
                        EQ_SELECTIVITY * COMPARISON_SELECTIVITY),
                // full pk with OR
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE (CS_ITEM_SK = 1 AND CS_ORDER_NUMBER = 1) OR CS_ORDER_NUMBER = 2",
                        EQ_SELECTIVITY),
                // full pk or full pk
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE (CS_ITEM_SK = 1 AND CS_ORDER_NUMBER = 1) "
                        + "OR (CS_ITEM_SK = 3 AND CS_ORDER_NUMBER = 3)", 1.0 / TABLE_REAL_SIZE),
                // full pk with computation, not covered by now
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_ORDER_NUMBER + CS_ITEM_SK = 1", EQ_SELECTIVITY)
        );
    }

    @ParameterizedTest
    @MethodSource("predicateSelectivity")
    void testPredicatesSelectivity(String sql, double selectivityFactor) {
        assertQuery(NODE, sql)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();
    }

    private static Stream<Arguments> predicateSelectivity() {
        return Stream.of(
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10", COMPARISON_SELECTIVITY),
                Arguments.of(appendEqConditions(2), 0.56),
                Arguments.of(appendEqConditions(3), 0.76),
                Arguments.of(appendEqConditions(20), 1.0),
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10 OR CS_SOLD_DATE_SK < 5",
                        COMPARISON_SELECTIVITY + COMPARISON_SELECTIVITY),
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE (CS_SOLD_DATE_SK > 10 AND CS_SOLD_DATE_SK < 20) OR "
                        + "(CS_SOLD_DATE_SK > 0 and CS_SOLD_DATE_SK < 5)", COMPARISON_SELECTIVITY * COMPARISON_SELECTIVITY),
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE (CS_SOLD_DATE_SK > 10 AND CS_SOLD_DATE_SK < 20) OR "
                        + "(CS_SOLD_DATE_SK = 0)", EQ_SELECTIVITY),
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE SQRT(CS_SOLD_DATE_SK) > 10", COMPARISON_SELECTIVITY),
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10 OR CS_SOLD_DATE_SK IS NOT NULL",
                        IS_NOT_NULL_SELECTIVITY),
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK = 10 OR LENGTH(CURRENT_DATE::VARCHAR) = 2", EQ_SELECTIVITY),
                // IS NOT NULL over NOT NULL column
                Arguments.of("SELECT * FROM CATALOG_SALES WHERE CS_ITEM_SK IS NOT NULL", 1.0)
        );
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
