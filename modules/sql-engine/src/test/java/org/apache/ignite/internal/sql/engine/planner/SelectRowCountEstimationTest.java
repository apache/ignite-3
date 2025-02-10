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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.sql.engine.framework.DataProvider.fromCollection;
import static org.apache.ignite.internal.sql.engine.framework.TestBuilders.tableScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.nodeRowCount;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.CancellationToken;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests to check row count estimation for select relation.
 */
@ExtendWith(QueryCheckerExtension.class)
public class SelectRowCountEstimationTest {
    private static final int TABLE_REAL_SIZE = 73_049;

    @InjectQueryCheckerFactory
    private static QueryCheckerFactory queryCheckerFactory;

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
        assertQuery("SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        assertQuery("SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10 OR CS_SOLD_DATE_SK < 5")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE)))
                .check();

        String query = appendEqConditions(2);
        selectivityFactor = 0.56;
        assertQuery(query)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        query = appendEqConditions(3);
        selectivityFactor = 0.76;
        assertQuery(query)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        query = appendEqConditions(20);
        selectivityFactor = 1.0;
        assertQuery(query)
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.25;
        assertQuery("SELECT * FROM CATALOG_SALES WHERE (CS_SOLD_DATE_SK > 10 AND CS_SOLD_DATE_SK < 20) OR "
                + "(CS_SOLD_DATE_SK > 0 and CS_SOLD_DATE_SK < 5)")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.33;
        assertQuery("SELECT * FROM CATALOG_SALES WHERE (CS_SOLD_DATE_SK > 10 AND CS_SOLD_DATE_SK < 20) OR "
                + "(CS_SOLD_DATE_SK = 0)")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.5;
        assertQuery("SELECT * FROM CATALOG_SALES WHERE SQRT(CS_SOLD_DATE_SK) > 10")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();

        selectivityFactor = 0.9;
        assertQuery("SELECT * FROM CATALOG_SALES WHERE CS_SOLD_DATE_SK > 10 OR CS_SOLD_DATE_SK IS NOT NULL")
                .matches(nodeRowCount("TableScan", approximatelyEqual(TABLE_REAL_SIZE * selectivityFactor)))
                .check();
    }

    private static String appendEqConditions(int count) {
        String query = "SELECT * FROM CATALOG_SALES WHERE";

        for (int i = 0; i < count; ++i) {
            if (i != 0) {
                query += " OR ";
            }

            query += " CS_SOLD_DATE_SK = " + (2 * i);
        }

        return query;
    }

    private static QueryChecker assertQuery(String query) {
        //noinspection DataFlowIssue
        return queryCheckerFactory.create(
                NODE.name(),
                new QueryProcessor() {
                    @Override
                    public CompletableFuture<QueryMetadata> prepareSingleAsync(SqlProperties properties,
                            @Nullable InternalTransaction transaction, String qry, Object... params) {
                        throw new AssertionError("Should not be called");
                    }

                    @Override
                    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
                            SqlProperties properties,
                            HybridTimestampTracker observableTime,
                            @Nullable InternalTransaction transaction,
                            @Nullable CancellationToken token,
                            String qry,
                            Object... params
                    ) {
                        return completedFuture(NODE.executeQuery(qry));
                    }

                    @Override
                    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                        return nullCompletedFuture();
                    }

                    @Override
                    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                        return nullCompletedFuture();
                    }
                },
                null,
                null,
                query
        );
    }

    private static Matcher<Double> approximatelyEqual(double expected) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof Double)) {
                    return false;
                }

                double value = (double) o;
                return Math.abs(1 - (value / expected)) < 0.05;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("equals to ").appendValue(expected).appendText("±5%");
            }
        };
    }
}
