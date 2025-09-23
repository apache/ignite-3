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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.ErrorGroups.Sql;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.metrics.SqlClientMetricSource;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Test SQL client metrics. */
public class ItSqlClientMetricsTest extends BaseSqlIntegrationTest {
    private MetricManager metricManager;
    private IgniteSql sql;

    @BeforeAll
    void beforeAll() {
        metricManager = queryProcessor().metricManager();
        sql = igniteSql();

        createAndPopulateTable();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 0);
    }

    @AfterEach
    void afterEach() throws Exception {
        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 0);
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    public void testNormalFlow() throws Exception {
        sql.execute(null, "SELECT * from " + DEFAULT_TABLE_NAME);

        // default pageSize greater than number of rows in a table, thus cursor will be closed immediately
        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 0);

        Statement statement = sql.statementBuilder()
                .query("SELECT * from " + DEFAULT_TABLE_NAME)
                .pageSize(1)
                .build();

        ResultSet<SqlRow> rs1 = sql.execute(null, statement);

        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 1);
        rs1.forEachRemaining(c -> {});
        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 0);

        ResultSet<SqlRow> rs2 = sql.execute(null, statement);
        ResultSet<SqlRow> rs3 = sql.execute(null, statement);

        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 2);

        rs2.close();
        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 1);

        rs3.close();
        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 0);
    }

    private static void assertInternalSqlException(String expectedText, Executable executable) {
        IgniteException err = assertThrows(IgniteException.class, executable);
        assertThat(err.getMessage(), containsString(expectedText));
    }

    @Test
    public void testErroneousFlow() throws Exception {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> sql.execute(null, "SELECT * ODINfrom " + DEFAULT_TABLE_NAME));
        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 0);

        assertInternalSqlException("Column 'A' not found in any table", () -> sql.execute(null, "SELECT a from " + DEFAULT_TABLE_NAME));
        assertMetricValue(SqlClientMetricSource.METRIC_OPEN_CURSORS, 0);
    }

    private void assertMetricValue(String metricName, Object expectedValue) throws InterruptedException {
        MetricSet metricSet = metricManager.metricSnapshot().metrics().get(SqlClientMetricSource.NAME);

        assertTrue(
                waitForCondition(
                        () -> expectedValue.toString().equals(metricSet.get(metricName).getValueAsString()),
                        1000)
        );
    }
}
