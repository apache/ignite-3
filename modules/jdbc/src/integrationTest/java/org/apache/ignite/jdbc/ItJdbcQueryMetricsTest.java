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

package org.apache.ignite.jdbc;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.jdbc.JdbcStatement;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.sql.metrics.QueryMetrics;
import org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource;
import org.junit.jupiter.api.Test;

/**
 * Tests for query execution metrics.
 */
public class ItJdbcQueryMetricsTest extends AbstractJdbcSelfTest {

    private List<MetricSet> metricsSet;

    private List<MetricSet> metrics() {
        if (metricsSet == null) {
            metricsSet = CLUSTER.runningNodes()
                    .map(node -> unwrapIgniteImpl(node).metricManager().metricSnapshot().metrics().get(SqlQueryMetricSource.NAME))
                    .collect(Collectors.toUnmodifiableList());
        }

        return metricsSet;
    }

    @Test
    public void testScriptErrors() throws SQLException {
        try (var stmt = conn.prepareStatement("SELECT 1; SELECT 1/?;")) {
            stmt.setInt(1, 0);

            QueryMetrics initialMetrics = currentMetrics();

            // The first statement is OK
            boolean rs1 = stmt.execute();
            assertTrue(rs1);

            awaitMetrics(initialMetrics, 1, 0, 0, 0);

            // The second statement fails
            assertThrows(SQLException.class, () -> {
                assertTrue(stmt.getMoreResults());
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    rs.getInt(1);
                }
            });

            awaitMetrics(initialMetrics, 1, 1, 0, 0);
        }
    }

    @Test
    public void testScriptCancellation() throws SQLException {
        try (var stmt = conn.prepareStatement("SELECT 1; SELECT 1/?;")) {
            stmt.setInt(1, 0);

            QueryMetrics initialMetrics = currentMetrics();

            // The first statement is OK
            boolean rs1 = stmt.execute();
            assertTrue(rs1);

            awaitMetrics(initialMetrics, 1, 0, 0, 0);

            // The second statement is cancelled
            assertThrows(SQLException.class, () -> {
                stmt.cancel();
                assertTrue(stmt.getMoreResults());
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    rs.getInt(1);
                }
            });

            awaitMetrics(initialMetrics, 1, 1, 1, 0);
        }
    }

    @Test
    public void testScriptTimeout() {
        QueryMetrics initialMetrics = currentMetrics();

        SQLException err;

        try (var stmt = conn.createStatement()) {
            JdbcStatement jdbc = stmt.unwrap(JdbcStatement.class);
            jdbc.setQueryTimeout(1);

            // Start a script
            err = assertThrows(SQLException.class, () -> {
                boolean rs1 = stmt.execute("SELECT 1; SELECT * FROM system_range(1, 10000); SELECT * FROM system_range(1, 20000);");
                assertTrue(rs1);

                // Let the first statement to complete successfully
                try (var rs = stmt.getResultSet()) {
                    rs.next();
                }

                // Get the second statement
                assertTrue(stmt.getMoreResults());
                try (var rs = stmt.getResultSet()) {
                    // Introduce a delay to trigger a timeout.
                    TimeUnit.SECONDS.sleep(2);

                    // Triggers the timeout
                    while (rs.next()) {
                        assertTrue(rs.getInt(1) >= 0);
                    }
                }
            });
        } catch (SQLException e) {
            err = e;
            log.info("Script error:", e);
        }

        assertThat(err.getMessage(), containsString("Query timeout"));

        log.info("Script timed out");

        awaitMetrics(initialMetrics, 1, 2, 0, 2);
    }

    private long metricValue(String name) {
        return metrics().stream().mapToLong(metrics -> {
            LongMetric metric = metrics.get(name);

            assertNotNull(metric);

            return metric.value();
        }).sum();
    }

    private QueryMetrics currentMetrics() {
        return new QueryMetrics(this::metricValue);
    }

    private void awaitMetrics(
            QueryMetrics initialMetrics,
            long succeededDelta,
            long failedDelta,
            long canceledDelta,
            long timedOutDelta
    ) {
        initialMetrics.awaitDeltas(this::metricValue, succeededDelta, failedDelta, canceledDelta, timedOutDelta);
    }
}
