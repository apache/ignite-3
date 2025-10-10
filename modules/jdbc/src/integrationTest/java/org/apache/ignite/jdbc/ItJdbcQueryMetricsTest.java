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
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.CANCELED_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.FAILED_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.SUCCESSFUL_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.TIMED_OUT_QUERIES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.jdbc2.JdbcStatement2;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for query execution metrics.
 */
public class ItJdbcQueryMetricsTest extends AbstractJdbcSelfTest {

    private MetricSet metricsSet;

    private MetricSet metrics() {
        if (metricsSet == null) {
            metricsSet = unwrapIgniteImpl(node(0)).metricManager().metricSnapshot().metrics().get(SqlQueryMetricSource.NAME);
        }
        return metricsSet;
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26142")
    public void testScriptErrors() throws SQLException {
        try (var stmt = conn.prepareStatement("SELECT 1; SELECT 1/?;")) {
            stmt.setInt(1, 0);

            long success0 = metricValue(SUCCESSFUL_QUERIES);
            long failed0 = metricValue(FAILED_QUERIES);
            long cancelled0 = metricValue(CANCELED_QUERIES);
            long timedout0 = metricValue(TIMED_OUT_QUERIES);

            // The first statement is OK
            boolean rs1 = stmt.execute();
            assertTrue(rs1);

            assertEquals(success0 + 1, metricValue(SUCCESSFUL_QUERIES));
            assertEquals(failed0, metricValue(FAILED_QUERIES));
            assertEquals(cancelled0, metricValue(CANCELED_QUERIES));
            assertEquals(timedout0, metricValue(TIMED_OUT_QUERIES));

            // The second statement fails
            assertThrows(SQLException.class, () -> {
                assertTrue(stmt.getMoreResults());
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    rs.getInt(1);
                }
            });
            assertEquals(success0 + 1, metricValue(SUCCESSFUL_QUERIES));
            assertEquals(failed0 + 1, metricValue(FAILED_QUERIES));
            assertEquals(cancelled0, metricValue(CANCELED_QUERIES));
            assertEquals(timedout0, metricValue(TIMED_OUT_QUERIES));
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26142")
    public void testScriptCancellation() throws SQLException {
        try (var stmt = conn.prepareStatement("SELECT 1; SELECT 1/?;")) {
            stmt.setInt(1, 0);

            long success0 = metricValue(SUCCESSFUL_QUERIES);
            long failed0 = metricValue(FAILED_QUERIES);
            long cancelled0 = metricValue(CANCELED_QUERIES);

            // The first statement is OK
            boolean rs1 = stmt.execute();
            assertTrue(rs1);

            assertEquals(success0 + 1, metricValue(SUCCESSFUL_QUERIES));
            assertEquals(failed0, metricValue(FAILED_QUERIES));
            assertEquals(cancelled0, metricValue(CANCELED_QUERIES));

            // The second statement is cancelled
            assertThrows(SQLException.class, () -> {
                stmt.cancel();
                assertTrue(stmt.getMoreResults());
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    rs.getInt(1);
                }
            });
            assertEquals(success0 + 1, metricValue(SUCCESSFUL_QUERIES));
            assertEquals(failed0 + 1, metricValue(FAILED_QUERIES));
            assertEquals(cancelled0 + 1, metricValue(CANCELED_QUERIES));
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26142")
    public void testScriptTimeout() {
        Callable<Map<String, Long>> runScript = () -> {

            long success0 = metricValue(SUCCESSFUL_QUERIES);
            long failed0 = metricValue(FAILED_QUERIES);
            long cancelled0 = metricValue(CANCELED_QUERIES);
            long timedout0 = metricValue(TIMED_OUT_QUERIES);

            log.info("Initial Metrics: success: {}, failed: {}, cancelled: {}, timed out: ",
                    success0, failed0, cancelled0, timedout0);

            SQLException err;

            try (var stmt = conn.createStatement()) {
                JdbcStatement2 jdbc = stmt.unwrap(JdbcStatement2.class);
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

            // Check the metrics

            long success1 = metricValue(SUCCESSFUL_QUERIES);
            long failed1 = metricValue(FAILED_QUERIES);
            long cancelled1 = metricValue(CANCELED_QUERIES);
            long timedout1 = metricValue(TIMED_OUT_QUERIES);

            log.info("Metrics: success: {}, failed: {}, cancelled: {}, timed out: {}",
                    success1, failed1, cancelled1, timedout1);

            return Map.of(
                    SUCCESSFUL_QUERIES, success1 - success0,
                    FAILED_QUERIES, failed1 - failed0,
                    CANCELED_QUERIES, cancelled1 - cancelled0,
                    TIMED_OUT_QUERIES, timedout1 - timedout0
            );
        };

        Map<String, Long> delta = Map.of(
                SUCCESSFUL_QUERIES, 1L, FAILED_QUERIES, 2L, CANCELED_QUERIES, 0L, TIMED_OUT_QUERIES, 2L
        );

        Awaitility.await().ignoreExceptions().until(runScript, Matchers.equalTo(delta));
    }

    private long metricValue(String name) {
        LongMetric metric = metrics().get(name);
        Objects.requireNonNull(metric, "metric does not exist: " + name);

        return metric.value();
    }
}
