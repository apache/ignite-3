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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.CANCELED_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.FAILED_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.SUCCESSFUL_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.TIMED_OUT_QUERIES;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for query execution metrics.
 */
public class ItSqlQueryExecutionMetricsTest extends BaseSqlIntegrationTest {

    private MetricSet metricsSet;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    public void beforeAll() {
        sql("CREATE TABLE t (id INT, val INT, PRIMARY KEY (id))");
        sql("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)");
    }

    @ParameterizedTest
    @MethodSource("singleSuccessful")
    public void testSingle(String sqlString) {
        Map<String, Long> metrics = Map.of(
                SUCCESSFUL_QUERIES, 1L, FAILED_QUERIES, 0L,
                CANCELED_QUERIES, 0L, TIMED_OUT_QUERIES, 0L
        );

        assertMetricIncreased(() -> sql(sqlString), metrics);
    }

    private static Stream<Arguments> singleSuccessful() {
        return Stream.of(
                Arguments.of("SELECT 1"),
                Arguments.of("SELECT * FROM t WHERE id=1"),
                Arguments.of("SELECT * FROM t"),
                Arguments.of("INSERT INTO t VALUES (100, 100)"),
                Arguments.of("DROP TABLE IF EXISTS none")
        );
    }

    @ParameterizedTest
    @MethodSource("singleUnsuccessful")
    public void testSingleWithErrors(String sqlString, Object[] params) {
        Map<String, Long> metrics = Map.of(
                SUCCESSFUL_QUERIES, 0L, FAILED_QUERIES, 1L,
                CANCELED_QUERIES, 0L, TIMED_OUT_QUERIES, 0L
        );

        assertMetricIncreased(() -> assertThrows(SqlException.class, () -> sql(sqlString, params)), metrics);
    }

    private static Stream<Arguments> singleUnsuccessful() {
        return Stream.of(
                // Parsing errors
                Arguments.of("ELECT * FROM t", new Object[0]),
                // Validation errors
                Arguments.of("INSERT INTO none VALUES (4)", new Object[0]),
                Arguments.of("SELECT * FROM none", new Object[0]),
                // Execution errors
                Arguments.of("SELECT 1/?", new Object[]{0}),
                Arguments.of("SELECT * FROM t WHERE id=1/?", new Object[]{0}),
                Arguments.of("SELECT * FROM t WHERE val=1/?", new Object[]{0}),
                Arguments.of("SELECT CASE WHEN x < 1100 THEN x ELSE x/? END FROM system_range(1, 1500)", new Object[]{0}),
                Arguments.of("UPDATE t SET val=1/?", new Object[]{0}),
                Arguments.of("INSERT INTO t VALUES (4, 4/?)", new Object[]{0}),
                Arguments.of("ALTER TABLE none ADD COLUMN v INT", new Object[0])
        );
    }

    @Test
    public void testSingleCancellation() {
        IgniteSql sql = igniteSql();

        Map<String, Long> metrics = Map.of(
                SUCCESSFUL_QUERIES, 0L, FAILED_QUERIES, 1L,
                CANCELED_QUERIES, 1L, TIMED_OUT_QUERIES, 0L
        );

        {
            CancelHandle cancelHandle = CancelHandle.create();

            assertMetricIncreased(() -> assertThrows(CompletionException.class, () -> {
                CompletableFuture<AsyncResultSet<SqlRow>> f = sql.executeAsync(null, cancelHandle.token(),
                        "SELECT x FROM system_range(1, 10000000000)");
                cancelHandle.cancelAsync();
                f.join();
            }), metrics);
        }
    }

    @Test
    public void testSingleTimeout() {
        IgniteSql sql = igniteSql();

        int timeoutSeconds = 100;
        TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;

        Map<String, Long> metrics = Map.of(
                SUCCESSFUL_QUERIES, 0L, FAILED_QUERIES, 1L,
                CANCELED_QUERIES, 0L, TIMED_OUT_QUERIES, 1L
        );

        // Run multiple times to make the test case stable w/o setting large timeout values.
        assertMetricIncreased(() -> assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "", () -> {
            Statement statement = sql.statementBuilder()
                    .query("SELECT * FROM system_range(1, 1000000000000000)")
                    .queryTimeout(timeoutSeconds, timeoutUnit)
                    .build();

            try (ResultSet<SqlRow> rs = sql.execute(null, statement)) {
                timeoutUnit.sleep(timeoutSeconds);
                // Triggers timeout
                while (rs.hasNext()) {
                    assertNotNull(rs.next());
                }
            }
        }), metrics);
    }

    @ParameterizedTest
    @MethodSource("scriptsSuccessful")
    public void testScriptSuccessful(List<String> statements) {
        IgniteSql sql = igniteSql();

        List<String> scriptStatements = new ArrayList<>();
        Collections.shuffle(scriptStatements);

        String script = String.join(";" + System.lineSeparator(), statements);

        log.info("Script:\n{}", script);

        Map<String, Long> metrics = Map.of(
                SUCCESSFUL_QUERIES, (long) statements.size(), FAILED_QUERIES, 0L,
                CANCELED_QUERIES, 0L, TIMED_OUT_QUERIES, 0L
        );

        assertMetricIncreased(() -> sql.executeScript(script), metrics);
    }

    private static Stream<List<String>> scriptsSuccessful() {
        return Stream.of(
                List.of("SELECT 1"),
                List.of("SELECT 1", "SELECT 2", "SELECT 3"),
                List.of("CREATE TABLE tx_001 (id INT, val INT, PRIMARY KEY (id))",
                        "SELECT 2",
                        "CREATE TABLE tx_002 (id INT, val INT, PRIMARY KEY (id))"
                ),
                List.of("CREATE TABLE tx_101 (id INT, val INT, PRIMARY KEY (id))",
                        "INSERT INTO t VALUES (1000, 1000)",
                        "CREATE TABLE tx_102 (id INT, val INT, PRIMARY KEY (id))"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("scriptsUnsuccessful")
    public void testScriptWithErrors(List<String> statements, Object[] params, int success, int error) {
        IgniteSql sql = igniteSql();

        String script = String.join(";" + System.lineSeparator(), statements);

        Map<String, Long> metrics = Map.of(
                SUCCESSFUL_QUERIES, (long) success, FAILED_QUERIES, (long) error,
                CANCELED_QUERIES, 0L, TIMED_OUT_QUERIES, 0L
        );

        assertMetricIncreased(() -> assertThrows(SqlException.class, () -> sql.executeScript(script, params)), metrics);
    }

    private static Stream<Arguments> scriptsUnsuccessful() {
        return Stream.of(
                // Parse errors
                Arguments.of(List.of("ELECT * FROM t"), new Object[0], 0, 1),
                Arguments.of(List.of("SELECT 1", "ELECT * FROM t"), new Object[0], 0, 1),
                Arguments.of(List.of("ELECT * FROM t", "SELECT 1"), new Object[0], 0, 1),
                // Validation errors
                Arguments.of(List.of("INSERT INTO none VALUES (4)", "SELECT 1"), new Object[0], 0, 1),
                Arguments.of(List.of("SELECT 1", "INSERT INTO none VALUES (4)"), new Object[0], 1, 1),
                Arguments.of(List.of("SELECT * FROM none", "SELECT 1"), new Object[0], 0, 1),
                Arguments.of(List.of("SELECT 1", "SELECT * FROM none"), new Object[0], 1, 1),
                // Execution errors
                Arguments.of(List.of("DROP TABLE unknown1", "SELECT 2", "SELECT 3"), new Object[0], 0, 1),
                Arguments.of(List.of("SELECT 1", "DROP TABLE unknown1", "SELECT 3"), new Object[0], 1, 1),
                Arguments.of(List.of("SELECT 1", "SELECT 2", "DROP TABLE unknown1"), new Object[0], 2, 1)
        );
    }

    private void assertMetricIncreased(Runnable task, Map<String, Long> deltas) {
        Callable<Boolean> condition = () -> {
            // Collect current metric values.
            Map<String, Long> expected = new HashMap<>();
            for (Entry<String, Long> e : deltas.entrySet()) {
                String metricName = e.getKey();
                long value = longMetricValue(metricName);
                expected.put(metricName, value + e.getValue());
            }

            // Run inside the condition.
            task.run();

            // Collect actual metric values.
            Map<String, Long> actual = new HashMap<>();
            for (String metricName : expected.keySet()) {
                long actualVal = longMetricValue(metricName);
                actual.put(metricName, actualVal);
            }
            boolean ok = actual.equals(expected);

            log.info("Expected: {}", expected);
            log.info("Delta: {}", deltas);
            log.info("Actual: {}", actual);
            log.info("Check passes: {}", ok);

            return ok;
        };

        // Checks multiple times until values match
        Awaitility.await().ignoreExceptions().until(condition);
    }

    private long longMetricValue(String metricName) {
        LongMetric metric = metrics().get(metricName);
        Objects.requireNonNull(metric, "metric does not exist: " + metricName);
        return metric.value();
    }

    private synchronized MetricSet metrics() {
        if (metricsSet == null) {
            metricsSet = unwrapIgniteImpl(node(0)).metricManager().metricSnapshot().metrics().get(SqlQueryMetricSource.NAME);
        }
        return metricsSet;
    }
}
