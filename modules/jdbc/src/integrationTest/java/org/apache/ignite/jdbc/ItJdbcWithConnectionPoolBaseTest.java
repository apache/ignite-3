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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.jdbc.JdbcDatabaseMetadata;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.CompletableFutures;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Basic set of tests to ensure that JDBC driver works fine with connection pool.
 */
public abstract class ItJdbcWithConnectionPoolBaseTest extends ClusterPerClassIntegrationTest {
    /** Connection pool. */
    private DataSource dataSource;

    @Override
    protected int initialNodes() {
        return 3;
    }

    protected abstract DataSource getDataSource();

    @BeforeAll
    void initDataSource() throws SQLException {
        dataSource = getDataSource();

        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
            }
        }
    }

    @AfterAll
    void shutdown() throws Exception {
        if (dataSource instanceof Closeable) {
            ((Closeable) dataSource).close();
        } else if (dataSource instanceof AutoCloseable) {
            ((AutoCloseable) dataSource).close();
        } else {
            throw new IllegalStateException("Cannot close the data source");
        }

        Awaitility.await().until(() -> ItJdbcStatementSelfTest.openResources(CLUSTER), is(0));
    }

    @AfterEach
    void cleanup() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM test");
            }
        }

        Awaitility.await().until(() -> ItJdbcStatementSelfTest.openCursors(CLUSTER), is(0));
    }

    static String connectionUrl() {
        String addresses = CLUSTER.runningNodes()
                .map(ignite -> unwrapIgniteImpl(ignite).clientAddress().port())
                .map(port -> "127.0.0.1" + ":" + port)
                .collect(Collectors.joining(","));

        String urlTemplate = "jdbc:ignite:thin://{}";

        return IgniteStringFormatter.format(urlTemplate, addresses);
    }

    @Test
    void testVisibilityOfChangesBetweenConnections() throws Exception {
        try (Connection conn0 = dataSource.getConnection();
                Connection conn1 = dataSource.getConnection();
                Connection conn2 = dataSource.getConnection()) {

            try (Statement createTableStmt = conn0.createStatement()) {
                createTableStmt.executeUpdate(
                        "CREATE TABLE specific_tab(id INT PRIMARY KEY, name VARCHAR(255))");
            }

            try (PreparedStatement insertStmt = conn1.prepareStatement(
                    "INSERT INTO specific_tab VALUES (1, 'test_data')")) {
                insertStmt.executeUpdate();
            }

            try (PreparedStatement selectStmt = conn2.prepareStatement(
                    "SELECT COUNT(*) FROM specific_tab")) {
                ResultSet rs = selectStmt.executeQuery();

                assertThat(rs.next(), is(true));
                assertThat(rs.getLong(1), is(1L));
            }
        }
    }

    @Test
    public void multipleConnections() {
        int connectionsCount = 100;

        CyclicBarrier barrier = new CyclicBarrier(connectionsCount);

        Callable<Integer> worker = () -> {
            barrier.await(5, TimeUnit.SECONDS);

            try (Connection conn = dataSource.getConnection()) {
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT x FROM TABLE(SYSTEM_RANGE(1, 1000))")) {

                    int sum = 0;

                    while (rs.next()) {
                        sum += rs.getInt(1);
                    }

                    return sum;
                }
            }
        };

        List<CompletableFuture<Integer>> futs = new ArrayList<>(connectionsCount);

        for (int i = 0; i < connectionsCount; i++) {
            // Depending on the pool size, workers will be executed sequentially or in parallel.
            futs.add(IgniteTestUtils.runAsync(worker));
        }

        await(CompletableFutures.allOf(futs));

        for (int i = 0; i < connectionsCount; i++) {
            assertThat(futs.get(i).join(), is(((1 + 1000) * 1000) / 2));
        }
    }

    @Test
    public void transactionCommit() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO test VALUES (1, 1), (123, 321)");
            }

            conn.commit();

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id, val FROM test ORDER BY id")) {
                assertThat(rs.next(), is(true));
                assertThat(rs.getInt(1), is(1));
                assertThat(rs.getInt(2), is(1));

                assertThat(rs.next(), is(true));
                assertThat(rs.getInt(1), is(123));
                assertThat(rs.getInt(2), is(321));
            }
        }
    }

    @Test
    public void transactionRollback() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO test VALUES (1, 1)");
            }

            conn.rollback();

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id, val FROM test")) {
                assertThat(rs.next(), is(false));
            }
        }
    }

    @Test
    public void getConnectionAfterException() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT x/(x-10) FROM TABLE(SYSTEM_RANGE(1,20)"));
        }

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertThat(rs.next(), is(true));
            assertThat(rs.getInt(1), is(1));
        }
    }

    @Test
    public void testConnectionMetadata() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            assertThat(meta.getURL(), containsString(connectionUrl()));
            assertThat(meta.getUserName(), nullValue());
            assertThat(meta.getDriverName(), is(JdbcDatabaseMetadata.DRIVER_NAME));
            assertThat(meta.getDatabaseProductName(), is(JdbcDatabaseMetadata.PRODUCT_NAME));
        }
    }
}
