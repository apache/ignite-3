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

import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.jdbc.JdbcConnection;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Basic test scenarios with JDBC connection failover.
 */
public class ItJdbcConnectionFailoverTest extends ClusterPerTestIntegrationTest {
    private static final int BASE_CLIENT_PORT = 10800;

    @Override
    protected int initialNodes() {
        return 0;
    }

    /**
     * Ensures that the client establishes connections to multiple nodes.
     */
    @Test
    void testMultipleConnectionEstablishment() throws SQLException {
        int nodesCount = 2;

        cluster.startAndInit(nodesCount, new int[]{0});

        try (Connection connection = getConnection(nodesCount)) {
            await().timeout(Duration.ofSeconds(5))
                    .until(() -> channelsCount(connection), is(nodesCount));
        }
    }

    /**
     * Ensures that the query is forwarded to the alive node.
     *
     * <p>Test sequentially restarts each cluster node keeping CMG majority alive.
     */
    @Test
    void testConnectionFailover() throws SQLException {
        int nodesCount = 3;
        cluster.startAndInit(nodesCount, new int[]{0, 1, 2});

        try (Connection connection = getConnection(nodesCount)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE ZONE zone1 (REPLICAS 3, PARTITIONS 1) STORAGE PROFILES ['default']");
                statement.executeUpdate("CREATE TABLE t(id INT PRIMARY KEY, val INT) ZONE zone1");

                try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                    performUpdates(preparedStatement, 0, 100);

                    cluster.stopNode(0);
                    performUpdates(preparedStatement, 100, 200);

                    cluster.startNode(0);
                    cluster.stopNode(1);
                    performUpdates(preparedStatement, 200, 300);

                    cluster.startNode(1);
                    cluster.stopNode(2);
                    performUpdates(preparedStatement, 300, 400);
                }

                try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM t WHERE id >= 0")) {
                    assertThat(rs.next(), is(true));
                    assertThat(rs.getInt(1), is(400));
                }
            }
        }
    }

    /**
     * Checks transparent connection establishment after losing all connections.
     */
    @Test
    void testTotalConnectionFailover() throws SQLException {
        int nodesCount = 3;
        cluster.startAndInit(nodesCount, new int[]{2});
        String dummyQuery = "SELECT 1";

        try (Connection connection = getConnection(nodesCount - 1)) {
            try (Statement stmt = connection.createStatement()) {
                assertThat(stmt.execute(dummyQuery), is(true));

                // Stop all cluster nodes known to the client.
                cluster.stopNode(0);
                cluster.stopNode(1);

                assertThrowsSqlException(
                        SQLException.class,
                        "Connection refused",
                        () -> stmt.execute(dummyQuery)
                );

                cluster.startNode(1);

                assertThat(stmt.execute(dummyQuery), is(true));
            }
        }
    }

    /**
     * Ensures that the client receives a meaningful exception when the node holding the client transaction goes down.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27091")
    void testTransactionCannotBeUsedAfterNodeRestart() throws SQLException {
        int nodesCount = 3;
        cluster.startAndInit(nodesCount, new int[]{2});

        String dummyQuery = "SELECT 1";

        try (Connection connection = getConnection(nodesCount - 1)) {
            connection.setAutoCommit(false);

            try (Statement stmt = connection.createStatement()) {
                assertThat(stmt.execute(dummyQuery), is(true));

                // Stop all cluster nodes known to the client.
                cluster.stopNode(0);
                cluster.stopNode(1);

                assertThrowsSqlException(
                        SQLException.class,
                        "Connection refused",
                        () -> stmt.execute(dummyQuery)
                );

                cluster.startNode(0);
                cluster.startNode(1);

                assertThrowsSqlException(
                        SQLException.class,
                        "Transaction context has been lost due to connection errors",
                        () -> stmt.execute(dummyQuery)
                );
            }
        }
    }

    private static int channelsCount(Connection connection) throws SQLException {
        JdbcConnection jdbcConnection = connection.unwrap(JdbcConnection.class);

        return jdbcConnection.channelsCount();
    }

    private static Connection getConnection(int nodesCount) throws SQLException {
        String addresses = IntStream.range(0, nodesCount)
                .mapToObj(i -> "127.0.0.1:" + (BASE_CLIENT_PORT + i))
                .collect(Collectors.joining(","));

        //noinspection CallToDriverManagerGetConnection
        return DriverManager.getConnection("jdbc:ignite:thin://" + addresses);
    }

    private static void performUpdates(PreparedStatement preparedStatement, int start, int end) throws SQLException {
        // Performing separate updates (not batch) to take into account partition awareness metadata.
        for (int n = start; n < end; n++) {
            preparedStatement.setInt(1, n);
            preparedStatement.setInt(2, n);

            assertThat(preparedStatement.executeUpdate(), is(1));
        }
    }
}
