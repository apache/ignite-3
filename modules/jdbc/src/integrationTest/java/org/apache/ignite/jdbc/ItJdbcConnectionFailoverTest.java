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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.jdbc.JdbcConnection;
import org.apache.ignite.internal.lang.RunnableX;
import org.awaitility.Awaitility;
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
     * Ensures that the query is forwarded to the alive node.
     *
     * <p>Test sequentially restarts each cluster node keeping CMG majority alive.
     */
    @Test
    void testBasicQueryForwardedToAliveNode() throws Throwable {
        int nodesCount = 3;
        cluster.startAndInit(nodesCount, new int[]{0, 1, 2});

        try (Connection connection = getConnection(nodesCount)) {
            try (Statement statement = connection.createStatement()) {
                Awaitility.await().until(() -> channelsCount(connection), is(nodesCount));

                RunnableX query = () -> {
                    for (int i = 0; i < 100; i++) {
                        assertThat(statement.execute("SELECT " + i), is(true));
                    }
                };

                query.run();

                cluster.stopNode(0);

                query.run();

                cluster.startNode(0);
                cluster.stopNode(1);
                query.run();

                cluster.startNode(1);
                cluster.stopNode(2);
                query.run();
            }
        }
    }

    /**
     * Ensures that the partition aware query is forwarded to the alive node.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27180")
    void testPartitionAwareQueryForwardedToRandomNode() throws SQLException {
        int nodesCount = 3;
        cluster.startAndInit(nodesCount, new int[]{0, 1, 2});

        try (Connection connection = getConnection(nodesCount)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE ZONE zone1 (REPLICAS 3) STORAGE PROFILES ['default']");
                statement.executeUpdate("CREATE TABLE t(id INT PRIMARY KEY, val INT) ZONE zone1");

                Awaitility.await().until(() -> channelsCount(connection), is(nodesCount));

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
     * Ensures that the connection to a previously stopped node will be restored after the specified time interval.
     *
     * <p>Note: this test relies on the internal implementation to ensure that the
     *          JDBC connection property is correctly applied to the underlying client.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-27188")
    void testConnectionRestoredAfterBackgroundReconnectInterval() throws Exception {
        int nodesCount = 3;
        cluster.startAndInit(nodesCount, new int[]{2});
        int reconnectInterval = 300;
        int timeout = reconnectInterval * 2;

        try (Connection connection = getConnection(nodesCount, "reconnectInterval=" + reconnectInterval)) {
            Awaitility.await().until(() -> channelsCount(connection), is(nodesCount));

            cluster.stopNode(0);

            assertThat(channelsCount(connection), is(nodesCount - 1));

            cluster.startNode(0);

            Thread.sleep(timeout);

            assertThat(channelsCount(connection), is(nodesCount));
        }

        // No background reconnection is expected.
        try (Connection connection = getConnection(nodesCount, "reconnectInterval=0")) {
            Awaitility.await().until(() -> channelsCount(connection), is(nodesCount));

            cluster.stopNode(0);

            assertThat(channelsCount(connection), is(nodesCount - 1));

            cluster.startNode(0);

            Thread.sleep(timeout);

            assertThat(channelsCount(connection), is(nodesCount - 1));
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

                //noinspection ThrowableNotThrown
                assertThrowsSqlException("Connection refused", () -> stmt.execute(dummyQuery));

                cluster.startNode(1);

                assertThat(stmt.execute(dummyQuery), is(true));
            }
        }
    }

    @Test
    void testConnectionRetryLimit() throws SQLException {
        String[] addresses = {
                "127.0.0.1:" + (BASE_CLIENT_PORT + 2),
                "127.0.0.1:" + (BASE_CLIENT_PORT + 1),
                "127.0.0.1:" + BASE_CLIENT_PORT
        };

        int nodesCount = 2;

        cluster.startAndInit(nodesCount, new int[]{1});

        //noinspection ThrowableNotThrown
        assertThrowsSqlException(
                "Failed to connect to server",
                () -> getConnection(String.join(",", addresses), "reconnectRetriesLimit=0")
        );

        getConnection(String.join(",", addresses), "reconnectRetriesLimit=1")
                .close();
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

                //noinspection ThrowableNotThrown
                assertThrowsSqlException("Connection refused", () -> stmt.execute(dummyQuery));

                cluster.startNode(0);
                cluster.startNode(1);

                //noinspection ThrowableNotThrown
                assertThrowsSqlException("Transaction context has been lost due to connection errors",
                        () -> stmt.execute(dummyQuery));
            }
        }
    }

    private static Connection getConnection(int nodesCount, String ... params) throws SQLException {
        String addresses = IntStream.range(0, nodesCount)
                .mapToObj(i -> "127.0.0.1:" + (BASE_CLIENT_PORT + i))
                .collect(Collectors.joining(","));

        return getConnection(addresses, params);
    }

    private static Connection getConnection(String addresses, String ... params) throws SQLException {
        String args = String.join("&", params);

        //noinspection CallToDriverManagerGetConnection
        return DriverManager.getConnection("jdbc:ignite:thin://" + addresses + "?" + args);
    }

    private static void performUpdates(PreparedStatement preparedStatement, int start, int end) throws SQLException {
        // Performing separate updates (not batch) to take into account partition awareness metadata.
        for (int n = start; n < end; n++) {
            preparedStatement.setInt(1, n);
            preparedStatement.setInt(2, n);

            assertThat(preparedStatement.executeUpdate(), is(1));
        }
    }

    private static int channelsCount(Connection connection) throws SQLException {
        JdbcConnection jdbcConnection = connection.unwrap(JdbcConnection.class);

        return jdbcConnection.channelsCount();
    }
}
