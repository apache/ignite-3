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

package org.apache.ignite.internal.jdbc2;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.jdbc.ItJdbcStatementSelfTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Jdbc tests that require a new cluster for each test.
 */
public class ItJdbcClusterPerIntegrationTest extends ClusterPerTestIntegrationTest {

    private static final List<String> STATEMENTS = List.of(
            "SELECT 1; SELECT 2/0; SELECT 3",
            "SELECT * FROM SYSTEM_RANGE(1, 1500); SELECT 2/0; SELECT 3",
            "SELECT 1; SELECT 2/0; SELECT * FROM SYSTEM_RANGE(3, 1500)"
    );

    private static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1:10800";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26789")
    public void noScriptResourcesAfterExecutingFailingScript() throws Exception {
        for (String statement : STATEMENTS) {
            log.info("Run statement: {}", statement);

            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                Statement stmt = conn.createStatement();
                stmt.execute(statement);
                // Connection should close the statement
            }
            // Fails with IGN-CMN-65535 Failed to find resource with id: XYZ

            expectNoResources();
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26789")
    public void noScriptResourcesAfterExecutingFailingScript2() throws Exception {
        for (String statement : STATEMENTS) {
            log.info("Run statement: {}", statement);

            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(statement);
                }
                // Fails with IGN-CMN-65535 Failed to find resource with id: XYZ

                // All resources should be released
                expectNoResources();
            }
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26789")
    public void noResourcesScriptAfterClientTerminates() throws Exception {
        for (String statement : STATEMENTS) {
            log.info("Run statement: {}", statement);

            Connection conn = DriverManager.getConnection(JDBC_URL);
            JdbcConnection2 jdbcConnection = conn.unwrap(JdbcConnection2.class);

            Statement stmt = conn.createStatement();
            // Do not close the statement, closing the client should release its resources.
            stmt.execute(statement);

            // Close the client
            jdbcConnection.closeClient();

            expectNoResources();
        }
    }

    @Test
    public void noStatementResourcesAfterClientTerminates() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        JdbcConnection2 jdbcConnection = conn.unwrap(JdbcConnection2.class);

        Statement stmt = conn.createStatement();
        // Do not close the statement, closing the client should release its resources.
        stmt.executeQuery("SELECT * FROM SYSTEM_RANGE(1, 15000)");

        // Close the client
        jdbcConnection.closeClient();

        expectNoResources();
    }

    private void expectNoResources() throws InterruptedException {
        assertTrue(waitForCondition(() -> ItJdbcStatementSelfTest.openResources(cluster) == 0, 5_000));
        assertTrue(waitForCondition(() -> ItJdbcStatementSelfTest.openCursors(cluster) == 0, 5_000));
    }
}
