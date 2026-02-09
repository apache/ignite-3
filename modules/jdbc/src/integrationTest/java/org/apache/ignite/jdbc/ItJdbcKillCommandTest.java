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
import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

/**
 * Tests KILL command execution.
 */
public class ItJdbcKillCommandTest extends AbstractJdbcSelfTest {
    private static final String QUERY = "SELECT x FROM system_range(0, 100000) order by x";

    @Test
    public void killUsingUpdate() throws SQLException {
        checkKillQuery(queryId -> {
            try (Statement stmt = conn.createStatement()) {
                return stmt.executeUpdate("KILL QUERY '" + queryId + "'");
            }
        });
    }

    @Test
    public void killUsingPreparedStatement() throws SQLException {
        checkKillQuery(queryId -> {
            try (PreparedStatement stmt = conn.prepareStatement("KILL QUERY '" + queryId + "'")) {
                return stmt.executeUpdate();
            }
        });
    }

    @Test
    public void killUsingExecute() throws SQLException {
        checkKillQuery(queryId -> {
            try (Statement stmt = conn.createStatement()) {
                boolean hasResultSet = stmt.execute("KILL QUERY '" + queryId + "'");

                assertThat(hasResultSet, is(false));

                return stmt.getUpdateCount();
            }
        });
    }

    @Test
    public void killUsingExecuteQueryIsForbidden() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            //noinspection ThrowableNotThrown
            assertThrowsSqlException("Statement of type \"Kill\" is not allowed in current context",
                    () -> stmt.executeQuery("KILL QUERY '" + UUID.randomUUID() + "'")
            );
        }
    }

    @Test
    public void dynamicParametersNotSupported() throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("KILL QUERY ?")) {
            pstmt.setString(1, UUID.randomUUID().toString());

            //noinspection ThrowableNotThrown
            assertThrowsSqlException(
                    "Failed to parse query: Encountered \"?\" at line 1, column 12",
                    pstmt::executeUpdate
            );
        }
    }

    private static void checkKillQuery(Checker checker) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(QUERY)) {
                assertThat(rs.next(), is(true));
                assertThat(rs.getInt(1), is(0));

                List<QueryInfo> queries = runningQueries();

                assertThat(queries, hasSize(1));

                UUID existingQuery = queries.get(0).id();

                // No-op.
                assertThat(checker.check(UUID.randomUUID()), is(0));
                assertThat(runningQueries(), hasSize(1));

                // Actual kill.
                assertThat(checker.check(existingQuery), is(0));
                waitUntilRunningQueriesCount(is(0));

                //noinspection ThrowableNotThrown
                assertThrowsSqlException(
                        QueryCancelledException.CANCEL_MSG, () -> {
                            //noinspection StatementWithEmptyBody
                            while (rs.next()) {
                            }
                        }
                );
            }
        }
    }

    private static void waitUntilRunningQueriesCount(Matcher<Integer> matcher) {
        SqlTestUtils.waitUntilRunningQueriesCount(CLUSTER, matcher);
    }

    private static List<QueryInfo> runningQueries() {
        return CLUSTER.runningNodes().flatMap(node -> {
            IgniteImpl ignite = unwrapIgniteImpl(node);
            SqlQueryProcessor queryProcessor = (SqlQueryProcessor) ignite.queryEngine();
            return queryProcessor.runningQueries().stream();
        }).collect(Collectors.toList());
    }

    @FunctionalInterface
    interface Checker {
        int check(UUID queryId) throws SQLException;
    }
}
