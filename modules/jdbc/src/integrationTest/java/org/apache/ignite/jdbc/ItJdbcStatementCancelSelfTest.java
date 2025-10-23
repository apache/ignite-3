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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Statement cancel test.
 */
@SuppressWarnings({"ThrowableNotThrown", "JDBCResourceOpenedButNotSafelyClosed"})
public class ItJdbcStatementCancelSelfTest extends AbstractJdbcSelfTest {
    @AfterEach
    void reset() throws SQLException {
        if (!stmt.isClosed()) {
            stmt.setFetchSize(1024);
        }

        dropAllTables();
    }

    @Test
    void cancelIsNoopWhenThereIsNoRunningQuery() throws SQLException {
        stmt.cancel();
    }

    @Test
    void cancellationOfLongRunningQuery() throws Exception {
        CompletableFuture<?> result = runAsync(() ->
                stmt.executeQuery("SELECT count(*) FROM system_range(0, 10000000000)")
        );

        assertTrue(
                waitForCondition(() -> sql("SELECT * FROM system.sql_queries").size() == 2, 5_000),
                "Query didn't appear in running queries view or disappeared too soon"
        );

        stmt.cancel();

        // second cancellation should not throw any error
        stmt.cancel();

        assertThrowsSqlException(
                QueryCancelledException.CANCEL_MSG,
                () -> await(result)
        );
    }

    @Test
    void cancellationOfMultiStatementQuery() throws Exception {
        stmt.executeUpdate("CREATE TABLE dummy (id INT PRIMARY KEY, val INT)");
        stmt.setFetchSize(1);

        stmt.execute("START TRANSACTION;"
                + "SELECT x FROM system_range(0, 100000) ORDER BY x;" // result should be big enough, so it doesn't fit into a single page
                + "COMMIT;" // script processing is expected to hung on COMMIT until all cursors have been closed
                + "INSERT INTO dummy VALUES (1, 1);");

        stmt.getMoreResults(); // move to SELECT

        ResultSet rs = stmt.getResultSet();

        assertNotNull(rs);

        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        stmt.cancel();

        assertThrowsSqlException(
                QueryCancelledException.CANCEL_MSG,
                stmt::getMoreResults
        );
    }

    @Test
    void cancelOfClosedStatementThrows() throws Exception {
        stmt.close();

        assertThrowsSqlException("Statement is closed.", stmt::cancel);
    }

    @Test
    void fetchingNextPageAfterCancelingShouldThrow() throws Exception {
        {
            ResultSet rs = stmt.executeQuery("SELECT * FROM system_range(0, 7500)");

            assertTrue(rs.next());

            stmt.cancel();

            assertThrowsSqlException(
                    QueryCancelledException.CANCEL_MSG,
                    () -> {
                        //noinspection StatementWithEmptyBody
                        while (rs.next()) {
                        }
                    }
            );
        }

        {
            // but new execute should work
            ResultSet rs = stmt.executeQuery("SELECT * FROM system_range(0, 7500)");

            //noinspection StatementWithEmptyBody
            while (rs.next()) {
            }
        }
    }

    @Test
    public void cancellationOfOneStatementShouldNotAffectAnother() throws Exception {

        try (Statement anotherStmt = conn.createStatement()) {
            ResultSet rs1 = stmt.executeQuery("SELECT * FROM system_range(0, 7500)");
            ResultSet rs2 = anotherStmt.executeQuery("SELECT * FROM system_range(0, 7500)");

            stmt.cancel();

            assertThrowsSqlException(
                    QueryCancelledException.CANCEL_MSG, () -> {
                        //noinspection StatementWithEmptyBody
                        while (rs1.next()) {
                        }
                    }
            );

            //noinspection StatementWithEmptyBody
            while (rs2.next()) {
            }
        }
    }

    @Test
    void cancellationOfPreparedStatement() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM system_range(0, ?)")) {
            CompletableFuture<?> result = runAsync(() -> {
                ps.setLong(1, 10000000000L);

                ps.executeQuery();
            });

            assertTrue(
                    waitForCondition(() -> sql("SELECT * FROM system.sql_queries").size() == 2, 5_000),
                    "Query didn't appear in running queries view or disappeared too soon"
            );

            ps.cancel();

            assertThrowsSqlException(
                    QueryCancelledException.CANCEL_MSG,
                    () -> await(result)
            );
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26143")
    void cancellationOfBatch() throws Exception {
        stmt.executeUpdate("CREATE TABLE dummy (id INT PRIMARY KEY, val INT)");
        stmt.addBatch("INSERT INTO dummy SELECT x, x FROM system_range(1, 1)");
        stmt.addBatch("INSERT INTO dummy SELECT x, x FROM system_range(2, 2)");
        stmt.addBatch("INSERT INTO dummy SELECT x, x FROM system_range(3, 1000000)");

        CompletableFuture<?> result = runAsync(stmt::executeBatch);

        assertTrue(
                waitForCondition(() -> sql("SELECT * FROM system.sql_queries").size() >= 2, 5_000),
                "Query didn't appear in running queries view or disappeared too soon"
        );

        stmt.cancel();

        assertThrowsSqlException(
                QueryCancelledException.CANCEL_MSG,
                () -> await(result)
        );
    }

    @Test
    void cancellationOfPreparedBatch() throws Exception {
        stmt.executeUpdate("CREATE TABLE dummy (id INT PRIMARY KEY, val INT)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO dummy SELECT x, x FROM system_range(?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 1);
            ps.addBatch();

            ps.setInt(1, 2);
            ps.setInt(2, 2);
            ps.addBatch();

            ps.setInt(1, 3);
            ps.setInt(2, 1000000);
            ps.addBatch();

            CompletableFuture<?> result = runAsync(ps::executeBatch);

            assertTrue(
                    waitForCondition(() -> sql("SELECT * FROM system.sql_queries").size() >= 2, 5_000),
                    "Query didn't appear in running queries view or disappeared too soon"
            );

            ps.cancel();

            assertThrowsSqlException(
                    QueryCancelledException.CANCEL_MSG,
                    () -> await(result)
            );
        }
    }
}
