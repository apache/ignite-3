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

package org.apache.ignite.internal.jdbc;

import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

/**
 * Unit tests for {@link JdbcStatement}.
 */
public class JdbcStatementSelfTest extends BaseIgniteAbstractTest {

    @Test
    public void close() throws SQLException {
        try (Statement stmt = createStatement()) {
            assertFalse(stmt.isClosed());

            stmt.close();
            assertTrue(stmt.isClosed());

            expectClosed(() -> stmt.executeQuery("SELECT 1"));
            expectClosed(() -> stmt.executeUpdate("UPDATE t SET val=1"));

            expectClosed(() -> stmt.setMaxFieldSize(100_000));
            expectClosed(stmt::getMaxFieldSize);

            expectClosed(() -> stmt.setEscapeProcessing(true));
            expectClosed(() -> stmt.setEscapeProcessing(false));

            expectClosed(() -> stmt.setQueryTimeout(-1));
            expectClosed(() -> stmt.setQueryTimeout(1));
            expectClosed(stmt::getQueryTimeout);

            expectClosed(stmt::cancel);

            expectClosed(stmt::getWarnings);
            expectClosed(stmt::clearWarnings);

            expectClosed(() -> stmt.setCursorName("C"));
            expectClosed(() -> stmt.setCursorName(null));

            expectClosed(() -> stmt.execute("SELECT 1"));

            expectClosed(stmt::getResultSet);

            expectClosed(stmt::getUpdateCount);

            expectClosed(stmt::getMoreResults);

            expectClosed(() -> stmt.setFetchDirection(ResultSet.FETCH_FORWARD));
            expectClosed(() -> stmt.setFetchDirection(ResultSet.FETCH_REVERSE));
            expectClosed(() -> stmt.setFetchDirection(ResultSet.FETCH_UNKNOWN));
            expectClosed(stmt::getFetchDirection);

            expectClosed(() -> stmt.setFetchSize(-1));
            expectClosed(() -> stmt.setFetchSize(0));
            expectClosed(() -> stmt.setFetchSize(1));
            expectClosed(stmt::getFetchSize);

            expectClosed(stmt::getResultSetConcurrency);
            expectClosed(stmt::getResultSetType);

            expectClosed(() -> stmt.addBatch("UPDATE t SET val=2"));

            expectClosed(stmt::clearBatch);

            expectClosed(stmt::executeBatch);

            expectClosed(stmt::getConnection);

            expectClosed(stmt::getMoreResults);

            expectClosed(() -> stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            expectClosed(() -> stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            expectClosed(() -> stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

            expectClosed(stmt::getGeneratedKeys);

            expectClosed(() -> stmt.executeUpdate("UPDATE t SET val=2", Statement.RETURN_GENERATED_KEYS));
            expectClosed(() -> stmt.executeUpdate("UPDATE t SET val=2", Statement.NO_GENERATED_KEYS));

            expectClosed(() -> stmt.executeUpdate("UPDATE t SET val=2", new int[]{0}));
            expectClosed(() -> stmt.executeUpdate("UPDATE t SET val=2", new String[]{"C1"}));

            expectClosed(stmt::getResultSetHoldability);

            expectClosed(() -> stmt.setPoolable(true));
            expectClosed(() -> stmt.setPoolable(false));
            expectClosed(stmt::isPoolable);

            expectClosed(stmt::closeOnCompletion);
            expectClosed(stmt::isCloseOnCompletion);
        }
    }

    @Test
    public void setMaxFieldSize() throws SQLException {
        try (Statement stmt = createStatement()) {
            assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Field size limit is not supported.",
                    () -> stmt.setMaxFieldSize(1)
            );

            assertEquals(0, stmt.getMaxFieldSize());

            assertThrowsSqlException(
                    SQLException.class,
                    "Invalid field limit.",
                    () -> stmt.setMaxFieldSize(-1)
            );
        }
    }

    @Test
    public void queryTimeout() throws SQLException {
        try (Statement stmt = createStatement()) {
            stmt.setQueryTimeout(2);
            assertEquals(2, stmt.getQueryTimeout());

            stmt.setQueryTimeout(0);
            assertEquals(0, stmt.getQueryTimeout());

            stmt.setQueryTimeout(4);
            assertEquals(4, stmt.getQueryTimeout());

            assertThrowsSqlException(
                    SQLException.class,
                    "Invalid timeout value.",
                    () -> stmt.setQueryTimeout(-1)
            );

            // No changes
            assertEquals(4, stmt.getQueryTimeout());
        }
    }

    @Test
    public void setMaxRows() throws SQLException {
        try (Statement stmt = createStatement()) {
            // Unlimited
            assertEquals(0, stmt.getMaxRows());

            stmt.setMaxRows(1000);
            assertEquals(1000, stmt.getMaxRows());

            // Change to unlimited
            stmt.setMaxRows(0);
            assertEquals(0, stmt.getMaxRows());

            assertThrowsSqlException(SQLException.class,
                    "Invalid max rows value.",
                    () -> stmt.setMaxRows(-1)
            );
        }
    }

    @Test
    public void setEscapeProcessing() throws SQLException {
        try (Statement stmt = createStatement()) {
            // Does nothing
            stmt.setEscapeProcessing(true);
            stmt.setEscapeProcessing(false);
        }
    }

    @Test
    public void warnings() throws SQLException {
        try (Statement stmt = createStatement()) {
            // Does nothing
            assertNull(stmt.getWarnings());
            stmt.clearWarnings();
        }
    }

    @Test
    public void setCursorName() throws SQLException {
        try (Statement stmt = createStatement()) {
            String error = "Setting cursor name is not supported.";

            assertThrowsSqlException(SQLException.class,
                    error,
                    () -> stmt.setCursorName("C")
            );

            assertThrowsSqlException(SQLException.class,
                    error,
                    () -> stmt.setCursorName(null)
            );
        }
    }

    @Test
    public void setFetchDirection() throws SQLException {
        try (Statement stmt = createStatement()) {
            // Initial state
            int value = ResultSet.FETCH_FORWARD;
            assertEquals(value, stmt.getFetchDirection());

            stmt.setFetchDirection(value);
            assertEquals(value, stmt.getFetchDirection());

            // Does not change anything
            assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Only forward direction is supported.",
                    () -> stmt.setFetchDirection(ResultSet.FETCH_REVERSE)
            );
            assertEquals(value, stmt.getFetchDirection());

            // Does not change anything
            assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Only forward direction is supported.",
                    () -> stmt.setFetchDirection(ResultSet.FETCH_UNKNOWN)
            );
            assertEquals(value, stmt.getFetchDirection());
        }
    }

    @Test
    public void setFetchSize() throws SQLException {
        try (Statement stmt = createStatement()) {
            // Fetch size is a hint
            assertEquals(0, stmt.getFetchSize());

            stmt.setFetchSize(1000);
            assertEquals(1000, stmt.getFetchSize());

            stmt.setFetchSize(0);
            assertEquals(0, stmt.getFetchSize());

            assertThrowsSqlException(SQLException.class,
                    "Invalid fetch size.",
                    () -> stmt.setFetchSize(-1)
            );
        }
    }

    @Test
    public void getResultSetConcurrency() throws SQLException {
        try (Statement stmt = createStatement()) {
            assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
        }
    }

    @Test
    public void getResultSetType() throws SQLException {
        try (Statement stmt = createStatement()) {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
        }
    }

    @Test
    public void getConnection() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);

        try (Statement stmt = createStatement(connection)) {
            assertSame(connection, stmt.getConnection());
        }
    }

    @Test
    public void getMoreResults() throws SQLException {
        try (Statement stmt = createStatement()) {
            // Nothing
            assertFalse(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));

            String error = "Multiple open results are not supported.";

            assertThrowsSqlException(SQLException.class,
                    error,
                    () -> stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

            assertThrowsSqlException(SQLException.class,
                    error,
                    () -> stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        }
    }

    @Test
    public void getGeneratedKeys() throws SQLException {
        try (Statement stmt = createStatement()) {
            assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Returning auto-generated keys is not supported.",
                    stmt::getGeneratedKeys
            );
        }
    }

    @Test
    public void updateWithColumns() throws SQLException {
        try (Statement stmt = createStatement()) {
            String sql = "UPDATE t SET c = 1";
            String error = "Returning auto-generated keys is not supported.";

            assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    error,
                    () -> stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS)
            );

            assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    error,
                    () -> stmt.executeUpdate(sql, new int[]{1})
            );

            assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    error,
                    () -> stmt.executeUpdate(sql, new String[]{"id"})
            );
        }
    }

    @Test
    public void getResultSetHoldability() throws SQLException {
        try (Statement stmt = createStatement()) {
            // any value
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
        }
    }

    @Test
    public void setPoolable() throws SQLException {
        try (Statement stmt = createStatement()) {
            assertFalse(stmt.isPoolable());

            String error = "Pooling is not supported.";

            assertThrowsSqlException(SQLException.class, error, () -> {
                stmt.setPoolable(true);
            });

            // Nothing happens
            stmt.setPoolable(false);
            assertFalse(stmt.isPoolable());
        }
    }

    @Test
    public void closeOnCompletion() throws SQLException {
        try (Statement stmt = createStatement()) {
            JdbcStatement jdbc = stmt.unwrap(JdbcStatement.class);

            ClientSyncResultSet igniteRs = Mockito.mock(ClientSyncResultSet.class);
            when(igniteRs.metadata()).thenReturn(new ResultSetMetadataImpl(List.of()));

            {
                ResultSet rs = jdbc.createResultSet(igniteRs);
                rs.close();
                assertFalse(stmt.isClosed());
            }

            stmt.closeOnCompletion();

            {
                ResultSet rs = jdbc.createResultSet(igniteRs);
                rs.close();
                assertTrue(stmt.isClosed());
            }
        }
    }

    @Test
    public void largeMaxRows() throws SQLException {
        try (Statement stmt = createStatement()) {
            assertThrows(UnsupportedOperationException.class, () -> stmt.setLargeMaxRows(1));

            assertEquals(0, stmt.getLargeMaxRows());
        }
    }

    @Test
    public void largeUpdateMethods() throws SQLException {
        try (Statement stmt = createStatement()) {
            String error = "executeLargeUpdate not implemented";

            assertThrowsSqlException(SQLFeatureNotSupportedException.class,
                    () -> stmt.executeLargeUpdate("UPDATE t SET val=2"));

            assertThrowsSqlException(SQLFeatureNotSupportedException.class,
                    error,
                    () -> stmt.executeLargeUpdate("UPDATE t SET val=2", Statement.RETURN_GENERATED_KEYS));

            assertThrowsSqlException(SQLFeatureNotSupportedException.class,
                    error,
                    () -> stmt.executeLargeUpdate("UPDATE t SET val=2", Statement.NO_GENERATED_KEYS));

            assertThrowsSqlException(SQLFeatureNotSupportedException.class,
                    error,
                    () -> stmt.executeLargeUpdate("UPDATE t SET val=2", new int[]{0}));

            assertThrowsSqlException(SQLFeatureNotSupportedException.class,
                    error,
                    () -> stmt.executeLargeUpdate("UPDATE t SET val=2", new String[]{"C1"}));
        }
    }

    protected Statement createStatement() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        JdbcConnection connection2 = Mockito.mock(JdbcConnection.class);

        ConnectionProperties properties = new ConnectionPropertiesImpl();

        when(connection.unwrap(JdbcConnection.class)).thenReturn(connection2);
        when(connection2.properties()).thenReturn(properties);

        return createStatement(connection);
    }

    protected Statement createStatement(Connection connection) {
        IgniteSql igniteSql = Mockito.mock(IgniteSql.class);
        return new JdbcStatement(connection, igniteSql, "PUBLIC", ResultSet.HOLD_CURSORS_OVER_COMMIT, 0);
    }

    static void expectClosed(Executable method) {
        assertThrowsSqlException(SQLException.class, "Statement is closed.", method);
    }
}
