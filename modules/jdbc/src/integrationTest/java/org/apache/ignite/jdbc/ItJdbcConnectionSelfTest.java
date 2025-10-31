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

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.jdbc2.JdbcConnection2;
import org.apache.ignite.jdbc.util.JdbcTestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItJdbcConnectionSelfTest extends AbstractJdbcSelfTest {
    /**
     * Test JDBC loading via ServiceLoader.
     */
    @Test
    public void testServiceLoader() {
        ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class);

        IgniteJdbcDriver igniteJdbcDriver = null;

        for (Driver driver : sl) {
            if (driver instanceof IgniteJdbcDriver) {
                igniteJdbcDriver = ((IgniteJdbcDriver) driver);
                break;
            }
        }

        assertNotNull(igniteJdbcDriver);
    }

    @SuppressWarnings({"EmptyTryBlock", "unused"})
    @Test
    public void testDefaults() throws Exception {
        var url = "jdbc:ignite:thin://127.0.0.1:10800";

        try (Connection conn = DriverManager.getConnection(url)) {
            // No-op.
        }

        try (Connection conn = DriverManager.getConnection(url + "/")) {
            // No-op.
        }
    }

    @SuppressWarnings({"EmptyTryBlock", "unused"})
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15611")
    @Test
    public void testDefaultsIpv6() throws Exception {
        var url = "jdbc:ignite:thin://[::1]:10800";

        try (Connection conn = DriverManager.getConnection(url)) {
            // No-op.
        }

        try (Connection conn = DriverManager.getConnection(url + "/")) {
            // No-op.
        }
    }

    /**
     * Test invalid endpoint.
     */
    @Test
    public void testInvalidEndpoint() {
        assertInvalid("jdbc:ignite:thin://", "Address is empty");
        assertInvalid("jdbc:ignite:thin://:10000", "Host name is empty");
        assertInvalid("jdbc:ignite:thin://     :10000", "Host name is empty");

        assertInvalid("jdbc:ignite:thin://127.0.0.1:-1", "(invalid port -1)");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:0", "(invalid port 0)");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:100000",
                "(invalid port 100000)");

        assertInvalid("jdbc:ignite:thin://[::1]:-1", "(invalid port -1)");
        assertInvalid("jdbc:ignite:thin://[::1]:0", "(invalid port 0)");
        assertInvalid("jdbc:ignite:thin://[::1]:100000",
                "(invalid port 100000)");
    }

    /**
     * Test schema property in URL.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSchema() throws Exception {
        assertInvalid(URL + "/qwe/qwe",
                "Invalid URL format (only schema name is allowed in URL path parameter 'host:port[/schemaName]')");

        try (Connection conn = DriverManager.getConnection(URL + "/public")) {
            assertEquals("public", conn.getSchema(), "Invalid schema");
        }

        String dfltSchema = "DEFAULT";

        try (Connection conn = DriverManager.getConnection(URL + "/\"" + dfltSchema + '"')) {
            assertEquals("\"DEFAULT\"", conn.getSchema(), "Invalid schema");
        }

        try (Connection conn = DriverManager.getConnection(URL + "/_not_exist_schema_")) {
            assertEquals("_not_exist_schema_", conn.getSchema(), "Invalid schema");
        }
    }

    /**
     * Test schema property in URL with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSchemaSemicolon() throws Exception {
        String dfltSchema = "DEFAULT";

        try (Connection conn = DriverManager.getConnection(URL + ";schema=public")) {
            assertEquals("public", conn.getSchema(), "Invalid schema");
        }

        try (Connection conn =
                DriverManager.getConnection(URL + ";schema=\"" + dfltSchema + '"')) {
            assertEquals("\"DEFAULT\"", conn.getSchema(), "Invalid schema");
        }

        try (Connection conn = DriverManager.getConnection(URL + ";schema=_not_exist_schema_")) {
            assertEquals("_not_exist_schema_", conn.getSchema(), "Invalid schema");
        }
    }

    /**
     * Assert that provided URL is invalid.
     *
     * @param url URL.
     * @param errMsg Error message.
     */
    private void assertInvalid(String url, String errMsg) {
        JdbcTestUtils.assertThrowsSqlException(errMsg, () -> DriverManager.getConnection(url));
    }

    @Test
    public void testClose() throws Exception {
        Connection conn;

        try (Connection conn0 = DriverManager.getConnection(URL)) {
            conn = conn0;

            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }

        assertTrue(conn.isClosed());

        assertFalse(conn.isValid(2), "Connection must be closed");

        JdbcTestUtils.assertThrowsSqlException("Invalid timeout", () -> conn.isValid(-2));
    }

    @Test
    public void testCreateStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            try (Statement stmt = conn.createStatement()) {
                assertNotNull(stmt);

                stmt.close();

                conn.close();

                // Exception when called on closed connection
                checkConnectionClosed(() -> conn.createStatement());
            }
        }
    }

    /**
     * Test create statement.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateStatement2() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            int[] rsTypes = {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            DatabaseMetaData meta = conn.getMetaData();

            for (int type : rsTypes) {
                for (int concur : rsConcurs) {
                    if (meta.supportsResultSetConcurrency(type, concur)) {
                        assertEquals(TYPE_FORWARD_ONLY, type);
                        assertEquals(CONCUR_READ_ONLY, concur);

                        try (Statement stmt = conn.createStatement(type, concur)) {
                            assertNotNull(stmt);

                            assertEquals(type, stmt.getResultSetType());
                            assertEquals(concur, stmt.getResultSetConcurrency());
                        }

                        continue;
                    }

                    JdbcTestUtils.assertThrowsSqlException(SQLFeatureNotSupportedException.class, () -> conn.createStatement(type, concur));
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY));
        }
    }

    /**
     * Test create statement.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            int[] rsTypes = {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            int[] rsHoldabilities = {CLOSE_CURSORS_AT_COMMIT};

            DatabaseMetaData meta = conn.getMetaData();

            for (int type : rsTypes) {
                for (int concur : rsConcurs) {
                    for (int holdabililty : rsHoldabilities) {
                        if (meta.supportsResultSetConcurrency(type, concur)) {
                            assertEquals(TYPE_FORWARD_ONLY, type);
                            assertEquals(CONCUR_READ_ONLY, concur);

                            try (Statement stmt = conn.createStatement(type, concur, holdabililty)) {
                                assertNotNull(stmt);

                                assertEquals(type, stmt.getResultSetType());
                                assertEquals(concur, stmt.getResultSetConcurrency());
                                assertEquals(holdabililty, stmt.getResultSetHoldability());
                            }

                            continue;
                        }

                        JdbcTestUtils.assertThrowsSqlException(SQLFeatureNotSupportedException.class,
                                () -> conn.createStatement(type, concur, holdabililty));
                    }
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT));
        }
    }

    @Test
    public void testPrepareStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // null query text
            JdbcTestUtils.assertThrowsSqlException(
                    "SQL string cannot be null",
                    () -> conn.prepareStatement(null)
            );

            final String sqlText = "select * from test where param = ?";

            try (PreparedStatement prepared = conn.prepareStatement(sqlText)) {
                assertNotNull(prepared);
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.prepareStatement(sqlText));
        }
    }

    /**
     * Test prepare statement.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "select * from test where param = ?";

            int[] rsTypes = {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            DatabaseMetaData meta = conn.getMetaData();

            for (int type : rsTypes) {
                for (int concur : rsConcurs) {
                    if (meta.supportsResultSetConcurrency(type, concur)) {
                        assertEquals(TYPE_FORWARD_ONLY, type);
                        assertEquals(CONCUR_READ_ONLY, concur);

                        // null query text
                        JdbcTestUtils.assertThrowsSqlException(
                                "SQL string cannot be null",
                                () -> conn.prepareStatement(null, type, concur)
                        );

                        continue;
                    }

                    JdbcTestUtils.assertThrowsSqlException(
                            SQLFeatureNotSupportedException.class,
                            () -> conn.prepareStatement(sqlText, type, concur)
                    );
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY));

            conn.close();
        }
    }

    /**
     * Test prepare statement.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareStatement4() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "select * from test where param = ?";

            int[] rsTypes = {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            int[] rsHoldabilities = {HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    for (final int holdabililty : rsHoldabilities) {
                        if (meta.supportsResultSetConcurrency(type, concur)) {
                            assertEquals(TYPE_FORWARD_ONLY, type);
                            assertEquals(CONCUR_READ_ONLY, concur);

                            // null query text
                            JdbcTestUtils.assertThrowsSqlException(
                                    "SQL string cannot be null",
                                    () -> conn.prepareStatement(null, type, concur, holdabililty)
                            );

                            continue;
                        }

                        JdbcTestUtils.assertThrowsSqlException(
                                SQLFeatureNotSupportedException.class,
                                () -> conn.prepareStatement(sqlText, type, concur, holdabililty)
                        );
                    }
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(
                    () -> conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)
            );

            conn.close();
        }
    }

    @Test
    public void testPrepareStatementAutoGeneratedKeysUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "insert into test (val) values (?)";

            String error = "Returning auto-generated keys is not supported.";

            JdbcTestUtils.assertThrowsSqlException(
                    error,
                    () -> conn.prepareStatement(sqlText, RETURN_GENERATED_KEYS)
            );

            try (PreparedStatement stmt = conn.prepareStatement("SELECT 1", NO_GENERATED_KEYS)) {
                stmt.executeQuery().close();
            }

            JdbcTestUtils.assertThrowsSqlException(
                    error,
                    () -> conn.prepareStatement(sqlText, new int[]{1})
            );

            JdbcTestUtils.assertThrowsSqlException(
                    error,
                    () -> conn.prepareStatement(sqlText, new String[]{"ID"})
            );
        }
    }

    @Test
    public void testPrepareCallUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "exec test()";

            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Callable functions are not supported.",
                    () -> conn.prepareCall(sqlText)
            );

            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Callable functions are not supported.",
                    () -> conn.prepareCall(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)
            );

            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Callable functions are not supported.",
                    () -> conn.prepareCall(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)
            );
        }
    }

    @Test
    public void testNativeSql() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // null query text
            assertThrows(
                    NullPointerException.class,
                    () -> conn.nativeSQL(null)
            );

            final String sqlText = "select * from test";

            assertEquals(sqlText, conn.nativeSQL(sqlText));

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.nativeSQL(sqlText));
        }
    }

    @Test
    public void testCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Should not be called in auto-commit mode
            JdbcTestUtils.assertThrowsSqlException(
                    "Transaction cannot be committed explicitly in auto-commit mode",
                    conn::commit
            );

            assertTrue(conn.getAutoCommit());

            // Should not be called in auto-commit mode
            JdbcTestUtils.assertThrowsSqlException(
                    "Transaction cannot be committed explicitly in auto-commit mode.",
                    conn::commit
            );

            conn.setAutoCommit(false);
            assertFalse(conn.getAutoCommit());

            // No exception is expected.
            conn.commit();

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(conn::commit);
        }
    }

    @Test
    public void testRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Should not be called in auto-commit mode
            JdbcTestUtils.assertThrowsSqlException(
                    "Transaction cannot be rolled back explicitly in auto-commit mode.",
                    conn::rollback
            );

            conn.setAutoCommit(false);

            // No exception is expected.
            conn.rollback();

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(conn::rollback);
        }
    }

    /**
     * Test get metadata.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertNotNull(meta);

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(conn::getMetaData);
        }
    }

    @Test
    public void testGetSetReadOnly() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.setReadOnly(true));

            // Exception when called on closed connection
            checkConnectionClosed(conn::isReadOnly);
        }
    }

    @Test
    public void testGetSetCatalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertFalse(conn.getMetaData().supportsCatalogsInDataManipulation());

            assertEquals("IGNITE", conn.getCatalog());

            conn.setCatalog("catalog");

            assertEquals("IGNITE", conn.getCatalog());

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.setCatalog(""));

            // Exception when called on closed connection
            checkConnectionClosed(conn::getCatalog);
        }
    }

    @Test
    public void testGetSetTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Invalid parameter value
            JdbcTestUtils.assertThrowsSqlException(
                    "Invalid transaction isolation level",
                    () -> conn.setTransactionIsolation(-1)
            );

            // default level
            assertEquals(TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());

            int[] levels = {TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED,
                    TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE};

            for (int level : levels) {
                conn.setTransactionIsolation(level);
                assertEquals(level, conn.getTransactionIsolation());
            }

            conn.close();

            // Exception when called on closed connection

            checkConnectionClosed(conn::getTransactionIsolation);

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.setTransactionIsolation(TRANSACTION_SERIALIZABLE));
        }
    }

    @Test
    public void testClearGetWarnings() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            SQLWarning warn = conn.getWarnings();

            assertNull(warn);

            conn.clearWarnings();

            warn = conn.getWarnings();

            assertNull(warn);

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(conn::getWarnings);

            // Exception when called on closed connection
            checkConnectionClosed(conn::clearWarnings);
        }
    }

    @Test
    public void testGetSetTypeMap() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Types mapping is not supported",
                    conn::getTypeMap
            );

            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "Types mapping is not supported",
                    () -> conn.setTypeMap(new HashMap<>())
            );

            conn.close();

            // Exception when called on closed connection

            JdbcTestUtils.assertThrowsSqlException(
                    "Connection is closed",
                    conn::getTypeMap
            );

            JdbcTestUtils.assertThrowsSqlException(
                    "Connection is closed",
                    () -> conn.setTypeMap(new HashMap<>())
            );
        }
    }

    /**
     * Get-set holdability test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetHoldability() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // default value
            assertEquals(conn.getMetaData().getResultSetHoldability(), conn.getHoldability());

            assertEquals(CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

            // Invalid constant

            JdbcTestUtils.assertThrowsSqlException(
                    "Invalid result set holdability (only close cursors at commit option is supported)",
                    () -> conn.setHoldability(-1)
            );

            conn.close();

            JdbcTestUtils.assertThrowsSqlException(
                    "Connection is closed",
                    conn::getHoldability
            );

            JdbcTestUtils.assertThrowsSqlException(
                    "Connection is closed",
                    () -> conn.setHoldability(HOLD_CURSORS_OVER_COMMIT)
            );
        }
    }

    @Test
    public void testCreateClob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Unsupported
            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "SQL-specific types are not supported",
                    conn::createClob
            );

            conn.close();

            JdbcTestUtils.assertThrowsSqlException(
                    "Connection is closed",
                    conn::createClob
            );
        }
    }

    @Test
    public void testCreateBlob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Unsupported
            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "SQL-specific types are not supported",
                    conn::createBlob
            );

            conn.close();

            JdbcTestUtils.assertThrowsSqlException(
                    "Connection is closed",
                    conn::createBlob
            );
        }
    }

    @Test
    public void testCreateNclob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Unsupported
            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "SQL-specific types are not supported",
                    conn::createNClob
            );

            conn.close();

            JdbcTestUtils.assertThrowsSqlException(
                    "Connection is closed",
                    conn::createNClob
            );
        }
    }

    @Test
    public void testCreateSqlXml() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Unsupported
            JdbcTestUtils.assertThrowsSqlException(
                    SQLFeatureNotSupportedException.class,
                    "SQL-specific types are not supported",
                    conn::createSQLXML
            );

            conn.close();

            JdbcTestUtils.assertThrowsSqlException(
                    "Connection is closed",
                    conn::createSQLXML
            );
        }
    }

    @Test
    public void testGetSetClientInfoPair() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            assertNull(conn.getWarnings());

            conn.setClientInfo(name, val);

            assertNull(conn.getClientInfo(val));

            conn.close();

            checkConnectionClosed(() -> conn.getClientInfo(name));

            JdbcTestUtils.assertThrowsSqlException(
                    SQLClientInfoException.class,
                    "Connection is closed",
                    () -> conn.setClientInfo(name, val)
            );
        }
    }

    @Test
    public void testGetSetClientInfoProperties() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            Properties props = new Properties();
            props.setProperty(name, val);

            conn.setClientInfo(props);

            Properties propsResult = conn.getClientInfo();

            assertNotNull(propsResult);

            assertTrue(propsResult.isEmpty());

            conn.close();

            checkConnectionClosed(conn::getClientInfo);

            JdbcTestUtils.assertThrowsSqlException(
                    SQLClientInfoException.class,
                    "Connection is closed",
                    () -> conn.setClientInfo(props)
            );
        }
    }

    @Test
    public void testCreateArrayOf() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String typeName = "varchar";

            String[] elements = {"apple", "pear"};

            // Invalid typename
            JdbcTestUtils.assertThrowsSqlException(
                    "SQL-specific types are not supported.",
                    () -> conn.createArrayOf(null, null)
            );

            // Unsupported

            checkNotSupported(() -> conn.createArrayOf(typeName, elements));

            conn.close();

            checkConnectionClosed(() -> conn.createArrayOf(typeName, elements));
        }
    }

    @Test
    public void testCreateStruct() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Invalid typename
            JdbcTestUtils.assertThrowsSqlException(
                    "SQL-specific types are not supported.",
                    () -> conn.createStruct(null, null)
            );

            final String typeName = "employee";

            Object[] attrs = {100, "Tom"};

            checkNotSupported(() -> conn.createStruct(typeName, attrs));

            conn.close();

            checkConnectionClosed(() -> conn.createStruct(typeName, attrs));
        }
    }

    @Test
    public void testGetSetSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertEquals("PUBLIC", conn.getSchema());

            final String schema = "test";

            conn.setSchema(schema);

            assertEquals("test", conn.getSchema());

            conn.setSchema('"' + schema + '"');

            assertEquals("\"test\"", conn.getSchema());

            conn.close();

            checkConnectionClosed(() -> conn.setSchema(schema));

            checkConnectionClosed(conn::getSchema);
        }
    }

    @Test
    public void testAbort() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Invalid executor.
            JdbcTestUtils.assertThrowsSqlException(
                    "Executor cannot be null",
                    () -> conn.abort(null)
            );

            Executor executor = Executors.newFixedThreadPool(1);

            conn.abort(executor);

            assertTrue(conn.isClosed());
        }
    }

    @Test
    public void testGetSetNetworkTimeout() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // default
            assertEquals(0, conn.getNetworkTimeout());

            final Executor executor = Executors.newFixedThreadPool(1);

            final int timeout = 1000;

            // Invalid timeout.
            JdbcTestUtils.assertThrowsSqlException(
                    "Network timeout cannot be negative",
                    () -> conn.setNetworkTimeout(executor, -1)
            );

            conn.setNetworkTimeout(executor, timeout);

            assertEquals(timeout, conn.getNetworkTimeout());

            conn.close();

            checkConnectionClosed(conn::getNetworkTimeout);

            checkConnectionClosed(() -> conn.setNetworkTimeout(executor, timeout));
        }
    }

    @Test
    public void testCurrentUser() throws Exception {
        var url = "jdbc:ignite:thin://127.0.0.1:10800";

        try (Connection conn = DriverManager.getConnection(url)) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT CURRENT_USER")) {
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("unknown", rs.getString(1));
                }
            }
        }
    }

    @Test
    public void testChangePartitionAwarenessCacheSize() throws SQLException {
        String urlPrefix = URL + "?partitionAwarenessMetadataCacheSize";

        assertInvalid(urlPrefix + "=A",
                "Failed to parse int property [name=partitionAwarenessMetadataCacheSize, value=A]");

        assertInvalid(urlPrefix + "=-1",
                "Property cannot be lower than 0 [name=partitionAwarenessMetadataCacheSize, value=-1]");

        assertInvalid(urlPrefix + "=2147483648",
                "Failed to parse int property [name=partitionAwarenessMetadataCacheSize, value=2147483648]");

        try (JdbcConnection2 conn = (JdbcConnection2) DriverManager.getConnection(urlPrefix + "=2147483647")) {
            assertEquals(Integer.MAX_VALUE, conn.properties().getPartitionAwarenessMetadataCacheSize());
        }

        try (JdbcConnection2 conn = (JdbcConnection2) DriverManager.getConnection(urlPrefix + "=0")) {
            assertEquals(0, conn.properties().getPartitionAwarenessMetadataCacheSize());
        }
    }
}
