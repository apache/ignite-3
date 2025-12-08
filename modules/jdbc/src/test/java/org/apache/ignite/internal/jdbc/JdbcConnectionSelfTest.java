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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.internal.jdbc.proto.JdbcDatabaseMetadataHandler;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.IgniteSql;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

/**
 * Tests for {@link JdbcConnection}.
 */
public class JdbcConnectionSelfTest extends BaseIgniteAbstractTest {
    /** Dummy connection url. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1:10800/";

    @Test
    public void nativeSql() throws SQLException {
        try (Connection conn = createConnection()) {
            String sql = "SELECT 1";
            String nativeSql = conn.nativeSQL(sql);
            assertSame(sql, nativeSql);
        }
    }

    @Test
    public void readOnly() throws SQLException {
        try (Connection conn = createConnection()) {
            assertFalse(conn.isReadOnly());

            conn.setReadOnly(true);
            assertTrue(conn.isReadOnly());

            conn.setReadOnly(false);
            assertFalse(conn.isReadOnly());
        }
    }

    @Test
    public void close() throws SQLException {
        try (Connection conn = createConnection()) {
            assertFalse(conn.isClosed());

            conn.close();

            assertTrue(conn.isClosed());

            expectClosed(conn::createStatement);

            expectClosed(() -> conn.prepareStatement("SELECT ?"));

            expectClosed(() -> conn.prepareCall("SELECT F()"));

            expectClosed(() -> conn.nativeSQL("SELECT 1"));

            expectClosed(() -> conn.setAutoCommit(false));
            expectClosed(conn::getAutoCommit);
            expectClosed(() -> conn.setAutoCommit(true));

            expectClosed(conn::commit);

            expectClosed(conn::rollback);

            expectClosed(conn::getMetaData);

            expectClosed(() -> conn.setReadOnly(true));
            expectClosed(conn::isReadOnly);

            expectClosed(() -> conn.setCatalog("C"));
            expectClosed(() -> conn.setCatalog(null));
            expectClosed(conn::getCatalog);

            expectClosed(() -> conn.setTransactionIsolation(Connection.TRANSACTION_NONE));
            expectClosed(conn::getTransactionIsolation);

            expectClosed(conn::getWarnings);
            expectClosed(conn::clearWarnings);

            expectClosed(() -> conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

            expectClosed(() -> conn.prepareStatement("SELECT ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

            expectClosed(() -> conn.prepareCall("SELECT F()", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

            expectClosed(conn::getTypeMap);

            expectClosed(() -> conn.setTypeMap(Map.of()));

            expectClosed(() -> conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
            expectClosed(conn::getHoldability);

            expectClosed(conn::setSavepoint);
            expectClosed(() -> conn.setSavepoint("S"));

            Savepoint savepoint = Mockito.mock(Savepoint.class);
            expectClosed(() -> conn.rollback(savepoint));
            expectClosed(() -> conn.releaseSavepoint(savepoint));

            expectClosed(() -> conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectClosed(() -> conn.prepareStatement("SELECT ?",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectClosed(() -> conn.prepareCall("SELECT F()",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectClosed(() -> conn.prepareStatement("SELECT ?", Statement.NO_GENERATED_KEYS));
            expectClosed(() -> conn.prepareStatement("SELECT ?", Statement.RETURN_GENERATED_KEYS));

            expectClosed(() -> conn.prepareStatement("SELECT ?", new int[]{1}));

            expectClosed(() -> conn.prepareStatement("SELECT ?", new String[]{"id"}));

            expectClosed(conn::createClob);

            expectClosed(conn::createBlob);

            expectClosed(conn::createNClob);

            expectClosed(conn::createSQLXML);

            expectClosed(() -> conn.setClientInfo("A", "B"));
            expectClosed(() -> conn.setClientInfo(new Properties()));
            expectClosed(conn::getClientInfo);

            expectClosed(() -> conn.createArrayOf("INTEGER", new Object[0]));
            expectClosed(() -> conn.createStruct("MyStruct", new Object[0]));

            expectClosed(() -> conn.getClientInfo("A"));
            expectClosed(() -> conn.setSchema("S"));

            expectClosed(conn::getSchema);

            expectClosed(() -> conn.setNetworkTimeout(Runnable::run, 1));
            expectClosed(conn::getNetworkTimeout);

            expectClosed(conn::beginRequest);
            expectClosed(conn::endRequest);

            ShardingKey shardingKey = Mockito.mock(ShardingKey.class);
            ShardingKey subShardingKey = Mockito.mock(ShardingKey.class);

            expectClosed(() -> conn.setShardingKeyIfValid(shardingKey, subShardingKey, 1));
            expectClosed(() -> conn.setShardingKeyIfValid(shardingKey, 1));

            expectClosed(() -> conn.setShardingKey(shardingKey));
            expectClosed(() -> conn.setShardingKey(shardingKey, subShardingKey));
        }
    }

    @Test
    public void abort() throws SQLException {
        try (Connection conn = createConnection()) {
            conn.abort(Runnable::run);

            assertTrue(conn.isClosed());

            // Does nothing
            conn.close();
            assertTrue(conn.isClosed());
        }
    }

    @Test
    public void notSupportedMethods() throws SQLException {
        try (Connection conn = createConnection()) {

            expectNotSupported(conn::getTypeMap);
            expectNotSupported(() -> conn.setTypeMap(Map.of()));

            conn.setAutoCommit(false);
            expectNotSupported(conn::setSavepoint);
            expectNotSupported(() -> conn.setSavepoint("S"));

            Savepoint savepoint = Mockito.mock(Savepoint.class);
            expectNotSupported(() -> conn.rollback(savepoint));
            expectNotSupported(() -> conn.releaseSavepoint(savepoint));

            // createStatement - not supported flags

            expectNotSupported(() -> conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            // prepareStatement - not supported flags
            expectNotSupported(() -> conn.prepareStatement("SELECT ?", Statement.RETURN_GENERATED_KEYS));

            expectNotSupported(() -> conn.prepareStatement("SELECT ?", ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.prepareStatement("SELECT ?", ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.prepareStatement("SELECT ?", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.prepareStatement("SELECT ?", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.prepareStatement("SELECT ?", new int[]{1}));
            expectNotSupported(() -> conn.prepareStatement("SELECT ?", new String[]{"id"}));

            // prepareCall

            expectNotSupported(() -> conn.prepareCall("SELECT F()"));

            expectNotSupported(() -> conn.prepareCall("SELECT F()", ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.prepareCall("SELECT F()", ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.prepareCall("SELECT ?", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT));

            expectNotSupported(() -> conn.prepareCall("SELECT F()", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));

            // Sharding key

            ShardingKey shardingKey = Mockito.mock(ShardingKey.class);
            ShardingKey subShardingKey = Mockito.mock(ShardingKey.class);

            expectNotSupported(() -> conn.setShardingKeyIfValid(shardingKey, subShardingKey, 1));
            expectNotSupported(() -> conn.setShardingKeyIfValid(shardingKey, 1));

            expectNotSupported(() -> conn.setShardingKey(shardingKey));
            expectNotSupported(() -> conn.setShardingKey(shardingKey, subShardingKey));
        }
    }

    @Test
    public void notSupportedTypes() throws SQLException {
        try (Connection conn = createConnection()) {
            expectNotSupported(conn::createClob);
            expectNotSupported(conn::createBlob);
            expectNotSupported(conn::createNClob);
            expectNotSupported(conn::createSQLXML);

            expectNotSupported(() -> conn.createArrayOf("INTEGER", new Object[0]));
            expectNotSupported(() -> conn.createStruct("MyStruct", new Object[0]));
        }
    }

    @Test
    public void catalog() throws SQLException {
        try (Connection conn = createConnection()) {
            assertEquals("IGNITE", conn.getCatalog());
            // Does nothing
            conn.setCatalog("C");
            assertEquals("IGNITE", conn.getCatalog());
        }
    }

    @Test
    public void transactionIsolation() throws SQLException {
        try (Connection conn = createConnection()) {
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());

            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, conn.getTransactionIsolation());

            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn.getTransactionIsolation());

            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            assertEquals(Connection.TRANSACTION_REPEATABLE_READ, conn.getTransactionIsolation());

            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());

            assertThrowsSqlException(SQLException.class,
                    "Invalid transaction isolation level",
                    () -> conn.setTransactionIsolation(123456)
            );

            // Does not change anything
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());

            assertThrowsSqlException(SQLException.class,
                    "Cannot set transaction isolation level to TRANSACTION_NONE.",
                    () -> conn.setTransactionIsolation(Connection.TRANSACTION_NONE)
            );

            // Does not change anything
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
        }
    }

    @Test
    public void holdability() throws SQLException {
        try (Connection conn = createConnection()) {
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

            conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

            String error = "Invalid result set holdability (only close cursors at commit option is supported).";
            assertThrowsSqlException(SQLException.class, error, () -> conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
            assertThrowsSqlException(SQLException.class, error, () -> conn.setHoldability(1234));

            // Does not change anything
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        }
    }

    @Test
    public void warnings() throws SQLException {
        try (Connection conn = createConnection()) {
            // Do nothing
            assertNull(conn.getWarnings());
            conn.clearWarnings();
        }
    }

    @Test
    public void valid() throws SQLException {
        try (Connection conn = createConnection()) {
            assertTrue(conn.isValid(0));
            assertTrue(conn.isValid(1));
            assertThrowsSqlException(SQLException.class, "Invalid timeout: -1", () -> conn.isValid(-1));

            conn.close();

            assertFalse(conn.isValid(0));
            assertFalse(conn.isValid(1));
            assertThrowsSqlException(SQLException.class, "Invalid timeout: -1", () -> conn.isValid(-1));
        }
    }

    @Test
    public void clientInfo() throws SQLException {
        try (Connection conn = createConnection()) {
            conn.setClientInfo("A", "B");
            assertNull(conn.getClientInfo("A"));

            conn.setClientInfo(new Properties());

            Properties props = conn.getClientInfo();
            assertNotNull(props);
            assertTrue(props.isEmpty());
        }
    }

    @Test
    public void schema() throws SQLException {
        try (Connection conn = createConnection()) {
            // Default schema
            assertEquals("PUBLIC", conn.getSchema());

            conn.setSchema("abc");
            assertEquals("abc", conn.getSchema());

            conn.setSchema("\"Abc\"");
            assertEquals("\"Abc\"", conn.getSchema());

            // Empty value resets to default
            conn.setSchema("");
            assertEquals("PUBLIC", conn.getSchema());

            conn.setSchema("S");
            assertEquals("S", conn.getSchema());

            conn.setSchema(null);
            assertEquals("PUBLIC", conn.getSchema());
        }

        try (Connection conn = createConnection((props) -> {
            props.setSchema("Abc");
        }, URL)) {
            assertEquals("Abc", conn.getSchema());
        }

        try (Connection conn = createConnection((props) -> {
            props.setSchema("\"Abc\"");
        }, URL)) {
            assertEquals("\"Abc\"", conn.getSchema());
        }
    }

    @Test
    public void metadata() throws SQLException {
        try (Connection conn = createConnection()) {
            assertNotNull(conn.getMetaData());
        }
    }

    @Test
    public void wrap() throws SQLException {
        try (Connection conn = createConnection()) {
            assertTrue(conn.isWrapperFor(JdbcConnection.class));
            assertSame(conn, conn.unwrap(JdbcConnection.class));

            assertTrue(conn.isWrapperFor(Connection.class));
            assertSame(conn, conn.unwrap(Connection.class));

            assertFalse(conn.isWrapperFor(Statement.class));
            assertThrowsSqlException(SQLException.class, "Connection is not a wrapper for ", () -> conn.unwrap(Statement.class));
        }
    }

    @Test
    void testQueryTimeoutProperty() throws SQLException {
        String propName = "queryTimeoutSeconds";
        String urlPrefix = URL + "?" + propName;

        SqlThrowingFunction<String, Number> valueGetter = url -> {
            try (Connection conn = createConnection(url)) {
                try (Statement stmt = conn.createStatement();
                        PreparedStatement pstmt = conn.prepareStatement("SELECT 1")) {

                    assertThat(stmt.getQueryTimeout(), Matchers.is(pstmt.getQueryTimeout()));

                    return stmt.getQueryTimeout();
                }
            }
        };

        assertThat(valueGetter.apply(URL), Matchers.is(0));
        assertThat(valueGetter.apply(urlPrefix + "=2147483647"), Matchers.is(Integer.MAX_VALUE));
        assertThat(valueGetter.apply(urlPrefix + "=0"), Matchers.is(0));

        expectConnectionException(urlPrefix + "=A",
                format("Failed to parse int property [name={}, value=A]", propName));

        expectConnectionException(urlPrefix + "=-1",
                format("Property cannot be lower than 0 [name={}, value=-1]", propName));

        expectConnectionException(urlPrefix + "=2147483648",
                format("Failed to parse int property [name={}, value=2147483648]", propName));

        // Check deprecated name "connectionTimeout"
        assertThat(valueGetter.apply(URL + "?queryTimeout=100"), is(100));

        // If both names are specified, the deprecated property name should be ignored
        assertThat(valueGetter.apply(URL + "?queryTimeoutSeconds=50&queryTimeout=100"), is(50));
    }

    @Test
    void testConnectionTimeoutProperty() throws SQLException {
        String propName = "connectionTimeoutMillis";
        String urlPrefix = URL + "?" + propName;

        SqlThrowingFunction<String, Number> valueGetter = url -> {
            try (JdbcConnection conn = (JdbcConnection) createConnection(url)) {
                return conn.properties().getConnectionTimeout();
            }
        };

        assertThat(valueGetter.apply(URL), Matchers.is(0));
        assertThat(valueGetter.apply(urlPrefix + "=2147483647"), Matchers.is(Integer.MAX_VALUE));
        assertThat(valueGetter.apply(urlPrefix + "=0"), Matchers.is(0));

        expectConnectionException(urlPrefix + "=A",
                format("Failed to parse int property [name={}, value=A]", propName));

        expectConnectionException(urlPrefix + "=-1",
                format("Property cannot be lower than 0 [name={}, value=-1]", propName));

        expectConnectionException(urlPrefix + "=2147483648",
                format("Failed to parse int property [name={}, value=2147483648]", propName));

        // Check deprecated name "connectionTimeout"
        assertThat(valueGetter.apply(URL + "?connectionTimeout=100"), is(100));

        // If both names are specified, the deprecated property name should be ignored
        assertThat(valueGetter.apply(URL + "?connectionTimeoutMillis=50&connectionTimeout=100"), is(50));
    }

    @Test
    public void testChangeBackgroundReconnectIntervalProperty() throws SQLException {
        String propertyName = "backgroundReconnectIntervalMillis";
        String urlPrefix = URL + "?" + propertyName;

        SqlThrowingFunction<String, Number> valueGetter = url -> {
            try (JdbcConnection conn = (JdbcConnection) createConnection(url)) {
                return conn.properties().getBackgroundReconnectInterval();
            }
        };

        assertThat(valueGetter.apply(URL), is(IgniteClientConfiguration.DFLT_BACKGROUND_RECONNECT_INTERVAL));
        assertThat(valueGetter.apply(urlPrefix + "=9223372036854775807"), is(Long.MAX_VALUE));
        assertThat(valueGetter.apply(urlPrefix + "=0"), is(0L));

        expectConnectionException(urlPrefix + "=A",
                format("Failed to parse int property [name={}, value=A]", propertyName));

        expectConnectionException(urlPrefix + "=-1",
                format("Property cannot be lower than 0 [name={}, value=-1]", propertyName));

        expectConnectionException(urlPrefix + "=9223372036854775808",
                format("Failed to parse int property [name={}, value=9223372036854775808]", propertyName));
    }

    @Test
    public void testChangePartitionAwarenessCacheSizeProperty() throws SQLException {
        // Default value.
        try (JdbcConnection conn = (JdbcConnection) createConnection(URL)) {
            assertEquals(
                    IgniteClientConfiguration.DFLT_SQL_PARTITION_AWARENESS_METADATA_CACHE_SIZE,
                    conn.properties().getPartitionAwarenessMetadataCacheSize()
            );
        }

        String urlPrefix = URL + "?partitionAwarenessMetadataCacheSize";

        expectConnectionException(urlPrefix + "=A",
                "Failed to parse int property [name=partitionAwarenessMetadataCacheSize, value=A]");

        expectConnectionException(urlPrefix + "=-1",
                "Property cannot be lower than 0 [name=partitionAwarenessMetadataCacheSize, value=-1]");

        expectConnectionException(urlPrefix + "=2147483648",
                "Failed to parse int property [name=partitionAwarenessMetadataCacheSize, value=2147483648]");

        try (JdbcConnection conn = (JdbcConnection) createConnection(urlPrefix + "=2147483647")) {
            assertEquals(Integer.MAX_VALUE, conn.properties().getPartitionAwarenessMetadataCacheSize());
        }

        try (JdbcConnection conn = (JdbcConnection) createConnection(urlPrefix + "=0")) {
            assertEquals(0, conn.properties().getPartitionAwarenessMetadataCacheSize());
        }
    }

    private static Connection createConnection() throws SQLException {
        return createConnection((props) -> {}, URL);
    }

    private static Connection createConnection(String url) throws SQLException {
        return createConnection((props) -> {}, url);
    }

    private static Connection createConnection(Consumer<ConnectionProperties> setup, String url) throws SQLException {
        IgniteClient ignite = Mockito.mock(IgniteClient.class);
        IgniteSql igniteSql = Mockito.mock(IgniteSql.class);

        when(ignite.sql()).thenReturn(igniteSql);

        ConnectionPropertiesImpl properties = new ConnectionPropertiesImpl();
        properties.init(url, new Properties());

        setup.accept(properties);

        JdbcDatabaseMetadataHandler eventHandler = Mockito.mock(JdbcDatabaseMetadataHandler.class);

        return new JdbcConnection(ignite, eventHandler, properties);
    }

    private static void expectClosed(Executable method) {
        assertThrowsSqlException(SQLException.class, "Connection is closed.", method);
    }

    private static void expectNotSupported(Executable method) {
        assertThrows(SQLFeatureNotSupportedException.class, method);
    }

    @SuppressWarnings({"ThrowableNotThrown", "resource"})
    private static void expectConnectionException(String url, String errMsg) {
        assertThrowsSqlException(errMsg, () -> createConnection(url));
    }

    /**
     * Function that can throw an {@link SQLException}.
     */
    @FunctionalInterface
    public interface SqlThrowingFunction<T, R> {
        /**
         * Applies the function to a given argument.
         *
         * @param t Argument.
         * @return Application result.
         * @throws SQLException If something goes wrong.
         */
        R apply(T t) throws SQLException;
    }
}
