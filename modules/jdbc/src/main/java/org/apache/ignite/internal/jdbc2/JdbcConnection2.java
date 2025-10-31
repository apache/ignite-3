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

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.ignite.internal.jdbc.proto.SqlStateCode.CONNECTION_CLOSED;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.jdbc.ConnectionProperties;
import org.apache.ignite.internal.jdbc.JdbcDatabaseMetadata;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * {@link Connection} implementation backed by the thin client.
 */
public class JdbcConnection2 implements Connection {

    private static final String CONNECTION_IS_CLOSED = "Connection is closed.";

    static final String RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED =
            "Returning auto-generated keys is not supported.";

    private static final String CALLABLE_FUNCTIONS_ARE_NOT_SUPPORTED =
            "Callable functions are not supported.";

    private static final String SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED =
            "SQL-specific types are not supported.";

    private static final String INVALID_RESULT_SET_HOLDABILITY =
            "Invalid result set holdability (only close cursors at commit option is supported).";

    private static final String INVALID_RESULT_SET_TYPE =
            "Invalid result set type (only forward is supported).";

    private static final String INVALID_RESULT_SET_CONCURRENCY =
            "Invalid concurrency (updates are not supported).";

    private static final String TRANSACTION_CANNOT_BE_COMMITED_IN_AUTOCOMMIT_MODE =
            "Transaction cannot be committed explicitly in auto-commit mode.";

    private static final String COMMIT_REQUEST_FAILED
            = "The transaction commit request failed.";

    private static final String TRANSACTION_CANNOT_BE_ROLLED_BACK_IN_AUTOCOMMIT_MODE =
            "Transaction cannot be rolled back explicitly in auto-commit mode.";

    private static final String ROLLBACK_REQUEST_FAILED
            = "The transaction rollback request failed.";

    private static final String CANNOT_SET_TRANSACTION_NONE =
            "Cannot set transaction isolation level to TRANSACTION_NONE.";

    private static final String INVALID_TRANSACTION_ISOLATION_LEVEL =
            "Invalid transaction isolation level.";

    private static final String SHARDING_KEYS_ARE_NOT_SUPPORTED =
            "Sharding keys are not supported.";

    private static final String SAVEPOINT_IN_AUTO_COMMIT_MODE =
            "Savepoint cannot be set in auto-commit mode.";

    private static final String SAVEPOINTS_ARE_NOT_SUPPORTED =
            "Savepoints are not supported.";

    private static final String TYPES_MAPPING_IS_NOT_SUPPORTED =
            "Types mapping is not supported.";

    private final IgniteClient igniteClient;

    private final IgniteSql igniteSql;

    private final ReentrantLock lock = new ReentrantLock();

    private final List<Statement> statements = new ArrayList<>();

    private final ConnectionProperties properties;

    private final JdbcDatabaseMetadata metadata;

    private volatile boolean closed;

    private String schemaName;

    private int txIsolation;

    private boolean autoCommit;

    private boolean readOnly;

    private int networkTimeoutMillis;

    private @Nullable Transaction transaction;

    /**
     * Creates new connection.
     *
     * @param client SQL client.
     * @param eventHandler Event handler.
     * @param props Connection properties.
     */
    public JdbcConnection2(
            IgniteClient client,
            JdbcQueryEventHandler eventHandler,
            ConnectionProperties props
    ) {
        igniteClient = client;
        igniteSql = client.sql();
        autoCommit = true;
        networkTimeoutMillis = props.getConnectionTimeout();
        txIsolation = TRANSACTION_SERIALIZABLE;
        schemaName = readSchemaName(props.getSchema());
        properties = props;

        //noinspection ThisEscapedInObjectConstruction
        metadata = new JdbcDatabaseMetadata(this, eventHandler, props.getUrl(), props.getUsername());
    }

    /** {@inheritDoc} */
    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
    }

    /** {@inheritDoc} */
    @Override
    public Statement createStatement(int resSetType, int resSetConcurrency) throws SQLException {
        return createStatement(resSetType, resSetConcurrency, CLOSE_CURSORS_AT_COMMIT);
    }

    /** {@inheritDoc} */
    @Override
    public Statement createStatement(int resSetType, int resSetConcurrency,
            int resSetHoldability) throws SQLException {
        ensureNotClosed();

        checkCursorOptions(resSetType, resSetConcurrency, resSetHoldability);

        JdbcStatement2 statement = new JdbcStatement2(this, igniteSql, schemaName, resSetHoldability);

        lock.lock();
        try {
            statements.add(statement);
        } finally {
            lock.unlock();
        }

        return statement;
    }

    /** {@inheritDoc} */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT);
    }

    /** {@inheritDoc} */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException(RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED);
        }

        return prepareStatement(sql);
    }

    /** {@inheritDoc} */
    @Override
    public PreparedStatement prepareStatement(String sql, int resSetType,
            int resSetConcurrency) throws SQLException {
        return prepareStatement(sql, resSetType, resSetConcurrency, CLOSE_CURSORS_AT_COMMIT);
    }

    /** {@inheritDoc} */
    @Override
    public PreparedStatement prepareStatement(String sql, int resSetType, int resSetConcurrency,
            int resSetHoldability) throws SQLException {
        ensureNotClosed();

        if (sql == null) {
            throw new SQLException("SQL string cannot be null.");
        }

        checkCursorOptions(resSetType, resSetConcurrency, resSetHoldability);

        return new JdbcPreparedStatement2(this, igniteSql, schemaName, resSetHoldability, sql);
    }

    /** {@inheritDoc} */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        ensureNotClosed();

        Objects.requireNonNull(sql);

        return sql;
    }

    @Nullable Transaction startTransactionIfNoAutoCommit() {
        if (transaction == null && !autoCommit) {
            transaction = igniteClient.transactions().begin(new TransactionOptions().readOnly(false));
            return transaction;
        } else {
            return transaction;
        }
    }

    private void commitTx() throws SQLException {
        Transaction tx = transaction;
        if (tx == null) {
            return;
        }

        // Null out the transaction first.
        transaction = null;

        try {
            tx.commit();
        } catch (Exception e) {
            throw JdbcExceptionMapperUtil.mapToJdbcException(COMMIT_REQUEST_FAILED, e);
        }
    }

    private void rollbackTx() throws SQLException {
        Transaction tx = transaction;
        if (tx == null) {
            return;
        }

        // Null out the transaction first.
        transaction = null;

        try {
            tx.rollback();
        } catch (Exception e) {
            throw JdbcExceptionMapperUtil.mapToJdbcException(ROLLBACK_REQUEST_FAILED, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureNotClosed();

        // If setAutoCommit is called and the auto-commit mode is not changed, the call is a no-op.
        if (this.autoCommit == autoCommit) {
            return;
        }

        boolean wasAutoCommit = this.autoCommit;
        // Autocommit should be changed even if commit fails.
        this.autoCommit = autoCommit;
        // If this method is called during a transaction and the auto-commit mode is changed, the transaction is committed.
        if (!wasAutoCommit && transaction != null) {
            commitTx();
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean getAutoCommit() throws SQLException {
        ensureNotClosed();

        return autoCommit;
    }

    /** {@inheritDoc} */
    @Override
    public void commit() throws SQLException {
        ensureNotClosed();

        if (autoCommit) {
            throw new SQLException(TRANSACTION_CANNOT_BE_COMMITED_IN_AUTOCOMMIT_MODE);
        }

        commitTx();
    }

    /** {@inheritDoc} */
    @Override
    public void rollback() throws SQLException {
        ensureNotClosed();

        if (autoCommit) {
            throw new SQLException(TRANSACTION_CANNOT_BE_ROLLED_BACK_IN_AUTOCOMMIT_MODE);
        }

        rollbackTx();
    }

    /** {@inheritDoc} */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        ensureNotClosed();

        if (savepoint == null) {
            throw new SQLException("Invalid savepoint.");
        }

        if (autoCommit) {
            throw new SQLException(TRANSACTION_CANNOT_BE_COMMITED_IN_AUTOCOMMIT_MODE);
        }

        throw new SQLFeatureNotSupportedException(SAVEPOINTS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        List<Exception> suppressedExceptions = null;

        boolean wasAutoCommit = this.autoCommit;
        // Rollback on close
        if (!wasAutoCommit && transaction != null) {
            rollbackTx();
        }

        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;

            for (Statement statement : statements) {
                try {
                    statement.close();
                } catch (Exception e) {
                    if (suppressedExceptions == null) {
                        suppressedExceptions = new ArrayList<>();
                    }
                    suppressedExceptions.add(e);
                }
            }

        } finally {
            lock.unlock();
        }

        try {
            igniteClient.close();
        } catch (Exception e) {
            throw connectionCloseException(e, suppressedExceptions);
        }

        if (suppressedExceptions != null) {
            throw connectionCloseException(null, suppressedExceptions);
        }
    }

    private static SQLException connectionCloseException(@Nullable Exception e, @Nullable List<Exception> suppressedExceptions) {
        SQLException err = new SQLException("Exception occurred while closing a connection.", e);
        if (suppressedExceptions != null) {
            for (Exception suppressed : suppressedExceptions) {
                err.addSuppressed(suppressed);
            }
        }
        return err;
    }

    /**
     * Ensures that connection is not closed.
     *
     * @throws SQLException If connection is closed.
     */
    private void ensureNotClosed() throws SQLException {
        if (closed) {
            throw new SQLException(CONNECTION_IS_CLOSED, CONNECTION_CLOSED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    /** {@inheritDoc} */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        return metadata;
    }

    /** {@inheritDoc} */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ensureNotClosed();

        this.readOnly = readOnly;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() throws SQLException {
        ensureNotClosed();

        return readOnly;
    }

    /** {@inheritDoc} */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override
    public String getCatalog() throws SQLException {
        ensureNotClosed();

        return JdbcDatabaseMetadata.CATALOG_NAME;
    }

    /** {@inheritDoc} */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ensureNotClosed();

        switch (level) {
            case TRANSACTION_READ_UNCOMMITTED:
            case TRANSACTION_READ_COMMITTED:
            case TRANSACTION_REPEATABLE_READ:
            case TRANSACTION_SERIALIZABLE:
                break;
            case TRANSACTION_NONE:
                throw new SQLException(CANNOT_SET_TRANSACTION_NONE);

            default:
                throw new SQLException(INVALID_TRANSACTION_ISOLATION_LEVEL, SqlStateCode.INVALID_TRANSACTION_LEVEL);
        }

        txIsolation = level;
    }

    /** {@inheritDoc} */
    @Override
    public int getTransactionIsolation() throws SQLException {
        ensureNotClosed();

        return txIsolation;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public SQLWarning getWarnings() throws SQLException {
        ensureNotClosed();

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void clearWarnings() throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(TYPES_MAPPING_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(TYPES_MAPPING_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        ensureNotClosed();

        if (holdability != CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLException(INVALID_RESULT_SET_HOLDABILITY);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getHoldability() throws SQLException {
        ensureNotClosed();

        return CLOSE_CURSORS_AT_COMMIT;
    }

    /** {@inheritDoc} */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        ensureNotClosed();

        if (autoCommit) {
            throw new SQLException(SAVEPOINT_IN_AUTO_COMMIT_MODE);
        }

        throw new SQLFeatureNotSupportedException(SAVEPOINTS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        ensureNotClosed();

        if (name == null) {
            throw new SQLException("Savepoint name cannot be null.");
        }

        if (autoCommit) {
            throw new SQLException(SAVEPOINT_IN_AUTO_COMMIT_MODE);
        }

        throw new SQLFeatureNotSupportedException(SAVEPOINTS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ensureNotClosed();

        if (savepoint == null) {
            throw new SQLException("Savepoint cannot be null.");
        }

        throw new SQLFeatureNotSupportedException(SAVEPOINTS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(CALLABLE_FUNCTIONS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public CallableStatement prepareCall(String sql, int resSetType, int resSetConcurrency)
            throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(CALLABLE_FUNCTIONS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public CallableStatement prepareCall(String sql, int resSetType, int resSetConcurrency,
            int resSetHoldability) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(CALLABLE_FUNCTIONS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Clob createClob() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Blob createBlob() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public NClob createNClob() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Invalid timeout: " + timeout);
        }

        return !closed;
    }

    /** {@inheritDoc} */
    @Override
    public void setClientInfo(String name, String val) throws SQLClientInfoException {
        if (closed) {
            throw new SQLClientInfoException(CONNECTION_IS_CLOSED, null);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setClientInfo(Properties props) throws SQLClientInfoException {
        if (closed) {
            throw new SQLClientInfoException(CONNECTION_IS_CLOSED, null);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String getClientInfo(String name) throws SQLException {
        ensureNotClosed();

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Properties getClientInfo() throws SQLException {
        ensureNotClosed();

        return new Properties();
    }

    /** {@inheritDoc} */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Struct createStruct(String typeName, Object[] attrs) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setSchema(String schema) throws SQLException {
        ensureNotClosed();

        this.schemaName = readSchemaName(schema);
    }

    /** {@inheritDoc} */
    @Override
    public String getSchema() throws SQLException {
        ensureNotClosed();

        return schemaName;
    }

    /** {@inheritDoc} */
    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("Executor cannot be null.");
        }

        close();
    }

    /** {@inheritDoc} */
    @Override
    public final void setNetworkTimeout(Executor executor, int ms) throws SQLException {
        ensureNotClosed();

        if (ms < 0) {
            throw new SQLException("Network timeout cannot be negative.");
        }

        networkTimeoutMillis = ms;
    }

    /** {@inheritDoc} */
    @Override
    public int getNetworkTimeout() throws SQLException {
        ensureNotClosed();

        return networkTimeoutMillis;
    }

    /** {@inheritDoc} */
    @Override
    public void beginRequest() throws SQLException {
        ensureNotClosed();

        // No-op
    }

    /** {@inheritDoc} */
    @Override
    public void endRequest() throws SQLException {
        ensureNotClosed();

        // No-op
    }

    /** {@inheritDoc} */
    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey,
            int timeout) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SHARDING_KEYS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SHARDING_KEYS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SHARDING_KEYS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SHARDING_KEYS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface)) {
            throw new SQLException("Connection is not a wrapper for " + iface.getName());
        }

        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcConnection2.class);
    }

    public ConnectionProperties properties() {
        return properties;
    }

    @TestOnly
    void closeClient() {
        igniteClient.close();
    }

    private static void checkCursorOptions(
            int resSetType,
            int resSetConcurrency,
            int resHoldability
    ) throws SQLFeatureNotSupportedException {

        if (resSetType != TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException(INVALID_RESULT_SET_TYPE);
        }

        if (resSetConcurrency != CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException(INVALID_RESULT_SET_CONCURRENCY);
        }

        if (resHoldability != CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException(
                    INVALID_RESULT_SET_HOLDABILITY);
        }
    }

    private static String readSchemaName(String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            return SqlCommon.DEFAULT_SCHEMA_NAME;
        }
        return schemaName;
    }
}
