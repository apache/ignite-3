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

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.client.sql.ClientAsyncResultSet;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.sql.QueryModifier;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * {@link Statement} implementation backed by the thin client.
 */
public class JdbcStatement2 implements Statement {

    static final EnumSet<QueryModifier> QUERY = EnumSet.of(QueryModifier.ALLOW_ROW_SET_RESULT);

    static final EnumSet<QueryModifier> DML_OR_DDL = EnumSet.of(
            QueryModifier.ALLOW_AFFECTED_ROWS_RESULT, QueryModifier.ALLOW_APPLIED_RESULT);

    static final Set<QueryModifier> ALL = QueryModifier.ALL;

    private static final String RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED =
            JdbcConnection2.RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED;

    private static final String LARGE_UPDATE_NOT_SUPPORTED =
            "executeLargeUpdate not implemented.";

    private static final String FIELD_SIZE_LIMIT_IS_NOT_SUPPORTED =
            "Field size limit is not supported.";

    private static final String CURSOR_NAME_IS_NOT_SUPPORTED =
            "Setting cursor name is not supported.";

    private static final String MULTIPLE_OPEN_RESULTS_ARE_NOT_SUPPORTED =
            "Multiple open results are not supported.";

    private static final String POOLING_IS_NOT_SUPPORTED =
            "Pooling is not supported.";

    private static final String STATEMENT_IS_CLOSED =
            "Statement is closed.";

    private static final String ONLY_FORWARD_DIRECTION_IS_SUPPORTED =
            "Only forward direction is supported.";

    final Connection connection;

    final IgniteSql igniteSql;

    private final String schemaName;

    private final int rsHoldability;

    protected volatile @Nullable ResultSetWrapper result;

    private long queryTimeoutMillis;

    private int pageSize;

    private int maxRows = 0;

    private volatile boolean closed;

    boolean closeOnCompletion;

    volatile @Nullable CancelHandle cancelHandle;

    JdbcStatement2(
            Connection connection,
            IgniteSql igniteSql,
            String schemaName,
            int rsHoldability
    ) {
        this.connection = connection;
        this.schemaName = schemaName;
        this.igniteSql = igniteSql;
        this.rsHoldability = rsHoldability;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        execute0(QUERY, Objects.requireNonNull(sql), ArrayUtils.OBJECT_EMPTY_ARRAY);

        ResultSetWrapper currentRs = result;
        assert currentRs != null;

        ResultSet rs = currentRs.current();

        if (rs == null) {
            throw new SQLException("The query isn't SELECT query: " + sql, SqlStateCode.PARSING_EXCEPTION);
        }

        return rs;
    }

    JdbcResultSet createResultSet(ClientSyncResultSet resultSet) throws SQLException {
        JdbcConnection2 jdbcConnection = connection.unwrap(JdbcConnection2.class);
        ZoneId zoneId = jdbcConnection.properties().getConnectionTimeZone();
        return new JdbcResultSet(resultSet, this, () -> zoneId, closeOnCompletion, maxRows);
    }

    /**
     * Execute the query with given parameters.
     *
     * @param sql Sql query.
     * @param args Query parameters.
     * @throws SQLException Onj error.
     */
    void execute0(Set<QueryModifier> queryModifiers, String sql, Object[] args) throws SQLException {
        ensureNotClosed();

        closeResults();

        if (sql == null || sql.isEmpty()) {
            throw new SQLException("SQL query is empty.");
        }

        JdbcConnection2 connection2 = connection.unwrap(JdbcConnection2.class);
        Transaction tx = connection2.startTransactionIfNoAutoCommit();

        org.apache.ignite.sql.Statement igniteStmt = createIgniteStatement(sql);
        ClientSql clientSql = (ClientSql) igniteSql;

        // Cancel handle is not reusable, we should create a new one for each execution.
        CancelHandle handle = CancelHandle.create();
        cancelHandle = handle;

        ClientAsyncResultSet<SqlRow> clientRs;
        try {
            clientRs = (ClientAsyncResultSet<SqlRow>) clientSql.executeAsyncInternal(tx,
                    (Mapper<SqlRow>) null,
                    handle.token(),
                    queryModifiers,
                    igniteStmt,
                    args
            ).join();

            result = new ResultSetWrapper(createResultSet(new ClientSyncResultSetImpl(clientRs)));
        } catch (Exception e) {
            throw JdbcExceptionMapperUtil.mapToJdbcException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        Objects.requireNonNull(sql, "sql");

        execute0(DML_OR_DDL, sql, ArrayUtils.OBJECT_EMPTY_ARRAY);

        ResultSetWrapper rs = result;
        assert rs != null;

        if (rs.isQuery()) {
            closeResults();
            throw new SQLException("The query is not DML statement: " + sql);
        }

        return rs.updateCount();
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        switch (autoGeneratedKeys) {
            case RETURN_GENERATED_KEYS:
                throw new SQLFeatureNotSupportedException((RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED));

            case NO_GENERATED_KEYS:
                return executeUpdate(sql);

            default:
                throw new SQLException("Invalid autoGeneratedKeys value");
        }
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        closed = true;

        closeResults();
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxFieldSize() throws SQLException {
        ensureNotClosed();

        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        ensureNotClosed();

        if (max < 0) {
            throw new SQLException("Invalid field limit.");
        }

        throw new SQLFeatureNotSupportedException(FIELD_SIZE_LIMIT_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxRows() throws SQLException {
        ensureNotClosed();

        return maxRows;
    }

    /** {@inheritDoc} */
    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        ensureNotClosed();

        if (maxRows < 0) {
            throw new SQLException("Invalid max rows value.");
        }

        this.maxRows = maxRows;
    }

    /** {@inheritDoc} */
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override
    public int getQueryTimeout() throws SQLException {
        ensureNotClosed();

        return (int) TimeUnit.MILLISECONDS.toSeconds(queryTimeoutMillis);
    }

    /** {@inheritDoc} */
    @Override
    public void setQueryTimeout(int timeout) throws SQLException {
        ensureNotClosed();

        if (timeout < 0) {
            throw new SQLException("Invalid timeout value.");
        }

        this.queryTimeoutMillis = TimeUnit.SECONDS.toMillis(timeout);
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() throws SQLException {
        ensureNotClosed();

        CancelHandle handle = cancelHandle;
        if (handle != null) {
            handle.cancel();
        }
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
    public void setCursorName(String name) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(CURSOR_NAME_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute(String sql) throws SQLException {
        ensureNotClosed();

        execute0(QueryModifier.ALL, Objects.requireNonNull(sql), ArrayUtils.OBJECT_EMPTY_ARRAY);

        ResultSetWrapper rs = result;
        assert rs != null;

        return rs.isQuery();
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        if (colIndexes != null && colIndexes.length > 0) {
            throw new SQLFeatureNotSupportedException(RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED);
        }

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        switch (autoGeneratedKeys) {
            case RETURN_GENERATED_KEYS:
                throw new SQLFeatureNotSupportedException(RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED);

            case NO_GENERATED_KEYS:
                return execute(sql);

            default:
                throw new SQLException("Invalid autoGeneratedKeys value.");
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        if (colNames != null && colNames.length > 0) {
            throw new SQLFeatureNotSupportedException(RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED);
        }

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ResultSet getResultSet() throws SQLException {
        ensureNotClosed();

        ResultSetWrapper rs = result;
        if (rs != null && rs.isQuery()) {
            return rs.current();
        } else {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getUpdateCount() throws SQLException {
        ensureNotClosed();

        ResultSetWrapper rs = result;
        if (rs == null) {
            return -1;
        } else {
            return rs.updateCount();
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    /** {@inheritDoc} */
    @Override
    public boolean getMoreResults(int current) throws SQLException {
        ensureNotClosed();

        switch (current) {
            case CLOSE_CURRENT_RESULT:
                ResultSetWrapper currentResult = result;
                if (currentResult == null) {
                    return false;
                }

                boolean moreResults = currentResult.nextResultSet();
                if (!moreResults) {
                    // next() closes the remaining result set if necessary
                    result = null;

                    return false;
                } else {
                    return currentResult.isQuery();
                }

            case CLOSE_ALL_RESULTS:
            case KEEP_CURRENT_RESULT:
                throw new SQLFeatureNotSupportedException(MULTIPLE_OPEN_RESULTS_ARE_NOT_SUPPORTED);

            default:
                throw new SQLException("Invalid 'current' parameter.");
        }
    }

    /** {@inheritDoc} */
    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(LARGE_UPDATE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(LARGE_UPDATE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(LARGE_UPDATE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureNotClosed();

        if (direction != FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException(ONLY_FORWARD_DIRECTION_IS_SUPPORTED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getFetchDirection() throws SQLException {
        ensureNotClosed();

        return FETCH_FORWARD;
    }

    /** {@inheritDoc} */
    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        ensureNotClosed();

        if (fetchSize <= 0) {
            throw new SQLException("Invalid fetch size.");
        }

        pageSize = fetchSize;
    }

    /** {@inheritDoc} */
    @Override
    public int getFetchSize() throws SQLException {
        ensureNotClosed();

        return pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public int getResultSetConcurrency() throws SQLException {
        ensureNotClosed();

        return CONCUR_READ_ONLY;
    }

    /** {@inheritDoc} */
    @Override
    public int getResultSetType() throws SQLException {
        ensureNotClosed();

        return TYPE_FORWARD_ONLY;
    }

    /** {@inheritDoc} */
    @Override
    public void addBatch(String sql) throws SQLException {
        ensureNotClosed();

        Objects.requireNonNull(sql);

        // TODO https://issues.apache.org/jira/browse/IGNITE-26143 batch operations
        throw new UnsupportedOperationException("Batch operation");
    }

    /** {@inheritDoc} */
    @Override
    public void clearBatch() throws SQLException {
        ensureNotClosed();

        // TODO https://issues.apache.org/jira/browse/IGNITE-26143 batch operations
        throw new UnsupportedOperationException("Batch operation");
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch() throws SQLException {
        ensureNotClosed();

        closeResults();

        // TODO https://issues.apache.org/jira/browse/IGNITE-26143 batch operations
        throw new UnsupportedOperationException("Batch operation");
    }

    /** {@inheritDoc} */
    @Override
    public Connection getConnection() throws SQLException {
        ensureNotClosed();

        return connection;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException((RETURNING_AUTO_GENERATED_KEYS_IS_NOT_SUPPORTED));
    }

    /** {@inheritDoc} */
    @Override
    public int getResultSetHoldability() throws SQLException {
        ensureNotClosed();

        return rsHoldability;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    /** {@inheritDoc} */
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ensureNotClosed();

        if (poolable) {
            throw new SQLFeatureNotSupportedException(POOLING_IS_NOT_SUPPORTED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isPoolable() throws SQLException {
        ensureNotClosed();

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public void closeOnCompletion() throws SQLException {
        ensureNotClosed();

        closeOnCompletion = true;

        ResultSetWrapper rs = result;
        if (rs != null) {
            rs.setCloseStatement(true);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        ensureNotClosed();

        return closeOnCompletion;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(Objects.requireNonNull(iface))) {
            throw new SQLException("Statement is not a wrapper for " + iface.getName());
        }

        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcStatement2.class);
    }

    /** Sets timeout in milliseconds. */
    @TestOnly
    public void timeout(long timeoutMillis) {
        this.queryTimeoutMillis = timeoutMillis;
    }

    org.apache.ignite.sql.Statement createIgniteStatement(String sql) throws SQLException {
        StatementBuilder builder = igniteSql.statementBuilder()
                .query(sql)
                .defaultSchema(schemaName);

        if (queryTimeoutMillis > 0) {
            builder.queryTimeout(queryTimeoutMillis, TimeUnit.MILLISECONDS);
        }

        if (getFetchSize() > 0) {
            builder.pageSize(getFetchSize());
        }

        JdbcConnection2 conn = connection.unwrap(JdbcConnection2.class);
        ZoneId zoneId = conn.properties().getConnectionTimeZone();

        return builder.timeZoneId(zoneId).build();
    }

    void ensureNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException(STATEMENT_IS_CLOSED);
        }
    }

    private void closeResults() throws SQLException {
        ResultSetWrapper rs = result;
        result = null;
        if (rs != null) {
            rs.close();
        }
    }

    /**
     * Used by statement on closeOnCompletion mode.
     */
    void closeIfAllResultsClosed() throws SQLException {
        if (!closeOnCompletion) {
            return;
        }

        close();
    }
}
