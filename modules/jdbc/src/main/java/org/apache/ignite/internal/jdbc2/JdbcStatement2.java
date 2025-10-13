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
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.sql.QueryModifier;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.internal.sql.SyncResultSetAdapter;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

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

    private final IgniteSql igniteSql;

    private final Connection connection;

    private final String schemaName;

    private final int rsHoldability;

    private volatile @Nullable JdbcResultSet resultSet;

    private int queryTimeoutSeconds;

    private int pageSize;

    private int maxRows = 0;

    private volatile boolean closed;

    private boolean closeOnCompletion;

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

        ResultSet rs = getResultSet();

        if (rs == null) {
            throw new SQLException("The query isn't SELECT query: " + sql, SqlStateCode.PARSING_EXCEPTION);
        }

        return rs;
    }

    JdbcResultSet createResultSet(org.apache.ignite.sql.ResultSet<SqlRow> resultSet) throws SQLException {
        JdbcConnection2 connection2 = connection.unwrap(JdbcConnection2.class);
        ZoneId zoneId = connection2.properties().getConnectionTimeZone();
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

        // TODO https://issues.apache.org/jira/browse/IGNITE-26139 Implement autocommit = false
        if (!connection.getAutoCommit()) {
            throw new UnsupportedOperationException("Explicit transactions are not supported yet.");
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-26142 multistatement.
        if (sql.indexOf(';') == -1 || sql.indexOf(';') == sql.length() - 1) {
            queryModifiers.remove(QueryModifier.ALLOW_MULTISTATEMENT);
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-26142 multistatement.
        if (queryModifiers.contains(QueryModifier.ALLOW_MULTISTATEMENT)) {
            throw new UnsupportedOperationException("Multi-statements are not supported yet.");
        }

        StatementBuilder stmtBuilder = igniteSql.statementBuilder()
                .query(sql)
                .defaultSchema(schemaName);

        if (queryTimeoutSeconds > 0) {
            stmtBuilder.queryTimeout(queryTimeoutSeconds, TimeUnit.SECONDS);
        }

        if (getFetchSize() > 0) {
            stmtBuilder.pageSize(getFetchSize());
        }

        JdbcConnection2 conn = connection.unwrap(JdbcConnection2.class);
        ZoneId zoneId = conn.properties().getConnectionTimeZone();

        org.apache.ignite.sql.Statement igniteStmt = stmtBuilder
                .timeZoneId(zoneId)
                .build();

        ClientSql clientSql = (ClientSql) igniteSql;

        AsyncResultSet<SqlRow> clientRs;
        try {
            clientRs = clientSql.executeAsyncInternal(null,
                    (Mapper<SqlRow>) null,
                    null,
                    queryModifiers,
                    igniteStmt,
                    args
            ).join();

            SyncResultSetAdapter<SqlRow> syncRs = new SyncResultSetAdapter<>(clientRs);

            resultSet = createResultSet(syncRs);
        } catch (Exception e) {
            Throwable cause = IgniteExceptionMapperUtil.mapToPublicException(e);
            throw new SQLException(cause.getMessage(), cause);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        Objects.requireNonNull(sql, "sql");

        execute0(DML_OR_DDL, sql, ArrayUtils.OBJECT_EMPTY_ARRAY);

        int rowCount = getUpdateCount();

        if (isQuery()) {
            closeResults();
            throw new SQLException("The query is not DML statement: " + sql);
        }

        return Math.max(rowCount, 0);
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

        return queryTimeoutSeconds;
    }

    /** {@inheritDoc} */
    @Override
    public void setQueryTimeout(int timeout) throws SQLException {
        ensureNotClosed();

        if (timeout < 0) {
            throw new SQLException("Invalid timeout value.");
        }

        this.queryTimeoutSeconds = timeout;
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() throws SQLException {
        ensureNotClosed();

        throw new UnsupportedOperationException();
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

        return isQuery();
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

        return isQuery() ? resultSet : null;
    }

    /** {@inheritDoc} */
    @Override
    public int getUpdateCount() throws SQLException {
        ensureNotClosed();

        JdbcResultSet rs = resultSet;
        if (rs == null || rs.isQuery()) {
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

                JdbcResultSet currentRs = resultSet;
                if (currentRs == null) {
                    return false;
                }

                resultSet = null;
                currentRs.close();
                return false;

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

        JdbcResultSet rs = resultSet;
        if (rs != null) {
            rs.closeStatement(true);
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

    protected boolean isQuery() {
        // This method is called after statement is executed, so the reference points to a correct result set.
        // The statement is not expected to be used from multiple threads, so this reference points to a correct result set.
        // getResultSet() performs its own result set checks.
        JdbcResultSet rs = resultSet;
        assert rs != null;

        return rs.isQuery();
    }

    void ensureNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException(STATEMENT_IS_CLOSED);
        }
    }

    private void closeResults() throws SQLException {
        JdbcResultSet rs = resultSet;
        if (rs != null) {
            rs.close();
        }
        resultSet = null;
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
