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

import static org.apache.ignite.internal.util.ArrayUtils.INT_EMPTY_ARRAY;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode;
import org.apache.ignite.internal.jdbc.proto.JdbcStatementType;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchExecuteResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcBatchPreparedStmntRequest;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Jdbc prepared statement implementation.
 */
public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

    /** Supported JDBC types. */
    private static final Set<JDBCType> SUPPORTED_TYPES = EnumSet.of(
            JDBCType.BOOLEAN,
            JDBCType.TINYINT,
            JDBCType.SMALLINT,
            JDBCType.INTEGER,
            JDBCType.BIGINT,
            JDBCType.FLOAT,
            JDBCType.REAL,
            JDBCType.DOUBLE,
            JDBCType.DECIMAL,
            JDBCType.DATE,
            JDBCType.TIME,
            JDBCType.TIMESTAMP,
            JDBCType.CHAR,
            JDBCType.VARCHAR,
            JDBCType.BINARY,
            JDBCType.VARBINARY,
            JDBCType.NULL,
            JDBCType.OTHER // UUID.
    );

    /** SQL query. */
    private final String sql;

    /** Query arguments. */
    private List<Object> currentArgs;

    /** Batched query arguments. */
    private List<Object[]> batchedArgs;

    /**
     * Creates new prepared statement.
     *
     * @param conn           Connection.
     * @param sql            SQL query.
     * @param resHoldability Result set holdability.
     * @param schema         Schema name.
     */
    JdbcPreparedStatement(JdbcConnection conn, String sql, int resHoldability, String schema) {
        super(conn, resHoldability, schema);

        this.sql = sql;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet executeQuery() throws SQLException {
        executeWithArguments(JdbcStatementType.SELECT_STATEMENT_TYPE, false);

        ResultSet rs = getResultSet();

        if (rs == null) {
            throw new SQLException("The query isn't SELECT query: " + sql, SqlStateCode.PARSING_EXCEPTION);
        }

        return rs;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("The method 'executeQuery(String)' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch() throws SQLException {
        ensureNotClosed();

        closeResults();

        if (CollectionUtils.nullOrEmpty(batchedArgs)) {
            return INT_EMPTY_ARRAY;
        }

        JdbcBatchPreparedStmntRequest req
                = new JdbcBatchPreparedStmntRequest(conn.getSchema(), sql, batchedArgs, conn.getAutoCommit());

        try {
            JdbcBatchExecuteResult res = conn.handler().batchPrepStatementAsync(conn.connectionId(), req).get();

            if (!res.success()) {
                throw new BatchUpdateException(res.err(),
                        IgniteQueryErrorCode.codeToSqlState(res.getErrorCode()),
                        res.getErrorCode(),
                        res.updateCounts());
            }

            return res.updateCounts();
        } catch (InterruptedException e) {
            throw new SQLException("Thread was interrupted.", e);
        } catch (ExecutionException e) {
            throw new SQLException("Batch request failed.", e);
        } catch (CancellationException e) {
            throw new SQLException("Batch request canceled.", SqlStateCode.QUERY_CANCELLED);
        } finally {
            batchedArgs = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate() throws SQLException {
        executeWithArguments(JdbcStatementType.UPDATE_STATEMENT_TYPE, false);

        int res = getUpdateCount();

        if (res == -1) {
            throw new SQLException("The query is not DML statement: " + sql, SqlStateCode.PARSING_EXCEPTION);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("The method 'executeUpdate(String)' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("The method 'executeUpdate(String, int)' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql, String[] colNames) throws SQLException {
        throw new SQLException("The method 'executeUpdate(String, String[])' is called on PreparedStatement "
                + "instance.", SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate(String sql, int[] colNames) throws SQLException {
        throw new SQLException("The method 'executeUpdate(String, int[])' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute() throws SQLException {
        executeWithArguments(JdbcStatementType.ANY_STATEMENT_TYPE, true);

        return isQuery();
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("The method 'execute(String)' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("The method 'executeUpdate(String, int)' is called on PreparedStatement "
                + "instance.", SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute(String sql, int[] colNames) throws SQLException {
        throw new SQLException("The method 'execute(String, int[])' is called on PreparedStatement "
                + "instance.", SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute(String sql, String[] colNames) throws SQLException {
        throw new SQLException("The method 'execute(String, String[]) is called on PreparedStatement "
                + "instance.", SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public void addBatch() throws SQLException {
        ensureNotClosed();

        if (batchedArgs == null) {
            batchedArgs = new ArrayList<>();
        }

        batchedArgs.add(currentArgs.stream().map(this::convertJdbcTypeToInternal).toArray());

        currentArgs = null;
    }

    /** {@inheritDoc} */
    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLException("The method 'addBatch(String)' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override
    public void clearBatch() throws SQLException {
        ensureNotClosed();

        batchedArgs = null;
    }

    /** {@inheritDoc} */
    @Override
    public void setNull(int paramIdx, int sqlType) throws SQLException {
        checkType(sqlType);

        setArgument(paramIdx, null);
    }

    /** {@inheritDoc} */
    @Override
    public void setNull(int paramIdx, int sqlType, String typeName) throws SQLException {
        checkType(sqlType);

        setArgument(paramIdx, null);
    }

    /** {@inheritDoc} */
    @Override
    public void setBoolean(int paramIdx, boolean x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setByte(int paramIdx, byte x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setShort(int paramIdx, short x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setInt(int paramIdx, int x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setLong(int paramIdx, long x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setFloat(int paramIdx, float x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setDouble(int paramIdx, double x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setBigDecimal(int paramIdx, BigDecimal x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setString(int paramIdx, String x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setBytes(int paramIdx, byte[] x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setDate(int paramIdx, Date x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setDate(int paramIdx, Date x, Calendar cal) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setTime(int paramIdx, Time x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setTime(int paramIdx, Time x, Calendar cal) throws SQLException {
        setArgument(paramIdx, x);
    }


    /** {@inheritDoc} */
    @Override
    public void setTimestamp(int paramIdx, Timestamp x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setTimestamp(int paramIdx, Timestamp x, Calendar cal) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setAsciiStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setAsciiStream(int paramIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setAsciiStream(int paramIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setUnicodeStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setBinaryStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setBinaryStream(int paramIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setBinaryStream(int paramIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void clearParameters() throws SQLException {
        ensureNotClosed();

        currentArgs = null;
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(int paramIdx, Object x, int targetSqlType) throws SQLException {
        checkType(targetSqlType);

        throw new SQLFeatureNotSupportedException("Conversion to target sql type is not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(int paramIdx, Object x) throws SQLException {
        if (x == null) {
            setNull(paramIdx, Types.OTHER);
        } else if (x instanceof Boolean) {
            setBoolean(paramIdx, (Boolean) x);
        } else if (x instanceof Byte) {
            setByte(paramIdx, (Byte) x);
        } else if (x instanceof Short) {
            setShort(paramIdx, (Short) x);
        } else if (x instanceof Integer) {
            setInt(paramIdx, (Integer) x);
        } else if (x instanceof Long) {
            setLong(paramIdx, (Long) x);
        } else if (x instanceof Float) {
            setFloat(paramIdx, (Float) x);
        } else if (x instanceof Double) {
            setDouble(paramIdx, (Double) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(paramIdx, (BigDecimal) x);
        } else if (x instanceof String) {
            setString(paramIdx, (String) x);
        } else if (x instanceof byte[]) {
            setBytes(paramIdx, (byte[]) x);
        } else if (x instanceof Date) {
            setDate(paramIdx, (Date) x);
        } else if (x instanceof Time) {
            setTime(paramIdx, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(paramIdx, (Timestamp) x);
        } else if (x instanceof UUID) {
            setArgument(paramIdx, x);
        } else {
            throw new SQLFeatureNotSupportedException("Parameter is not supported: " + x + " <" + x.getClass().getTypeName() + ">");
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(int paramIdx, Object x, int targetSqlType, int scaleOrLen) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Conversion to target sql type is not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setCharacterStream(int paramIdx, Reader x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setCharacterStream(int paramIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setCharacterStream(int paramIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setRef(int paramIdx, Ref x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setBlob(int paramIdx, Blob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setBlob(int paramIdx, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setBlob(int paramIdx, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setClob(int paramIdx, Clob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setClob(int paramIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setClob(int paramIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setArray(int paramIdx, Array x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Meta data for prepared statement is not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setURL(int paramIdx, URL x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Parameter type is unsupported. [cls=" + URL.class + ']');
    }

    /** {@inheritDoc} */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Parameter metadata are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setRowId(int paramIdx, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setNString(int paramIdx, String val) throws SQLException {
        ensureNotClosed();

        setString(paramIdx, val);
    }

    /** {@inheritDoc} */
    @Override
    public void setNCharacterStream(int paramIdx, Reader val, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setNCharacterStream(int paramIdx, Reader val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setNClob(int paramIdx, NClob val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setNClob(int paramIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setNClob(int paramIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setSQLXML(int paramIdx, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(Objects.requireNonNull(iface))) {
            throw new SQLException("Prepared statement is not a wrapper for " + iface.getName());
        }

        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcPreparedStatement.class);
    }

    /**
     * Sets query argument value.
     *
     * @param paramIdx Index.
     * @param val Value.
     * @throws SQLException If index is invalid.
     */
    private void setArgument(int paramIdx, Object val) throws SQLException {
        ensureNotClosed();

        if (paramIdx < 1) {
            throw new SQLException("Parameter index is invalid: " + paramIdx);
        }

        if (currentArgs == null) {
            currentArgs = new ArrayList<>(paramIdx);
        }

        while (currentArgs.size() < paramIdx) {
            currentArgs.add(null);
        }

        currentArgs.set(paramIdx - 1, val);
    }

    /**
     * Converts value of JDBC type to value of Ignite Client protocol types.
     *
     * <ul>
     *     <li>java.sql.* to Java Time API</li>
     * </ul>
     */
    private @Nullable Object convertJdbcTypeToInternal(@Nullable Object val) {
        if (val instanceof java.util.Date) {
            if (val instanceof Timestamp) {
                Timestamp timeStamp = (Timestamp) val;
                return timeStamp.toLocalDateTime();
            } else if (val instanceof Date) {
                Date date = (Date) val;
                return date.toLocalDate();
            } else if (val instanceof Time) {
                Time time = (Time) val;
                return time.toLocalTime();
            }

            return ((java.util.Date) val).toInstant();
        }

        return val;
    }

    List<Object> getArguments() {
        return currentArgs;
    }

    /**
     * Execute query with arguments and nullify them afterwards.
     *
     * @param statementType Expected statement type.
     * @throws SQLException If failed.
     */
    private void executeWithArguments(JdbcStatementType statementType, boolean multiStatement) throws SQLException {
        Object[] args = currentArgs == null ? ArrayUtils.OBJECT_EMPTY_ARRAY :
                currentArgs.stream().map(this::convertJdbcTypeToInternal).toArray();

        execute0(statementType, sql, multiStatement, args);
    }

    private static void checkType(int sqlType) throws SQLException {
        JDBCType jdbcType = JDBCType.valueOf(sqlType);
        if (!SUPPORTED_TYPES.contains(jdbcType)) {
            throw new SQLFeatureNotSupportedException("Type is not supported: " + sqlType);
        }
    }
}
