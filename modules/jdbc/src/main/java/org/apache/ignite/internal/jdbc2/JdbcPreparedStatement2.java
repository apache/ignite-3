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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
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
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.sql.IgniteSql;
import org.jetbrains.annotations.Nullable;

/**
 * Jdbc prepared statement.
 */
public class JdbcPreparedStatement2 extends JdbcStatement2 implements PreparedStatement {

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

    private static final String SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED =
            "SQL-specific types are not supported.";

    private static final String STREAMS_ARE_NOT_SUPPORTED =
            "Streams are not supported.";

    private static final String CONVERSION_TO_TARGET_SQL_TYPE_IS_NOT_SUPPORTED =
            "Conversion to target sql type is not supported.";

    private static final String PARAMETER_METADATA_IS_NOT_SUPPORTED =
            "Parameter metadata is not supported.";

    private static final String RESULT_SET_METADATA_IS_NOT_SUPPORTED =
            "ResultSet metadata for prepared statement is not supported.";

    private final String sql;

    private List<Object> currentArguments = List.of();

    JdbcPreparedStatement2(
            Connection connection,
            IgniteSql igniteSql,
            String schema,
            int resHoldability,
            String sql
    ) {
        super(connection, igniteSql, schema, resHoldability);

        this.sql = sql;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet executeQuery() throws SQLException {
        execute0(QUERY, sql, currentArguments.toArray());

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

        throw new UnsupportedOperationException("Batch operation");
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate() throws SQLException {
        execute0(DML_OR_DDL, sql, currentArguments.toArray());

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
        execute0(ALL, sql, currentArguments.toArray());

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
        throw new SQLException("The method 'execute(String, int)' is called on PreparedStatement "
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

        throw new UnsupportedOperationException("Batch operation");
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

        throw new UnsupportedOperationException("Batch operation");
    }

    /** {@inheritDoc} */
    @Override
    public void setNull(int paramIdx, int sqlType) throws SQLException {
        ensureNotClosed();
        checkType(sqlType);

        setArgumentValue(paramIdx, null);
    }

    /** {@inheritDoc} */
    @Override
    public void setNull(int paramIdx, int sqlType, String typeName) throws SQLException {
        ensureNotClosed();
        checkType(sqlType);

        setArgumentValue(paramIdx, null);
    }

    /** {@inheritDoc} */
    @Override
    public void setBoolean(int paramIdx, boolean x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setByte(int paramIdx, byte x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setShort(int paramIdx, short x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setInt(int paramIdx, int x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setLong(int paramIdx, long x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setFloat(int paramIdx, float x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setDouble(int paramIdx, double x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setBigDecimal(int paramIdx, BigDecimal x) throws SQLException {
        if (x == null) {
            setNull(paramIdx, Types.DECIMAL);
        } else {
            ensureNotClosed();
            setArgumentValue(paramIdx, x);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setString(int paramIdx, String x) throws SQLException {
        if (x == null) {
            setNull(paramIdx, Types.VARCHAR);
        } else {
            ensureNotClosed();
            setArgumentValue(paramIdx, x);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setBytes(int paramIdx, byte[] x) throws SQLException {
        if (x == null) {
            setNull(paramIdx, Types.VARBINARY);
        } else {
            ensureNotClosed();
            setArgumentValue(paramIdx, x);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setDate(int paramIdx, Date x) throws SQLException {
        if (x == null) {
            setNull(paramIdx, Types.DATE);
        } else {
            setLocalDate(paramIdx, x.toLocalDate());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setDate(int paramIdx, Date x, Calendar cal) throws SQLException {
        setDate(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setTime(int paramIdx, Time x) throws SQLException {
        if (x == null) {
            setNull(paramIdx, Types.TIME);
        } else {
            setLocalTime(paramIdx, x.toLocalTime());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setTime(int paramIdx, Time x, Calendar cal) throws SQLException {
        setTime(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setTimestamp(int paramIdx, Timestamp x) throws SQLException {
        if (x == null) {
            setNull(paramIdx, Types.TIMESTAMP);
        } else {
            setLocalDateTime(paramIdx, x.toLocalDateTime());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setTimestamp(int paramIdx, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override
    public void setAsciiStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setAsciiStream(int paramIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setAsciiStream(int paramIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setUnicodeStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setBinaryStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setBinaryStream(int paramIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setBinaryStream(int paramIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void clearParameters() throws SQLException {
        ensureNotClosed();

        currentArguments = List.of();
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(int paramIdx, Object x) throws SQLException {
        if (x == null) {
            setNull(paramIdx, Types.NULL);
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
        } else if (x instanceof LocalTime) {
            setLocalTime(paramIdx, (LocalTime) x);
        } else if (x instanceof LocalDate) {
            setLocalDate(paramIdx, (LocalDate) x);
        } else if (x instanceof LocalDateTime) {
            setLocalDateTime(paramIdx, (LocalDateTime) x);
        } else if (x instanceof Instant) {
            setInstant(paramIdx, (Instant) x);
        } else if (x instanceof UUID) {
            setUuid(paramIdx, (UUID) x);
        } else {
            throw new SQLException("Parameter type is not supported: " + x.getClass().getName());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(int paramIdx, Object x, int targetSqlType, int scaleOrLen) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(CONVERSION_TO_TARGET_SQL_TYPE_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(int paramIdx, Object x, int targetSqlType) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(CONVERSION_TO_TARGET_SQL_TYPE_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(int paramIdx, Object x, SQLType targetSqlType) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(CONVERSION_TO_TARGET_SQL_TYPE_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(int paramIdx, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(CONVERSION_TO_TARGET_SQL_TYPE_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setCharacterStream(int paramIdx, Reader x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setCharacterStream(int paramIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setCharacterStream(int paramIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setRef(int paramIdx, Ref x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setBlob(int paramIdx, Blob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setBlob(int paramIdx, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setBlob(int paramIdx, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setClob(int paramIdx, Clob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setClob(int paramIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setClob(int paramIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setArray(int paramIdx, Array x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(RESULT_SET_METADATA_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setURL(int paramIdx, URL x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(PARAMETER_METADATA_IS_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setRowId(int paramIdx, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
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

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setNCharacterStream(int paramIdx, Reader val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(STREAMS_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setNClob(int paramIdx, NClob val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setNClob(int paramIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setNClob(int paramIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void setSQLXML(int paramIdx, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public long[] executeLargeBatch() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("executeLargeBatch not implemented.");
    }

    /** {@inheritDoc} */
    @Override
    public long executeLargeUpdate() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("executeLargeUpdate not implemented.");
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        throw new SQLException("The method 'executeLargeUpdate(String, int)' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        ensureNotClosed();

        throw new SQLException("The method 'executeLargeUpdate(String)' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLException("The method 'executeLargeUpdate(String, int[])' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        ensureNotClosed();

        throw new SQLException("The method 'executeLargeUpdate(String, String[])' is called on PreparedStatement instance.",
                SqlStateCode.UNSUPPORTED_OPERATION);
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
        return iface != null && iface.isAssignableFrom(JdbcPreparedStatement2.class);
    }

    private void setLocalDate(int paramIdx, LocalDate x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    private void setLocalTime(int paramIdx, LocalTime x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    private void setLocalDateTime(int paramIdx, LocalDateTime x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    private void setInstant(int paramIdx, Instant x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    private void setUuid(int paramIdx, UUID x) throws SQLException {
        ensureNotClosed();
        setArgumentValue(paramIdx, x);
    }

    private void setArgumentValue(int paramIdx, @Nullable Object val) throws SQLException {
        // The caller method ensures that this statement is not closed.

        if (paramIdx < 1) {
            throw new SQLException("Parameter index is invalid: " + paramIdx);
        }

        if (currentArguments.isEmpty()) {
            currentArguments = new ArrayList<>(paramIdx);
        }

        while (currentArguments.size() < paramIdx) {
            currentArguments.add(null);
        }

        currentArguments.set(paramIdx - 1, val);
    }

    List<Object> getArguments() {
        return currentArguments;
    }

    private static void checkType(int sqlType) throws SQLException {
        JDBCType jdbcType = JDBCType.valueOf(sqlType);
        if (!SUPPORTED_TYPES.contains(jdbcType)) {
            throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
        }
    }
}
