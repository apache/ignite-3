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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.temporal.Temporal;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC ResultSet adapter backed by {@link org.apache.ignite.sql.ResultSet}.
 */
public class JdbcResultSet implements ResultSet {

    private static final String UPDATES_ARE_NOT_SUPPORTED = "Updates are not supported.";

    private static final String SQL_STRUCTURED_TYPE_ARE_NOT_SUPPORTED = "SQL structured type are not supported.";

    private static final String SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED = "SQL-specific types are not supported.";

    private static final ResultSetMetadata EMPTY_METADATA = new ResultSetMetadataImpl(List.of());

    private static final BigDecimal MIN_DOUBLE = BigDecimal.valueOf(-Double.MAX_VALUE);

    private static final BigDecimal MAX_DOUBLE = BigDecimal.valueOf(Double.MAX_VALUE);

    private final org.apache.ignite.sql.ResultSet<SqlRow> rs;

    private final ResultSetMetadata rsMetadata;

    private final Statement statement;

    private int fetchSize;

    private @Nullable SqlRow currentRow;

    private int currentPosition;

    private boolean closed;

    private boolean wasNull;

    private JdbcResultSetMetadata jdbcMeta;

    /**
     * Constructor.
     */
    public JdbcResultSet(
            org.apache.ignite.sql.ResultSet<SqlRow> rs,
            Statement statement
    ) {
        this.rs = rs;

        ResultSetMetadata metadata = rs.metadata();
        this.rsMetadata = metadata != null ? metadata : EMPTY_METADATA;

        this.statement = statement;
        this.currentRow = null;
        this.closed = false;
        this.wasNull = false;
    }

    @Override
    public boolean next() throws SQLException {
        ensureNotClosed();

        try {
            if (!rs.hasNext()) {
                currentRow = null;
                return false;
            }
            currentRow = rs.next();
            currentPosition += 1;
            return true;
        } catch (Exception e) {
            Throwable cause = IgniteExceptionMapperUtil.mapToPublicException(e);
            throw new SQLException(cause.getMessage(), cause);
        }
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;

        try {
            rs.close();
        } catch (Exception e) {
            Throwable cause = IgniteExceptionMapperUtil.mapToPublicException(e);
            throw new SQLException(cause.getMessage(), cause);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasNull() throws SQLException {
        ensureNotClosed();

        return wasNull;
    }

    /** {@inheritDoc} */
    @Override
    public String getString(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        Object value = getValue(colIdx);
        if (value == null) {
            return null;
        }

        if (value instanceof Temporal || value instanceof byte[] || value instanceof UUID) {
            throw new UnsupportedOperationException();
        } else {
            return String.valueOf(value);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String getString(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getString(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean getBoolean(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return false;
        }

        ColumnMetadata column = rsMetadata.columns().get(colIdx - 1);
        switch (column.type()) {
            case BOOLEAN:
                return ((Boolean) val);
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                long num = ((Number) val).longValue();
                return num != 0;
            case STRING:
                String str = (String) val;
                if ("0".equals(str)) {
                    return false;
                } else if ("1".equals(str)) {
                    return true;
                }
                break;
            default:
                // Fallthrough
        }

        throw new SQLException("Cannot convert to boolean: " + val, SqlStateCode.CONVERSION_FAILED);
    }

    /** {@inheritDoc} */
    @Override
    public boolean getBoolean(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getBoolean(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public byte getByte(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return 0;
        }

        ColumnType columnType = getColumnType(colIdx);
        switch (columnType) {
            case BOOLEAN:
                return (Boolean) val ? (byte) 1 : (byte) 0;
            case INT8:
                return (byte) val;
            case INT16:
            case INT32:
            case INT64:
                //noinspection NumericCastThatLosesPrecision
                return (byte) getLongValue(((Number) val).longValue(), Byte.TYPE.getTypeName(), Byte.MIN_VALUE, Byte.MAX_VALUE);
            case FLOAT:
                //noinspection NumericCastThatLosesPrecision
                return (byte) getFloatValueAsLong((float) val, Byte.TYPE.getTypeName(), Byte.MIN_VALUE, Byte.MAX_VALUE);
            case DOUBLE:
                //noinspection NumericCastThatLosesPrecision
                return (byte) getDoubleValueAsLong((double) val, Byte.TYPE.getTypeName(), Byte.MIN_VALUE, Byte.MAX_VALUE);
            case DECIMAL:
                //noinspection NumericCastThatLosesPrecision
                return (byte) getDecimalValueAsLong((BigDecimal) val, Byte.TYPE.getTypeName(), Byte.MIN_VALUE, Byte.MAX_VALUE);
            case STRING:
                try {
                    return Byte.parseByte(val.toString());
                } catch (NumberFormatException e) {
                    throw conversionError(Byte.TYPE.getTypeName(), val, e);
                }
            default:
                throw conversionError(Byte.TYPE.getTypeName(), val);
        }
    }

    /** {@inheritDoc} */
    @Override
    public byte getByte(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getByte(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public short getShort(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return 0;
        }

        ColumnType columnType = getColumnType(colIdx);
        switch (columnType) {
            case BOOLEAN:
                return (Boolean) val ? (short) 1 : (short) 0;
            case INT8:
                return (byte) val;
            case INT16:
                return (short) val;
            case INT32:
            case INT64:
                //noinspection NumericCastThatLosesPrecision
                return (short) getLongValue(((Number) val).longValue(), Short.TYPE.getTypeName(), Short.MIN_VALUE, Short.MAX_VALUE);
            case FLOAT:
                //noinspection NumericCastThatLosesPrecision
                return (short) getFloatValueAsLong((float) val, Short.TYPE.getTypeName(), Short.MIN_VALUE, Short.MAX_VALUE);
            case DOUBLE:
                //noinspection NumericCastThatLosesPrecision
                return (short) getDoubleValueAsLong((double) val, Short.TYPE.getTypeName(), Short.MIN_VALUE, Short.MAX_VALUE);
            case DECIMAL:
                //noinspection NumericCastThatLosesPrecision
                return (short) getDecimalValueAsLong((BigDecimal) val, Short.TYPE.getTypeName(), Short.MIN_VALUE, Short.MAX_VALUE);
            case STRING:
                try {
                    return Short.parseShort(val.toString());
                } catch (NumberFormatException e) {
                    throw conversionError(Short.TYPE.getTypeName(), val, e);
                }
            default:
                throw conversionError(Short.TYPE.getTypeName(), val);
        }
    }

    /** {@inheritDoc} */
    @Override
    public short getShort(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getShort(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public int getInt(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return 0;
        }

        ColumnType columnType = getColumnType(colIdx);
        switch (columnType) {
            case BOOLEAN:
                return (Boolean) val ? 1 : 0;
            case INT8:
                return (byte) val;
            case INT16:
                return (short) val;
            case INT32:
                return (int) val;
            case INT64:
                //noinspection NumericCastThatLosesPrecision
                return (int) getLongValue(((Number) val).longValue(), Integer.TYPE.getTypeName(), Integer.MIN_VALUE, Integer.MAX_VALUE);
            case FLOAT:
                //noinspection NumericCastThatLosesPrecision
                return (int) getFloatValueAsLong((float) val, Integer.TYPE.getTypeName(), Integer.MIN_VALUE, Integer.MAX_VALUE);
            case DOUBLE:
                //noinspection NumericCastThatLosesPrecision
                return (int) getDoubleValueAsLong((double) val, Integer.TYPE.getTypeName(), Integer.MIN_VALUE, Integer.MAX_VALUE);
            case DECIMAL:
                //noinspection NumericCastThatLosesPrecision
                return (int) getDecimalValueAsLong((BigDecimal) val, Integer.TYPE.getTypeName(), Integer.MIN_VALUE, Integer.MAX_VALUE);
            case STRING:
                try {
                    return Integer.parseInt(val.toString());
                } catch (NumberFormatException e) {
                    throw conversionError(Integer.TYPE.getTypeName(), val, e);
                }
            default:
                throw conversionError(Integer.TYPE.getTypeName(), val);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getInt(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getInt(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public long getLong(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return 0;
        }

        ColumnType columnType = getColumnType(colIdx);
        switch (columnType) {
            case BOOLEAN:
                return (Boolean) val ? 1 : 0;
            case INT8:
                return (byte) val;
            case INT16:
                return (short) val;
            case INT32:
                return (int) val;
            case INT64:
                return (long) val;
            case FLOAT:
                return getFloatValueAsLong((float) val, Long.TYPE.getTypeName(), Long.MIN_VALUE, Long.MAX_VALUE);
            case DOUBLE:
                return getDoubleValueAsLong((double) val, Long.TYPE.getTypeName(), Long.MIN_VALUE, Long.MAX_VALUE);
            case DECIMAL:
                return getDecimalValueAsLong((BigDecimal) val, Long.TYPE.getTypeName(), Long.MIN_VALUE, Long.MAX_VALUE);
            case STRING:
                try {
                    return Long.parseLong(val.toString());
                } catch (NumberFormatException e) {
                    throw conversionError(Long.TYPE.getTypeName(), val, e);
                }
            default:
                throw conversionError(Long.TYPE.getTypeName(), val);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long getLong(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getLong(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return 0;
        }

        ColumnType columnType = getColumnType(colIdx);
        switch (columnType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
                return ((Number) val).floatValue();
            case DOUBLE:
                double num = (double) val;
                if (num < -Float.MAX_VALUE || num > Float.MAX_VALUE) {
                    throw conversionError(Float.TYPE.getTypeName(), val);
                }
                //noinspection NumericCastThatLosesPrecision
                return (float) num;
            case DECIMAL:
                BigDecimal bd = (BigDecimal) val;
                if (bd.doubleValue() < -Float.MAX_VALUE || bd.doubleValue() > Float.MAX_VALUE) {
                    throw conversionError(Float.TYPE.getTypeName(), val);
                }
                return bd.floatValue();
            case STRING:
                try {
                    return Float.parseFloat(val.toString());
                } catch (NumberFormatException e) {
                    throw conversionError(Float.TYPE.getTypeName(), val, e);
                }
            default:
                throw conversionError(Float.TYPE.getTypeName(), val);
        }
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getFloat(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return 0.0d;
        }

        ColumnType columnType = getColumnType(colIdx);
        switch (columnType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
                return ((Number) val).doubleValue();
            case DOUBLE:
                return (double) val;
            case DECIMAL:
                BigDecimal bd = (BigDecimal) val;
                if (bd.compareTo(MIN_DOUBLE) < 0 || bd.compareTo(MAX_DOUBLE) > 0) {
                    throw conversionError(Double.TYPE.getTypeName(), val);
                }
                return bd.doubleValue();
            case STRING:
                try {
                    return Double.parseDouble(val.toString());
                } catch (NumberFormatException e) {
                    throw conversionError(Double.TYPE.getTypeName(), val, e);
                }
            default:
                throw conversionError(Double.TYPE.getTypeName(), val);
        }
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getDouble(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public BigDecimal getBigDecimal(int colIdx, int scale) throws SQLException {
        BigDecimal val = getBigDecimal(colIdx);

        return val == null ? null : val.setScale(scale, RoundingMode.HALF_UP);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public BigDecimal getBigDecimal(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return null;
        }

        ColumnType columnType = getColumnType(colIdx);
        switch (columnType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return new BigDecimal(((Number) val).longValue());
            case FLOAT:
            case DOUBLE:
                return new BigDecimal(((Number) val).doubleValue());
            case DECIMAL:
                return (BigDecimal) val;
            case STRING:
                try {
                    return new BigDecimal(val.toString());
                } catch (Exception e) {
                    throw new SQLException("Cannot convert to BigDecimal: " + val, SqlStateCode.CONVERSION_FAILED, e);
                }
            default:
                throw new SQLException("Cannot convert to BigDecimal: " + val, SqlStateCode.CONVERSION_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public BigDecimal getBigDecimal(String colLb, int scale) throws SQLException {
        int colIdx = findColumn(colLb);

        return getBigDecimal(colIdx, scale);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public BigDecimal getBigDecimal(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getBigDecimal(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getBytes(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getBytes(String colLb) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(String colLb) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(int colIdx, Calendar cal) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(String colLb, Calendar cal) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(String colLb) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(int colIdx, Calendar cal) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(String colLb, Calendar cal) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(int colIdx, Calendar cal) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        return getTimestamp(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(String colLb, Calendar cal) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(String colLb) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getAsciiStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getAsciiStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getUnicodeStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getUnicodeStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getBinaryStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Stream are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getBinaryStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
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
    public String getCursorName() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Cursor name is not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        return initMetadata();
    }

    /** {@inheritDoc} */
    @Override
    public int findColumn(String colLb) throws SQLException {
        ensureNotClosed();

        try {
            int index = rsMetadata.indexOf(colLb);
            if (index >= 0) {
                return index + 1;
            }
        } catch (Exception ignore) {
            // ignore
        }

        throw new SQLException("Column not found: " + colLb, SqlStateCode.INVALID_ARGUMENT);
    }

    /** {@inheritDoc} */
    @Override
    public Reader getCharacterStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Reader getCharacterStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean isBeforeFirst() throws SQLException {
        ensureNotClosed();

        return currentRow == null && rs.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isAfterLast() throws SQLException {
        ensureNotClosed();

        boolean hasNext = rs.hasNext();
        // Result set is empty
        if (currentPosition == 0 && !hasNext) {
            return false;
        } else {
            return currentRow == null && !hasNext;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isFirst() throws SQLException {
        ensureNotClosed();

        return currentRow != null && currentPosition == 1;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isLast() throws SQLException {
        ensureNotClosed();

        return currentRow != null && !rs.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public void beforeFirst() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override
    public void afterLast() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean first() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean last() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override
    public int getRow() throws SQLException {
        ensureNotClosed();

        return isAfterLast() ? 0 : currentPosition;
    }

    /** {@inheritDoc} */
    @Override
    public boolean absolute(int row) throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean relative(int rows) throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean previous() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureNotClosed();

        if (direction != FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Only forward direction is supported");
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
            throw new SQLException("Fetch size must be greater than zero.");
        }

        this.fetchSize = fetchSize;
    }

    /** {@inheritDoc} */
    @Override
    public int getFetchSize() throws SQLException {
        ensureNotClosed();

        return fetchSize;
    }

    /** {@inheritDoc} */
    @Override
    public int getType() throws SQLException {
        ensureNotClosed();

        return TYPE_FORWARD_ONLY;
    }

    /** {@inheritDoc} */
    @Override
    public int getConcurrency() throws SQLException {
        ensureNotClosed();

        return CONCUR_READ_ONLY;
    }

    /** {@inheritDoc} */
    @Override
    public boolean rowUpdated() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public boolean rowInserted() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public boolean rowDeleted() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNull(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNull(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBoolean(int colIdx, boolean x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBoolean(String colLb, boolean x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateByte(int colIdx, byte x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateByte(String colLb, byte x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateShort(int colIdx, short x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateShort(String colLb, short x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateInt(int colIdx, int x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateInt(String colLb, int x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateLong(int colIdx, long x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateLong(String colLb, long x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateFloat(int colIdx, float x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateFloat(String colLb, float x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateDouble(int colIdx, double x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateDouble(String colLb, double x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBigDecimal(int colIdx, BigDecimal x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBigDecimal(String colLb, BigDecimal x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateString(int colIdx, String x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateString(String colLb, String x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBytes(int colIdx, byte[] x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBytes(String colLb, byte[] x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateDate(int colIdx, Date x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateDate(String colLb, Date x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateTime(int colIdx, Time x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateTime(String colLb, Time x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateTimestamp(int colIdx, Timestamp x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateTimestamp(String colLb, Timestamp x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(int colIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(String colLb, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(int colIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(String colLb, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(int colIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(String colLb, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(int colIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(int colIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(String colLb, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(int colIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(String colLb, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(String colLb, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(int colIdx, Reader x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(String colLb, Reader reader, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(int colIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(int colIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateObject(int colIdx, Object x, int scaleOrLen) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateObject(int colIdx, Object x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateObject(String colLb, Object x, int scaleOrLen) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateObject(String colLb, Object x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void insertRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void deleteRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void refreshRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Row refreshing is not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void cancelRowUpdates() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Row updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void moveToInsertRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void moveToCurrentRow() throws SQLException {
        ensureNotClosed();

        if (getConcurrency() == CONCUR_READ_ONLY) {
            throw new SQLException("The result set concurrency is CONCUR_READ_ONLY");
        }
    }

    /** {@inheritDoc} */
    @Override
    public Statement getStatement() throws SQLException {
        ensureNotClosed();

        return statement;
    }

    /** {@inheritDoc} */
    @Override
    public Object getObject(int colIdx, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(SQL_STRUCTURED_TYPE_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public <T> T getObject(int colIdx, Class<T> targetCls) throws SQLException {
        ensureNotClosed();

        return (T) getObject0(colIdx, targetCls);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public <T> T getObject(String colLb, Class<T> type) throws SQLException {
        int colIdx = findColumn(colLb);

        return getObject(colIdx, type);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public Object getObject(int colIdx) throws SQLException {
        return getValue(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public Object getObject(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getValue(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Object getObject(String colLb, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(SQL_STRUCTURED_TYPE_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Ref getRef(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Ref getRef(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Blob getBlob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Blob getBlob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Clob getClob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Clob getClob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Array getArray(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Array getArray(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public URL getURL(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public URL getURL(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateRef(int colIdx, Ref x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateRef(String colLb, Ref x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(int colIdx, Blob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(String colLb, Blob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(int colIdx, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(String colLb, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(int colIdx, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(String colLb, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(int colIdx, Clob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(String colLb, Clob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(int colIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(int colIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateArray(int colIdx, Array x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateArray(String colLb, Array x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public RowId getRowId(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public RowId getRowId(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateRowId(int colIdx, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateRowId(String colLb, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public int getHoldability() throws SQLException {
        ensureNotClosed();

        return HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() throws SQLException {
        return closed || statement.isClosed();
    }

    /** {@inheritDoc} */
    @Override
    public void updateNString(int colIdx, String val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNString(String colLb, String val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(int colIdx, NClob val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(String colLb, NClob val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(int colIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(int colIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public NClob getNClob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public NClob getNClob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public SQLXML getSQLXML(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public SQLXML getSQLXML(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateSQLXML(int colIdx, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateSQLXML(String colLb, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public String getNString(int colIdx) throws SQLException {
        return getString(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public String getNString(String colLb) throws SQLException {
        return getString(colLb);
    }

    /** {@inheritDoc} */
    @Override
    public Reader getNCharacterStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public Reader getNCharacterStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(SQL_SPECIFIC_TYPES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNCharacterStream(int colIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNCharacterStream(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNCharacterStream(int colIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public void updateNCharacterStream(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException(UPDATES_ARE_NOT_SUPPORTED);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface)) {
            throw new SQLException("Result set is not a wrapper for " + iface.getName());
        }

        return (T) this;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcResultSet.class);
    }

    /**
     * Gets object field value by index.
     *
     * @param colIdx Column index.
     * @return Object field value.
     * @throws SQLException In case of error.
     */
    @Nullable
    Object getValue(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        if (colIdx < 1 || colIdx > rsMetadata.columns().size()) {
            throw new SQLException("Invalid column index: " + colIdx, SqlStateCode.INVALID_ARGUMENT);
        }

        try {
            assert currentRow != null;
            Object val = currentRow.value(colIdx - 1);

            wasNull = val == null;

            return val;
        } catch (Exception e) {
            Throwable cause = IgniteExceptionMapperUtil.mapToPublicException(e);
            throw new SQLException("Unable to value for column: " + colIdx, cause);
        }
    }

    /**
     * Ensures that result set is positioned on a row.
     *
     * @throws SQLException If result set is not positioned on a row.
     */
    private void ensureHasCurrentRow() throws SQLException {
        if (currentRow == null) {
            throw new SQLException("Result set is not positioned on a row.");
        }
    }

    /**
     * Ensures that result set is not closed.
     *
     * @throws SQLException If result set is closed.
     */
    private void ensureNotClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Result set is closed.", SqlStateCode.INVALID_CURSOR_STATE);
        }
    }

    /**
     * Get object of given class.
     *
     * @param colIdx Column index.
     * @param targetCls Class representing the Java data type to convert the designated column to.
     * @return Converted object.
     * @throws SQLException On error.
     */
    private @Nullable Object getObject0(int colIdx, Class<?> targetCls) throws SQLException {
        if (targetCls == Boolean.class) {
            return getBoolean(colIdx);
        } else if (targetCls == Byte.class) {
            return getByte(colIdx);
        } else if (targetCls == Short.class) {
            return getShort(colIdx);
        } else if (targetCls == Integer.class) {
            return getInt(colIdx);
        } else if (targetCls == Long.class) {
            return getLong(colIdx);
        } else if (targetCls == Float.class) {
            return getFloat(colIdx);
        } else if (targetCls == Double.class) {
            return getDouble(colIdx);
        } else if (targetCls == String.class) {
            return getString(colIdx);
        } else if (targetCls == BigDecimal.class) {
            return getBigDecimal(colIdx);
        } else if (targetCls == Date.class) {
            return getDate(colIdx);
        } else if (targetCls == Time.class) {
            return getTime(colIdx);
        } else if (targetCls == Timestamp.class) {
            return getTimestamp(colIdx);
        } else if (targetCls == byte[].class) {
            return getBytes(colIdx);
        } else {
            Object val = getValue(colIdx);

            if (val == null) {
                return null;
            }

            Class<?> cls = val.getClass();

            if (targetCls.isAssignableFrom(cls)) {
                return val;
            } else {
                throw conversionError(targetCls.getTypeName(), val);
            }
        }
    }

    private JdbcResultSetMetadata initMetadata() {
        if (jdbcMeta == null) {
            jdbcMeta = new JdbcResultSetMetadata(rsMetadata);
        }
        return jdbcMeta;
    }

    private ColumnType getColumnType(int colIdx) {
        ColumnMetadata column = rsMetadata.columns().get(colIdx - 1);
        return column.type();
    }

    private static long getLongValue(long val, String typeName, long min, long max) throws SQLException {
        if (val < min || val > max) {
            throw conversionError(typeName, val);
        }
        return val;
    }

    private static long getFloatValueAsLong(float val, String typeName, long min, long max) throws SQLException {
        if (val < min || val > max || Float.isInfinite(val) || Float.isNaN(val)) {
            throw conversionError(typeName, val);
        }
        //noinspection NumericCastThatLosesPrecision
        return (long) val;
    }

    private static long getDoubleValueAsLong(double val, String typeName, long min, long max) throws SQLException {
        if (val < min || val > max || Double.isInfinite(val) || Double.isNaN(val)) {
            throw conversionError(typeName, val);
        }
        //noinspection NumericCastThatLosesPrecision
        return (long) val;
    }

    private static long getDecimalValueAsLong(BigDecimal num, String typeName, long min, long max) throws SQLException {
        long val;
        boolean failed;

        // We can safely remove a fractional part, conversion from one int type to another involves rounding but 
        // longValueExact fails if a fractional part present.
        BigDecimal intNum = num.setScale(0, RoundingMode.DOWN);
        try {
            val = intNum.longValueExact();
            failed = false;
        } catch (ArithmeticException e) {
            failed = true;
            val = 0;
        }

        if (failed || val < min || val > max) {
            throw conversionError(typeName, num);
        }

        return val;
    }

    private static SQLException conversionError(String typeName, Object val) {
        return conversionError(typeName, val, null);
    }

    private static SQLException conversionError(String typeName, Object val, @Nullable Throwable cause) {
        return new SQLException(format("Cannot convert to {}: {}", typeName, val), SqlStateCode.CONVERSION_FAILED, cause);
    }
}
