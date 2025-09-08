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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.ByteBuffer;
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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.internal.util.StringUtils;
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

    private final org.apache.ignite.sql.ResultSet<SqlRow> rs;

    private final Supplier<ZoneId> zoneIdSupplier;

    private final Statement statement;

    private Map<String, Integer> columnOrder;

    private int fetchSize;

    private @Nullable SqlRow currentRow;

    private int currentPosition;

    private boolean closed;

    private boolean wasNull;

    private JdbcResultSetMetadata meta;

    /**
     * Constructor.
     */
    public JdbcResultSet(
            org.apache.ignite.sql.ResultSet<SqlRow> rs,
            Statement statement,
            Supplier<ZoneId> zoneIdSupplier
    ) {
        this.rs = rs;
        this.zoneIdSupplier = zoneIdSupplier;
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
            return currentRow != null;
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
        } else if (value instanceof Instant) {
            LocalDateTime localDateTime = instantWithLocalTimeZone((Instant) value);
            JdbcResultSetMetadata jdbcMetadata = initMetadata();

            return Formatters.formatDateTime(localDateTime, colIdx, jdbcMetadata);
        } else if (value instanceof LocalTime) {
            JdbcResultSetMetadata jdbcMetadata = initMetadata();

            return Formatters.formatTime((LocalTime) value, colIdx, jdbcMetadata);
        } else if (value instanceof LocalDateTime) {
            JdbcResultSetMetadata jdbcMetadata = initMetadata();

            return Formatters.formatDateTime((LocalDateTime) value, colIdx, jdbcMetadata);
        } else if (value instanceof LocalDate) {
            return Formatters.formatDate((LocalDate) value);
        } else if (value instanceof byte[]) {
            return StringUtils.toHexString((byte[]) value);
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

        if (val instanceof Boolean) {
            return ((Boolean) val);
        } else if (val instanceof BigDecimal) {
            return BigDecimal.ZERO.compareTo((BigDecimal) val) != 0;
        } else if (val instanceof Double) {
            return ((Number) val).doubleValue() != 0L;
        } else if (val instanceof Float) {
            return ((Number) val).floatValue() != 0L;
        } else if (val instanceof Number) {
            return ((Number) val).longValue() != 0L;
        } else if (val instanceof String) {
            try {
                return Integer.parseInt(val.toString()) != 0;
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot convert to boolean: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to boolean: " + val, SqlStateCode.CONVERSION_FAILED);
        }
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

        if (val instanceof Number) {
            return ((Number) val).byteValue();
        } else if (val instanceof Boolean) {
            return (Boolean) val ? (byte) 1 : (byte) 0;
        } else if (val instanceof String) {
            try {
                return Byte.parseByte(val.toString());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot convert to byte: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to byte: " + val, SqlStateCode.CONVERSION_FAILED);
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

        if (val instanceof Number) {
            return ((Number) val).shortValue();
        } else if (val instanceof Boolean) {
            return (Boolean) val ? (short) 1 : (short) 0;
        } else if (val instanceof String) {
            try {
                return Short.parseShort(val.toString());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot convert to short: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to short: " + val, SqlStateCode.CONVERSION_FAILED);
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

        if (val instanceof Number) {
            return ((Number) val).intValue();
        } else if (val instanceof Boolean) {
            return (Boolean) val ? 1 : 0;
        } else if (val instanceof String) {
            try {
                return Integer.parseInt(val.toString());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot convert to int: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to int: " + val, SqlStateCode.CONVERSION_FAILED);
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
        if (val instanceof Number) {
            return ((Number) val).longValue();
        } else if (val instanceof Boolean) {
            return ((Boolean) val ? 1 : 0);
        } else if (val instanceof String) {
            try {
                return Long.parseLong(val.toString());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot convert to long: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to long: " + val, SqlStateCode.CONVERSION_FAILED);
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

        if (val instanceof Number) {
            return ((Number) val).floatValue();
        } else if (val instanceof Boolean) {
            return ((Boolean) val ? 1 : 0);
        } else if (val instanceof String) {
            try {
                return Float.parseFloat(val.toString());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot convert to float: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to float: " + val, SqlStateCode.CONVERSION_FAILED);
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

        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        } else if (val instanceof Boolean) {
            return ((Boolean) val ? 1.0d : 0.0d);
        } else if (val instanceof String) {
            try {
                return Double.parseDouble(val.toString());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot convert to double: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to double: " + val, SqlStateCode.CONVERSION_FAILED);
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
    public BigDecimal getBigDecimal(int colIdx, int scale) throws SQLException {
        BigDecimal val = getBigDecimal(colIdx);

        return val == null ? null : val.setScale(scale, RoundingMode.HALF_UP);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal getBigDecimal(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return null;
        }

        if (val instanceof BigDecimal) {
            return (BigDecimal) val;
        } else if (val instanceof Float || val instanceof Double) {
            // Perform conversion from double for floating point numbers
            return new BigDecimal(((Number) val).doubleValue());
        } else if (val instanceof Number) {
            // Perform exact conversion from integer types
            return new BigDecimal(((Number) val).longValue());
        } else if (val instanceof Boolean) {
            return new BigDecimal((Boolean) val ? 1 : 0);
        } else if (val instanceof String) {
            try {
                return new BigDecimal(val.toString());
            } catch (Exception e) {
                throw new SQLException("Cannot convert to BigDecimal: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to BigDecimal: " + val, SqlStateCode.CONVERSION_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal getBigDecimal(String colLb, int scale) throws SQLException {
        int colIdx = findColumn(colLb);

        return getBigDecimal(colIdx, scale);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal getBigDecimal(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getBigDecimal(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getBytes(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return null;
        }

        if (val instanceof byte[]) {
            return (byte[]) val;
        } else if (val instanceof Byte) {
            return new byte[]{(byte) val};
        } else if (val instanceof Short) {
            short x = (short) val;

            return ByteBuffer.allocate(2).putShort(x).array();
        } else if (val instanceof Integer) {
            int x = (int) val;
            return ByteBuffer.allocate(4).putInt(x).array();
        } else if (val instanceof Long) {
            long x = (long) val;
            return ByteBuffer.allocate(8).putLong(x).array();
        } else if (val instanceof Float) {
            float x = (Float) val;
            return ByteBuffer.allocate(4).putFloat(x).array();
        } else if (val instanceof Double) {
            double x = (Double) val;
            return ByteBuffer.allocate(8).putDouble(x).array();
        } else if (val instanceof String) {
            return ((String) val).getBytes(UTF_8);
        } else if (val instanceof UUID) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);

            bb.putLong(((UUID) val).getMostSignificantBits());
            bb.putLong(((UUID) val).getLeastSignificantBits());

            return bb.array();
        } else {
            throw new SQLException("Cannot convert to byte[]: " + val, SqlStateCode.CONVERSION_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getBytes(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getBytes(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return null;
        }

        if (val instanceof LocalDate) {
            return Date.valueOf((LocalDate) val);
        } else if (val instanceof LocalTime) {
            return new Date(Time.valueOf((LocalTime) val).getTime());
        } else if (val instanceof Instant) {
            LocalDateTime localDateTime = instantWithLocalTimeZone((Instant) val);

            return Date.valueOf(localDateTime.toLocalDate());
        } else if (val instanceof LocalDateTime) {
            return Date.valueOf(((LocalDateTime) val).toLocalDate());
        } else {
            throw new SQLException("Cannot convert to date: " + val, SqlStateCode.CONVERSION_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getDate(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(int colIdx, Calendar cal) throws SQLException {
        return getDate(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(String colLb, Calendar cal) throws SQLException {
        int colIdx = findColumn(colLb);

        return getDate(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return null;
        }

        if (val instanceof LocalTime) {
            return Time.valueOf((LocalTime) val);
        } else if (val instanceof LocalDate) {
            return new Time(Date.valueOf((LocalDate) val).getTime());
        } else if (val instanceof Instant) {
            LocalDateTime localDateTime = instantWithLocalTimeZone((Instant) val);
            LocalTime localTime = localDateTime.toLocalTime();

            return Time.valueOf(localTime);
        } else if (val instanceof LocalDateTime) {
            return Time.valueOf(((LocalDateTime) val).toLocalTime());
        } else {
            throw new SQLException("Cannot convert to time: " + val, SqlStateCode.CONVERSION_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getTime(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(int colIdx, Calendar cal) throws SQLException {
        return getTime(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(String colLb, Calendar cal) throws SQLException {
        int colIdx = findColumn(colLb);

        return getTime(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return null;
        }

        if (val instanceof LocalTime) {
            return new Timestamp(Time.valueOf((LocalTime) val).getTime());
        } else if (val instanceof LocalDate) {
            return new Timestamp(Date.valueOf((LocalDate) val).getTime());
        } else if (val instanceof Instant) {
            LocalDateTime localDateTime = instantWithLocalTimeZone((Instant) val);

            return Timestamp.valueOf(localDateTime);
        } else if (val instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) val);
        } else {
            throw new SQLException("Cannot convert to timestamp: " + val, SqlStateCode.CONVERSION_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(int colIdx, Calendar cal) throws SQLException {
        return getTimestamp(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(String colLb, Calendar cal) throws SQLException {
        int colIdx = findColumn(colLb);

        return getTimestamp(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getTimestamp(colIdx);
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

        return null;
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

        Objects.requireNonNull(colLb);

        Integer order = columnOrder().get(colLb.toUpperCase());

        if (order == null) {
            throw new SQLException("Column not found: " + colLb, SqlStateCode.PARSING_EXCEPTION);
        }

        assert order >= 0;

        return order + 1;
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

        return currentRow == null && !rs.hasNext();
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

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean rowInserted() throws SQLException {
        ensureNotClosed();

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean rowDeleted() throws SQLException {
        ensureNotClosed();

        return false;
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
    public <T> T getObject(int colIdx, Class<T> targetCls) throws SQLException {
        ensureNotClosed();

        return (T) getObject0(colIdx, targetCls);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T getObject(String colLb, Class<T> type) throws SQLException {
        int colIdx = findColumn(colLb);

        return getObject(colIdx, type);
    }

    /** {@inheritDoc} */
    @Override
    public Object getObject(int colIdx) throws SQLException {
        return getValue(colIdx);
    }

    /** {@inheritDoc} */
    @Override
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
    Object getValue(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        if (colIdx < 1 || colIdx > rs.metadata().columns().size()) {
            throw new SQLException("Invalid column index: " + colIdx, SqlStateCode.PARSING_EXCEPTION);
        }

        try {
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
    private Object getObject0(int colIdx, Class<?> targetCls) throws SQLException {
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
                throw new SQLException("Cannot convert to " + targetCls.getName() + ": " + val,
                        SqlStateCode.CONVERSION_FAILED);
            }
        }
    }

    /**
     * Init if needed and return column order.
     *
     * @return Column order map.
     * @throws SQLException On error.
     */
    private Map<String, Integer> columnOrder() throws SQLException {
        if (columnOrder != null) {
            return columnOrder;
        }

        JdbcResultSetMetadata metadata = initMetadata();

        columnOrder = new HashMap<>(metadata.getColumnCount());

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            String colName = metadata.getColumnLabel(i + 1).toUpperCase();

            if (!columnOrder.containsKey(colName)) {
                columnOrder.put(colName, i);
            }
        }

        return columnOrder;
    }

    private JdbcResultSetMetadata initMetadata() throws SQLException {
        if (meta == null) {
            ResultSetMetadata metadata = rs.metadata();
            if (metadata == null) {
                throw new SQLException("ResultSet doesn't have metadata.");
            }
            meta = new JdbcResultSetMetadata(metadata);
        }
        return meta;
    }

    private LocalDateTime instantWithLocalTimeZone(Instant val) {
        ZoneId zoneId = zoneIdSupplier.get();

        if (zoneId == null) {
            zoneId = ZoneId.systemDefault();
        }
        return LocalDateTime.ofInstant(val, zoneId);
    }

    private static class Formatters {
        static final DateTimeFormatter TIME = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .toFormatter();

        static final DateTimeFormatter DATE = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4)
                .appendLiteral('-')
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendLiteral('-')
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .toFormatter();

        static final DateTimeFormatter DATE_TIME = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4)
                .appendLiteral('-')
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendLiteral('-')
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .appendLiteral(' ')
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .toFormatter();

        static String formatTime(LocalTime value, int colIdx, JdbcResultSetMetadata jdbcMeta) throws SQLException {
            return formatWithPrecision(TIME, value, colIdx, jdbcMeta);
        }

        static String formatDateTime(LocalDateTime value, int colIdx, JdbcResultSetMetadata jdbcMeta) throws SQLException {
            return formatWithPrecision(DATE_TIME, value, colIdx, jdbcMeta);
        }

        static String formatDate(LocalDate value) {
            return DATE.format(value);
        }

        private static String formatWithPrecision(
                DateTimeFormatter formatter,
                TemporalAccessor value,
                int colIdx,
                JdbcResultSetMetadata jdbcMeta
        ) throws SQLException {

            StringBuilder sb = new StringBuilder();

            formatter.formatTo(value, sb);

            int precision = jdbcMeta.getPrecision(colIdx);
            if (precision <= 0) {
                return sb.toString();
            }

            assert precision <= 9 : "Precision is out of range. Precision: " + precision + ". Column: " + colIdx;

            // Append nano seconds according to the specified precision.
            long nanos = value.getLong(ChronoField.NANO_OF_SECOND);
            long scaled = nanos / (long) Math.pow(10, 9 - precision);

            sb.append('.');
            for (int i = 0; i < precision; i++) {
                sb.append('0');
            }

            int pos = precision - 1;
            int start = sb.length() - precision;

            do {
                int digit = (int) (scaled % 10);
                char c = (char) ('0' + digit);
                sb.setCharAt(start + pos, c);
                scaled /= 10;
                pos--;
            } while (scaled != 0 && pos >= 0);

            return sb.toString();
        }
    }
}
