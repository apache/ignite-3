/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
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
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.QueryFetchRequest;
import org.apache.ignite.internal.jdbc.proto.event.QueryFetchResult;

/**
 * Jdbc result set implementation.
 */
public class JdbcResultSet implements ResultSet {
    /** Decimal format to convert string to decimal. */
    private static final ThreadLocal<DecimalFormat> decimalFormat = new ThreadLocal<>() {
        /** {@inheritDoc} */
        @Override
        protected DecimalFormat initialValue() {
            DecimalFormatSymbols symbols = new DecimalFormatSymbols();

            symbols.setGroupingSeparator(',');
            symbols.setDecimalSeparator('.');

            DecimalFormat decimalFormat = new DecimalFormat("", symbols);

            decimalFormat.setParseBigDecimal(true);

            return decimalFormat;
        }
    };

    /** Statement. */
    private final JdbcStatement stmt;

    /** Cursor ID. */
    private final Long cursorId;

    /** Jdbc column metadata. */
    private List<JdbcColumnMeta> meta;

    /** Metadata initialization flag. */
    private boolean metaInit;

    /** Column order map. */
    private Map<String, Integer> colOrder;

    /** Rows. */
    private List<List<Object>> rows;

    /** Rows iterator. */
    private Iterator<List<Object>> rowsIter;

    /** Current row. */
    private List<Object> curRow;

    /** Current position. */
    private int curPos;

    /** Finished flag. */
    private boolean finished;

    /** Closed flag. */
    private boolean closed;

    /** Was {@code NULL} flag. */
    private boolean wasNull;

    /** Fetch size. */
    private int fetchSize;

    /** Is query flag. */
    private boolean isQuery;

    /** Auto close server cursors flag. */
    private boolean autoClose;

    /** Update count. */
    private long updCnt;

    /** Close statement after close result set count. */
    private boolean closeStmt;

    /** Query request handler. */
    private JdbcQueryEventHandler qryHandler;

    /** Jdbc metadata. */
    private JdbcResultSetMetadata jdbcMeta;

    /**
     * Creates new result set.
     *
     * @param stmt       Statement.
     * @param cursorId   Cursor ID.
     * @param fetchSize  Fetch size.
     * @param finished   Finished flag.
     * @param rows       Rows.
     * @param isQry      Is Result ser for Select query.
     * @param autoClose  Is automatic close of server cursors enabled.
     * @param updCnt     Update count.
     * @param closeStmt  Close statement on the result set close.
     * @param qryHandler QueryEventHandler (local or remote).
     */
    JdbcResultSet(JdbcStatement stmt, long cursorId, int fetchSize, boolean finished,
            List<List<Object>> rows, boolean isQry, boolean autoClose, long updCnt, boolean closeStmt, JdbcQueryEventHandler qryHandler) {
        assert stmt != null;
        assert fetchSize > 0;

        this.qryHandler = qryHandler;
        this.stmt = stmt;
        this.cursorId = cursorId;
        this.fetchSize = fetchSize;
        this.finished = finished;
        this.isQuery = isQry;
        this.autoClose = autoClose;
        this.closeStmt = closeStmt;

        if (isQuery) {
            this.rows = rows;

            rowsIter = rows != null ? rows.iterator() : null;
        } else {
            this.updCnt = updCnt;
        }
    }

    /**
     * Creates new result set.
     *
     * @param rows Rows.
     * @param meta Column metadata.
     */
    public JdbcResultSet(List<List<Object>> rows, List<JdbcColumnMeta> meta) {
        stmt = null;
        cursorId = null;

        finished = true;
        isQuery = true;

        this.rows = rows;
        this.rowsIter = rows.iterator();
        this.meta = meta;
        this.metaInit = true;

        initColumnOrder();
    }

    /** {@inheritDoc} */
    @Override
    public boolean next() throws SQLException {
        ensureNotClosed();

        if ((rowsIter == null || !rowsIter.hasNext()) && !finished) {
            QueryFetchResult res = qryHandler.fetchAsync(new QueryFetchRequest(cursorId, fetchSize)).join();

            if (!res.hasResults()) {
                throw IgniteQueryErrorCode.createJdbcSqlException(res.err(), res.status());
            }

            rows = res.items();
            finished = res.last();

            rowsIter = rows.iterator();
        }

        if (rowsIter != null) {
            if (rowsIter.hasNext()) {
                curRow = rowsIter.next();

                curPos++;

                return true;
            } else {
                rowsIter = null;
                curRow = null;

                return false;
            }
        } else {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws SQLException {
        close0();

        if (closeStmt) {
            stmt.closeIfAllResultsClosed();
        }
    }

    /**
     * Close result set.
     *
     * @throws SQLException On error.
     */
    void close0() throws SQLException {
        if (isClosed()) {
            return;
        }

        try {
            if (stmt != null && (!finished || (isQuery && !autoClose))) {
                QueryCloseResult res = qryHandler.closeAsync(new QueryCloseRequest(cursorId)).join();

                if (!res.hasResults()) {
                    throw IgniteQueryErrorCode.createJdbcSqlException(res.err(), res.status());
                }
            }
        } finally {
            closed = true;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasNull() throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        return wasNull;
    }

    /** {@inheritDoc} */
    @Override
    public String getString(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        return val == null ? null : String.valueOf(val);
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

        Class<?> cls = val.getClass();

        if (cls == Boolean.class) {
            return ((Boolean) val);
        } else if (val instanceof Number) {
            return ((Number) val).intValue() != 0;
        } else if (cls == String.class || cls == Character.class) {
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

        Class<?> cls = val.getClass();

        if (val instanceof Number) {
            return ((Number) val).byteValue();
        } else if (cls == Boolean.class) {
            return (Boolean) val ? (byte) 1 : (byte) 0;
        } else if (cls == String.class || cls == Character.class) {
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

        Class<?> cls = val.getClass();

        if (val instanceof Number) {
            return ((Number) val).shortValue();
        } else if (cls == Boolean.class) {
            return (Boolean) val ? (short) 1 : (short) 0;
        } else if (cls == String.class || cls == Character.class) {
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

        Class<?> cls = val.getClass();

        if (val instanceof Number) {
            return ((Number) val).intValue();
        } else if (cls == Boolean.class) {
            return (Boolean) val ? 1 : 0;
        } else if (cls == String.class || cls == Character.class) {
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

        Class<?> cls = val.getClass();

        if (val instanceof Number) {
            return ((Number) val).longValue();
        } else if (cls == Boolean.class) {
            return ((Boolean) val ? 1 : 0);
        } else if (cls == String.class || cls == Character.class) {
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

        Class<?> cls = val.getClass();

        if (val instanceof Number) {
            return ((Number) val).floatValue();
        } else if (cls == Boolean.class) {
            return (float) ((Boolean) val ? 1 : 0);
        } else if (cls == String.class || cls == Character.class) {
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
            return 0d;
        }

        Class<?> cls = val.getClass();

        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        } else if (cls == Boolean.class) {
            return ((Boolean) val ? 1d : 0d);
        } else if (cls == String.class || cls == Character.class) {
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

        Class<?> cls = val.getClass();

        if (cls == BigDecimal.class) {
            return (BigDecimal) val;
        } else if (val instanceof Number) {
            return new BigDecimal(((Number) val).doubleValue());
        } else if (cls == Boolean.class) {
            return new BigDecimal((Boolean) val ? 1 : 0);
        } else if (cls == String.class || cls == Character.class) {
            try {
                return (BigDecimal) decimalFormat.get().parse(val.toString());
            } catch (ParseException e) {
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

        Class<?> cls = val.getClass();

        if (cls == byte[].class) {
            return (byte[]) val;
        } else if (cls == Byte.class) {
            return new byte[]{(byte) val};
        } else if (cls == Short.class) {
            short x = (short) val;

            return new byte[]{(byte) (x >> 8), (byte) x};
        } else if (cls == Integer.class) {
            int x = (int) val;

            return new byte[]{(byte) (x >> 24), (byte) (x >> 16), (byte) (x >> 8), (byte) x};
        } else if (cls == Long.class) {
            long x = (long) val;

            return new byte[]{(byte) (x >> 56), (byte) (x >> 48), (byte) (x >> 40), (byte) (x >> 32),
                    (byte) (x >> 24), (byte) (x >> 16), (byte) (x >> 8), (byte) x};
        } else if (cls == Float.class) {
            return ByteBuffer.allocate(4).putFloat(((Float) val)).array();
        } else if (cls == Double.class) {
            return ByteBuffer.allocate(8).putDouble(((Double) val)).array();
        } else if (cls == String.class) {
            return ((String) val).getBytes(UTF_8);
        } else if (cls == UUID.class) {
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

        Class<?> cls = val.getClass();

        if (cls == LocalDate.class) {
            return Date.valueOf((LocalDate) val);
        } else if (cls == LocalTime.class) {
            return new Date(Time.valueOf((LocalTime) val).getTime());
        } else if (cls == Instant.class) {
            var odlDate = java.util.Date.from((Instant) val);
            return new Date(odlDate.getTime());
        } else if (cls == LocalDateTime.class) {
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

        Class<?> cls = val.getClass();

        if (cls == LocalTime.class) {
            return Time.valueOf((LocalTime) val);
        } else if (cls == LocalDate.class) {
            return new Time(Date.valueOf((LocalDate) val).getTime());
        } else if (cls == Instant.class) {
            var oldTs = Timestamp.from((Instant) val);
            return new Time(oldTs.getTime());
        } else if (cls == LocalDateTime.class) {
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

        Class<?> cls = val.getClass();

        if (cls == LocalTime.class) {
            return new Timestamp(Time.valueOf((LocalTime) val).getTime());
        } else if (cls == LocalDate.class) {
            return new Timestamp(Date.valueOf((LocalDate) val).getTime());
        } else if (cls == Instant.class) {
            return Timestamp.from(((Instant) val));
        } else if (cls == LocalDateTime.class) {
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

        if (jdbcMeta == null) {
            jdbcMeta = new JdbcResultSetMetadata(meta());
        }

        return jdbcMeta;
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

        return curPos == 0 && rowsIter != null && rowsIter.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isAfterLast() throws SQLException {
        ensureNotClosed();

        return finished && rowsIter == null && curRow == null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isFirst() throws SQLException {
        ensureNotClosed();

        return curPos == 1;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isLast() throws SQLException {
        ensureNotClosed();

        return finished && rowsIter != null && !rowsIter.hasNext() && curRow != null;
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

        return isAfterLast() ? 0 : curPos;
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

        return stmt.getResultSetType();
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

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNull(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBoolean(int colIdx, boolean x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBoolean(String colLb, boolean x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateByte(int colIdx, byte x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateByte(String colLb, byte x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateShort(int colIdx, short x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateShort(String colLb, short x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateInt(int colIdx, int x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateInt(String colLb, int x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateLong(int colIdx, long x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateLong(String colLb, long x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateFloat(int colIdx, float x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateFloat(String colLb, float x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateDouble(int colIdx, double x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateDouble(String colLb, double x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBigDecimal(int colIdx, BigDecimal x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBigDecimal(String colLb, BigDecimal x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateString(int colIdx, String x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateString(String colLb, String x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBytes(int colIdx, byte[] x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBytes(String colLb, byte[] x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateDate(int colIdx, Date x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateDate(String colLb, Date x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateTime(int colIdx, Time x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateTime(String colLb, Time x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateTimestamp(int colIdx, Timestamp x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateTimestamp(String colLb, Timestamp x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(int colIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(String colLb, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(int colIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(String colLb, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(int colIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateAsciiStream(String colLb, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(int colIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(int colIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(String colLb, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(int colIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(String colLb, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBinaryStream(String colLb, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(int colIdx, Reader x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(String colLb, Reader reader, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(int colIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(int colIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateCharacterStream(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateObject(int colIdx, Object x, int scaleOrLen) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateObject(int colIdx, Object x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateObject(String colLb, Object x, int scaleOrLen) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateObject(String colLb, Object x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void insertRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void deleteRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
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

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
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

        return stmt;
    }

    /** {@inheritDoc} */
    @Override
    public Object getObject(int colIdx, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQL structured type are not supported.");
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
        throw new SQLFeatureNotSupportedException("SQL structured type are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Ref getRef(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Ref getRef(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Blob getBlob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Blob getBlob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Clob getClob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Clob getClob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Array getArray(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Array getArray(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public URL getURL(int colIdx) throws SQLException {
        Object val = getValue(colIdx);

        if (val == null) {
            return null;
        }

        Class<?> cls = val.getClass();

        if (cls == URL.class) {
            return (URL) val;
        } else if (cls == String.class) {
            try {
                return new URL(val.toString());
            } catch (MalformedURLException e) {
                throw new SQLException("Cannot convert to URL: " + val, SqlStateCode.CONVERSION_FAILED, e);
            }
        } else {
            throw new SQLException("Cannot convert to URL: " + val, SqlStateCode.CONVERSION_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override
    public URL getURL(String colLb) throws SQLException {
        int colIdx = findColumn(colLb);

        return getURL(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public void updateRef(int colIdx, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateRef(String colLb, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(int colIdx, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(String colLb, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(int colIdx, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(String colLb, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(int colIdx, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateBlob(String colLb, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(int colIdx, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(String colLb, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(int colIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(int colIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateClob(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateArray(int colIdx, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateArray(String colLb, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public RowId getRowId(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public RowId getRowId(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateRowId(int colIdx, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateRowId(String colLb, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
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
        return closed || stmt == null || stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override
    public void updateNString(int colIdx, String val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNString(String colLb, String val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(int colIdx, NClob val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(String colLb, NClob val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(int colIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(int colIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNClob(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public NClob getNClob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public NClob getNClob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public SQLXML getSQLXML(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public SQLXML getSQLXML(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateSQLXML(int colIdx, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateSQLXML(String colLb, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
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

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Reader getNCharacterStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNCharacterStream(int colIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNCharacterStream(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNCharacterStream(int colIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public void updateNCharacterStream(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
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
     * Get the isQuery flag.
     *
     * @return Is query flag.
     */
    public boolean isQuery() {
        return isQuery;
    }

    /**
     * Gets object field value by index.
     *
     * @param colIdx Column index.
     * @return Object field value.
     * @throws SQLException In case of error.
     */
    private Object getValue(int colIdx) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        try {
            Object val = curRow.get(colIdx - 1);

            wasNull = val == null;

            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Invalid column index: " + colIdx, SqlStateCode.PARSING_EXCEPTION, e);
        }
    }

    /**
     * Ensures that result set is positioned on a row.
     *
     * @throws SQLException If result set is not positioned on a row.
     */
    private void ensureHasCurrentRow() throws SQLException {
        if (curRow == null) {
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
     * Get the update count.
     *
     * @return Update count for no-SELECT queries.
     */
    public long updatedCount() {
        return updCnt;
    }

    /**
     * Set the close statement flag.
     *
     * @param closeStmt Close statement on this result set close.
     */
    public void closeStatement(boolean closeStmt) {
        this.closeStmt = closeStmt;
    }

    /**
     * Get object of given class.
     *
     * @param colIdx    Column index.
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
        } else if (targetCls == URL.class) {
            return getURL(colIdx);
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
        if (colOrder != null) {
            return colOrder;
        }

        if (!metaInit) {
            meta();
        }

        initColumnOrder();

        return colOrder;
    }

    /**
     * Init column order map.
     */
    private void initColumnOrder() {
        colOrder = new HashMap<>(meta.size());

        for (int i = 0; i < meta.size(); ++i) {
            String colName = meta.get(i).columnLabel().toUpperCase();

            if (!colOrder.containsKey(colName)) {
                colOrder.put(colName, i);
            }
        }
    }

    /**
     * Returns columns metadata list.
     *
     * @return Results metadata.
     * @throws SQLException On error.
     */
    private List<JdbcColumnMeta> meta() throws SQLException {
        if (finished && (!isQuery || autoClose)) {
            throw new SQLException("Server cursor is already closed.", SqlStateCode.INVALID_CURSOR_STATE);
        }

        if (!metaInit) {
            JdbcMetaColumnsResult res = qryHandler.queryMetadataAsync(new JdbcQueryMetadataRequest(cursorId)).join();

            meta = res.meta();

            metaInit = true;
        }

        return meta;
    }
}
