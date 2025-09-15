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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Compatibility tests to ensure that both JDBC result set adapters behave identically for core java.sql.ResultSet methods (excluding
 * metadata checks).
 */
public abstract class JdbcResultSetBaseSelfTest extends BaseIgniteAbstractTest {

    protected abstract ResultSet createResultSet(
            @Nullable ZoneId zoneId,
            List<ColumnDefinition> cols,
            List<List<Object>> rows
    ) throws SQLException;

    private ResultSet createSingleRow(ColumnDefinition col, @Nullable Object value) throws SQLException {
        return createMultiRow(new ColumnDefinition[]{col}, new Object[]{value});
    }

    private ResultSet createMultiRow(@Nullable ZoneId zoneId, ColumnDefinition[] columns, Object[] values) throws SQLException {
        if (columns.length != values.length) {
            throw new IllegalArgumentException("cols, values must have the same length");
        }

        List<ColumnDefinition> cols = Arrays.asList(columns);
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.asList(values));

        return createResultSet(zoneId, cols, rows);
    }

    private ResultSet createMultiRow(ColumnDefinition[] columns, Object[] values) throws SQLException {
        return createMultiRow(null, columns, values);
    }

    @Test
    public void getNotSupportedTypes() throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 3, 0, false), "ABC")) {
            assertTrue(rs.next());

            expectNotSupported(() -> rs.getArray(1));
            expectNotSupported(() -> rs.getArray("C"));

            expectNotSupported(() -> rs.getAsciiStream(1));
            expectNotSupported(() -> rs.getAsciiStream("C"));

            expectNotSupported(() -> rs.getBinaryStream(1));
            expectNotSupported(() -> rs.getBinaryStream("C"));

            expectNotSupported(() -> rs.getBlob(1));
            expectNotSupported(() -> rs.getBlob("C"));

            expectNotSupported(() -> rs.getClob(1));
            expectNotSupported(() -> rs.getClob("C"));

            expectNotSupported(() -> rs.getCharacterStream(1));
            expectNotSupported(() -> rs.getCharacterStream("C"));

            expectNotSupported(() -> rs.getNCharacterStream(1));
            expectNotSupported(() -> rs.getNCharacterStream("C"));

            expectNotSupported(() -> rs.getNClob(1));
            expectNotSupported(() -> rs.getNClob("C"));

            expectNotSupported(() -> rs.getRef(1));
            expectNotSupported(() -> rs.getRef("C"));

            expectNotSupported(() -> rs.getRowId(1));
            expectNotSupported(() -> rs.getRowId("C"));

            expectNotSupported(() -> rs.getSQLXML(1));
            expectNotSupported(() -> rs.getSQLXML("C"));

            expectNotSupported(() -> rs.getURL(1));
            expectNotSupported(() -> rs.getURL("C"));

            expectNotSupported(() -> rs.getObject(1, Map.of()));
            expectNotSupported(() -> rs.getObject("C", Map.of()));
        }
    }

    @Test
    public void getNotImplemented() throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 3, 0, false), "ABC")) {

            expectNotSupported(() -> rs.getAsciiStream(1));
            expectNotSupported(() -> rs.getAsciiStream("C"));

            expectNotSupported(() -> rs.getBinaryStream(1));
            expectNotSupported(() -> rs.getBinaryStream("C"));

            expectNotSupported(() -> rs.getCharacterStream(1));
            expectNotSupported(() -> rs.getCharacterStream("C"));
        }
    }

    @Test
    public void notSupportedMethods() throws Exception {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 3, 0, false), "ABC")) {
            assertTrue(rs.next());

            expectNotSupported(rs::rowInserted);
            expectNotSupported(rs::rowUpdated);
            expectNotSupported(rs::rowDeleted);

            expectNotSupported(() -> rs.updateBoolean(1, true));
            expectNotSupported(() -> rs.updateBoolean("C", true));

            expectNotSupported(() -> rs.updateByte(1, (byte) 0));
            expectNotSupported(() -> rs.updateByte("C", (byte) 0));

            expectNotSupported(() -> rs.updateShort(1, (short) 0));
            expectNotSupported(() -> rs.updateShort("C", (short) 0));

            expectNotSupported(() -> rs.updateInt(1, 0));
            expectNotSupported(() -> rs.updateInt("C", 0));

            expectNotSupported(() -> rs.updateLong(1, 0));
            expectNotSupported(() -> rs.updateLong("C", 0));

            expectNotSupported(() -> rs.updateFloat(1, 0.0f));
            expectNotSupported(() -> rs.updateFloat("C", 0.0f));

            expectNotSupported(() -> rs.updateDouble(1, 0.0));
            expectNotSupported(() -> rs.updateDouble("C", 0.0));

            expectNotSupported(() -> rs.updateString(1, ""));
            expectNotSupported(() -> rs.updateString("C", ""));

            expectNotSupported(() -> rs.updateTime(1, new Time(0)));
            expectNotSupported(() -> rs.updateTime("C", new Time(0)));

            expectNotSupported(() -> rs.updateDate(1, new Date(0)));
            expectNotSupported(() -> rs.updateDate("C", new Date(0)));

            expectNotSupported(() -> rs.updateTimestamp(1, new Timestamp(0)));
            expectNotSupported(() -> rs.updateTimestamp("C", new Timestamp(0)));

            expectNotSupported(() -> rs.updateBytes(1, new byte[]{1}));
            expectNotSupported(() -> rs.updateBytes("C", new byte[]{1}));

            expectNotSupported(() -> rs.updateArray(1, null));
            expectNotSupported(() -> rs.updateArray("C", null));

            expectNotSupported(() -> rs.updateBlob(1, (Blob) null));
            expectNotSupported(() -> rs.updateBlob(1, (InputStream) null));
            expectNotSupported(() -> rs.updateBlob(1, null, 0L));
            expectNotSupported(() -> rs.updateBlob("C", (Blob) null));
            expectNotSupported(() -> rs.updateBlob("C", (InputStream) null));
            expectNotSupported(() -> rs.updateBlob("C", null, 0L));

            expectNotSupported(() -> rs.updateClob(1, (Clob) null));
            expectNotSupported(() -> rs.updateClob(1, (Reader) null));
            expectNotSupported(() -> rs.updateClob(1, null, 0L));
            expectNotSupported(() -> rs.updateClob("C", (Clob) null));
            expectNotSupported(() -> rs.updateClob("C", (Reader) null));
            expectNotSupported(() -> rs.updateClob("C", null, 0L));

            expectNotSupported(() -> rs.updateNClob(1, (NClob) null));
            expectNotSupported(() -> rs.updateNClob(1, (Reader) null));
            expectNotSupported(() -> rs.updateNClob(1, null, 0L));
            expectNotSupported(() -> rs.updateNClob("C", (NClob) null));
            expectNotSupported(() -> rs.updateNClob("C", (Reader) null));
            expectNotSupported(() -> rs.updateNClob("C", null, 0L));

            expectNotSupported(() -> rs.updateAsciiStream(1, null));
            expectNotSupported(() -> rs.updateAsciiStream(1, null, 0));
            expectNotSupported(() -> rs.updateAsciiStream(1, null, 0L));
            expectNotSupported(() -> rs.updateAsciiStream("C", null));
            expectNotSupported(() -> rs.updateAsciiStream("C", null, 0));
            expectNotSupported(() -> rs.updateAsciiStream("C", null, 0L));

            expectNotSupported(() -> rs.updateBinaryStream(1, null));
            expectNotSupported(() -> rs.updateBinaryStream(1, null, 0));
            expectNotSupported(() -> rs.updateBinaryStream(1, null, 0L));
            expectNotSupported(() -> rs.updateBinaryStream("C", null));
            expectNotSupported(() -> rs.updateBinaryStream("C", null, 0));
            expectNotSupported(() -> rs.updateBinaryStream("C", null, 0L));

            expectNotSupported(() -> rs.updateCharacterStream(1, null));
            expectNotSupported(() -> rs.updateCharacterStream(1, null, 0));
            expectNotSupported(() -> rs.updateCharacterStream(1, null, 0L));
            expectNotSupported(() -> rs.updateCharacterStream("C", null));
            expectNotSupported(() -> rs.updateCharacterStream("C", null, 0));
            expectNotSupported(() -> rs.updateCharacterStream("C", null, 0L));

            expectNotSupported(() -> rs.updateNCharacterStream(1, null));
            expectNotSupported(() -> rs.updateNCharacterStream(1, null, 0));
            expectNotSupported(() -> rs.updateNCharacterStream(1, null, 0L));
            expectNotSupported(() -> rs.updateNCharacterStream("C", null));
            expectNotSupported(() -> rs.updateNCharacterStream("C", null, 0));
            expectNotSupported(() -> rs.updateNCharacterStream("C", null, 0L));

            expectNotSupported(() -> rs.updateRef(1, null));
            expectNotSupported(() -> rs.updateRef("C", null));

            expectNotSupported(() -> rs.updateRowId(1, null));
            expectNotSupported(() -> rs.updateRowId("C", null));

            expectNotSupported(() -> rs.updateNString(1, null));
            expectNotSupported(() -> rs.updateNString("C", null));

            expectNotSupported(() -> rs.updateSQLXML(1, null));
            expectNotSupported(() -> rs.updateSQLXML("C", null));

            expectNotSupported(() -> rs.updateObject(1, null));
            expectNotSupported(() -> rs.updateObject(1, null, 0));
            expectNotSupported(() -> rs.updateObject("C", null));
            expectNotSupported(() -> rs.updateObject("C", null, 0));

            expectNotSupported(() -> rs.updateBigDecimal(1, null));
            expectNotSupported(() -> rs.updateBigDecimal("C", null));

            expectNotSupported(() -> rs.updateNull(1));
            expectNotSupported(() -> rs.updateNull("C"));

            expectNotSupported(rs::cancelRowUpdates);

            expectNotSupported(rs::updateRow);

            expectNotSupported(rs::deleteRow);

            expectNotSupported(rs::insertRow);

            expectNotSupported(rs::moveToInsertRow);

            expectNotSupported(rs::getCursorName);
        }
    }

    @Test
    public void navigationMethods() throws SQLException {
        // Empty result set
        try (ResultSet rs = createResultSet(null,
                List.of(new ColumnDefinition("C", ColumnType.STRING, 3, 0, false)),
                List.of())
        ) {
            assertFalse(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(0, rs.getRow());

            assertFalse(rs.next());

            assertFalse(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(0, rs.getRow());
        }

        // Non empty result set
        try (ResultSet rs = createResultSet(null,
                List.of(new ColumnDefinition("C", ColumnType.STRING, 3, 0, false)),
                List.of(List.of("A"), List.of("B")))
        ) {
            assertTrue(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(0, rs.getRow());

            assertTrue(rs.next());

            assertFalse(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
            assertTrue(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(1, rs.getRow());

            assertTrue(rs.next());

            assertFalse(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertTrue(rs.isLast());
            assertEquals(2, rs.getRow());

            assertFalse(rs.next());

            assertFalse(rs.isBeforeFirst());
            assertTrue(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(0, rs.getRow());
        }

        try (ResultSet rs = createResultSet(null,
                List.of(new ColumnDefinition("C", ColumnType.STRING, 3, 0, false)),
                List.of(List.of("A"), List.of("B")))
        ) {
            String error = "Result set is forward-only.";

            expectSqlException(rs::first, error);
            expectSqlException(rs::afterLast, error);
            expectSqlException(rs::last, error);

            assertTrue(rs.next());

            expectSqlException(rs::first, error);
            expectSqlException(rs::afterLast, error);
            expectSqlException(rs::last, error);

            expectSqlException(() -> rs.absolute(0), error);
            expectSqlException(() -> rs.relative(0), error);
            expectSqlException(rs::previous, error);

            assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
            rs.setFetchDirection(ResultSet.FETCH_FORWARD);
            assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());

            expectSqlException(() -> rs.setFetchDirection(ResultSet.FETCH_UNKNOWN),
                    "Only forward direction is supported");
            expectSqlException(() -> rs.setFetchDirection(ResultSet.FETCH_REVERSE),
                    "Only forward direction is supported");

            assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());

            expectSqlException(rs::moveToCurrentRow, "The result set concurrency is CONCUR_READ_ONLY");
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"PERIOD", "DURATION"}, mode = EnumSource.Mode.EXCLUDE)
    public void wasNull(ColumnType columnType) throws SQLException {
        Object value;
        switch (columnType) {
            case NULL:
                value = null;
                break;
            case BOOLEAN:
                value = true;
                break;
            case INT8:
                value = (byte) 1;
                break;
            case INT16:
                value = (short) 1;
                break;
            case INT32:
                value = 1;
                break;
            case INT64:
                value = 1L;
                break;
            case FLOAT:
                value = 0.0f;
                break;
            case DOUBLE:
                value = 0.0d;
                break;
            case DECIMAL:
                value = BigDecimal.ZERO;
                break;
            case DATE:
                value = LocalDate.now();
                break;
            case TIME:
                value = LocalTime.now();
                break;
            case DATETIME:
                value = LocalDateTime.now();
                break;
            case TIMESTAMP:
                value = Instant.now();
                break;
            case UUID:
                value = new UUID(1, 1);
                break;
            case STRING:
                value = "";
                break;
            case BYTE_ARRAY:
                value = new byte[0];
                break;
            case PERIOD:
            case DURATION:
            default:
                throw new IllegalArgumentException("Unexpected type: " + columnType);
        }

        try (ResultSet rs = createResultSet(null,
                List.of(new ColumnDefinition("C", columnType, 0, 0, true)),
                List.of(Collections.singletonList(value),
                        Collections.singletonList(null),
                        Collections.singletonList(value)
                )
        )) {
            // Allow to access wasNull for non-positioned result set.
            assertFalse(rs.wasNull());

            // First row
            assertTrue(rs.next());
            // Allow to access wasNull, if no getter was set as well.
            assertFalse(rs.wasNull());

            // Read a column, so we can call wasNull
            Object val1 = rs.getObject(1);
            assertEquals(val1 == null, rs.wasNull());

            // Second row
            assertTrue(rs.next());
            // Result should not change
            assertEquals(val1 == null, rs.wasNull());

            Object val2 = rs.getObject(1);
            assertNull(val2);
            assertTrue(rs.wasNull());

            // Third row
            assertTrue(rs.next());
            // Result should not change
            assertTrue(rs.wasNull());

            Object val3 = rs.getObject(1);
            assertEquals(val3 == null, rs.wasNull());

            // Move past the last row
            assertFalse(rs.next());
            // Result should not change
            assertEquals(val3 == null, rs.wasNull());
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"PERIOD", "DURATION"}, mode = EnumSource.Mode.EXCLUDE)
    public void wasNullPositional(ColumnType columnType) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), null)) {
            assertTrue(rs.next());

            switch (columnType) {
                case NULL:
                    assertNull(rs.getObject(1));
                    assertTrue(rs.wasNull());
                    break;
                case BOOLEAN:
                    assertFalse(rs.getBoolean(1));
                    assertTrue(rs.wasNull());
                    break;
                case INT8:
                    assertEquals(0, rs.getByte(1));
                    assertTrue(rs.wasNull());
                    break;
                case INT16:
                    assertEquals(0, rs.getShort(1));
                    assertTrue(rs.wasNull());
                    break;
                case INT32:
                    assertEquals(0, rs.getInt(1));
                    assertTrue(rs.wasNull());
                    break;
                case INT64:
                    assertEquals(0, rs.getLong(1));
                    assertTrue(rs.wasNull());
                    break;
                case FLOAT:
                    assertEquals(0, rs.getFloat(1));
                    assertTrue(rs.wasNull());
                    break;
                case DOUBLE:
                    assertEquals(0, rs.getDouble(1));
                    assertTrue(rs.wasNull());
                    break;
                case DECIMAL:
                    assertNull(rs.getBigDecimal(1));
                    assertTrue(rs.wasNull());
                    break;
                case DATE:
                    assertNull(rs.getDate(1));
                    assertTrue(rs.wasNull());
                    break;
                case TIME:
                    assertNull(rs.getTime(1));
                    assertTrue(rs.wasNull());
                    break;
                case DATETIME:
                    assertNull(rs.getTimestamp(1));
                    assertTrue(rs.wasNull());
                    break;
                case TIMESTAMP:
                    assertNull(rs.getTimestamp(1));
                    assertTrue(rs.wasNull());
                    break;
                case UUID:
                    assertNull(rs.getObject(1, UUID.class));
                    assertTrue(rs.wasNull());
                    break;
                case STRING:
                    assertNull(rs.getString(1));
                    assertTrue(rs.wasNull());
                    break;
                case BYTE_ARRAY:
                    assertNull(rs.getBytes(1));
                    assertTrue(rs.wasNull());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected type: " + columnType);
            }

            assertFalse(rs.next());
            assertTrue(rs.wasNull());
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"PERIOD", "DURATION"}, mode = EnumSource.Mode.EXCLUDE)
    public void wasNullNamed(ColumnType columnType) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), null)) {
            assertTrue(rs.next());

            switch (columnType) {
                case NULL:
                    assertNull(rs.getObject("C"));
                    assertTrue(rs.wasNull());
                    break;
                case BOOLEAN:
                    assertFalse(rs.getBoolean("C"));
                    assertTrue(rs.wasNull());
                    break;
                case INT8:
                    assertEquals(0, rs.getByte("C"));
                    assertTrue(rs.wasNull());
                    break;
                case INT16:
                    assertEquals(0, rs.getShort("C"));
                    assertTrue(rs.wasNull());
                    break;
                case INT32:
                    assertEquals(0, rs.getInt("C"));
                    assertTrue(rs.wasNull());
                    break;
                case INT64:
                    assertEquals(0, rs.getLong("C"));
                    assertTrue(rs.wasNull());
                    break;
                case FLOAT:
                    assertEquals(0, rs.getFloat("C"));
                    assertTrue(rs.wasNull());
                    break;
                case DOUBLE:
                    assertEquals(0, rs.getDouble("C"));
                    assertTrue(rs.wasNull());
                    break;
                case DECIMAL:
                    assertNull(rs.getBigDecimal("C"));
                    assertTrue(rs.wasNull());
                    break;
                case DATE:
                    assertNull(rs.getDate("C"));
                    assertTrue(rs.wasNull());
                    break;
                case TIME:
                    assertNull(rs.getTime("C"));
                    assertTrue(rs.wasNull());
                    break;
                case DATETIME:
                    assertNull(rs.getTimestamp("C"));
                    assertTrue(rs.wasNull());
                    break;
                case TIMESTAMP:
                    assertNull(rs.getTimestamp("C"));
                    assertTrue(rs.wasNull());
                    break;
                case UUID:
                    assertNull(rs.getObject(1, UUID.class));
                    assertTrue(rs.wasNull());
                    break;
                case STRING:
                    assertNull(rs.getString("C"));
                    assertTrue(rs.wasNull());
                    break;
                case BYTE_ARRAY:
                    assertNull(rs.getBytes("C"));
                    assertTrue(rs.wasNull());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected type: " + columnType);
            }

            assertFalse(rs.next());
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void getUnknownColumn() throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), 1)) {
            assertTrue(rs.next());

            expectInvalidColumn(rs::getBoolean, -1);
            expectInvalidColumn(rs::getBoolean, 2);
            expectInvalidColumn(rs::getBoolean, "X");

            expectInvalidColumn(rs::getByte, -1);
            expectInvalidColumn(rs::getByte, 2);
            expectInvalidColumn(rs::getByte, "X");

            expectInvalidColumn(rs::getShort, -1);
            expectInvalidColumn(rs::getShort, 2);
            expectInvalidColumn(rs::getShort, "X");

            expectInvalidColumn(rs::getInt, -1);
            expectInvalidColumn(rs::getInt, 2);
            expectInvalidColumn(rs::getInt, "X");

            expectInvalidColumn(rs::getLong, -1);
            expectInvalidColumn(rs::getLong, 2);
            expectInvalidColumn(rs::getLong, "X");

            expectInvalidColumn(rs::getFloat, -1);
            expectInvalidColumn(rs::getFloat, 2);
            expectInvalidColumn(rs::getFloat, "X");

            expectInvalidColumn(rs::getDouble, -1);
            expectInvalidColumn(rs::getDouble, 2);
            expectInvalidColumn(rs::getDouble, "X");

            expectInvalidColumn(rs::getBigDecimal, -1);
            expectInvalidColumn(rs::getBigDecimal, 2);
            expectInvalidColumn(rs::getBigDecimal, "X");

            expectInvalidColumn(rs::getDate, -1);
            expectInvalidColumn(rs::getDate, 2);
            expectInvalidColumn(rs::getDate, "X");

            expectInvalidColumn(rs::getTime, -1);
            expectInvalidColumn(rs::getTime, 2);
            expectInvalidColumn(rs::getTime, "X");

            expectInvalidColumn(rs::getTimestamp, -1);
            expectInvalidColumn(rs::getTimestamp, 2);
            expectInvalidColumn(rs::getTimestamp, "X");

            expectInvalidColumn(rs::getString, -1);
            expectInvalidColumn(rs::getString, 2);
            expectInvalidColumn(rs::getString, "X");

            expectInvalidColumn(rs::getNString, -1);
            expectInvalidColumn(rs::getNString, 2);
            expectInvalidColumn(rs::getNString, "X");

            expectInvalidColumn(rs::getObject, -1);
            expectInvalidColumn(rs::getObject, 2);
            expectInvalidColumn(rs::getObject, "X");

            expectSqlException(() -> rs.getObject(-1, Integer.class), "Invalid column index: -1");
            expectSqlException(() -> rs.getObject(2, Integer.class), "Invalid column index: 2");
            expectSqlException(() -> rs.getObject("X", Integer.class), "Column not found: X");

            expectSqlException(() -> rs.getObject(-1, Map.of()), "SQL structured type are not supported");
            expectSqlException(() -> rs.getObject(2, Map.of()), "SQL structured type are not supported");
            expectSqlException(() -> rs.getObject("X", Map.of()), "SQL structured type are not supported");
        }
    }

    @Test
    public void methodsArePositioned() throws SQLException {
        try (ResultSet rs = createResultSet(null,
                List.of(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false)),
                List.of())
        ) {
            expectPositioned(() -> rs.getBoolean(1));
            expectPositioned(() -> rs.getBoolean("C"));

            expectPositioned(() -> rs.getByte(1));
            expectPositioned(() -> rs.getByte("C"));

            expectPositioned(() -> rs.getShort(1));
            expectPositioned(() -> rs.getShort("C"));

            expectPositioned(() -> rs.getInt(1));
            expectPositioned(() -> rs.getInt("C"));

            expectPositioned(() -> rs.getLong(1));
            expectPositioned(() -> rs.getLong("C"));

            expectPositioned(() -> rs.getFloat(1));
            expectPositioned(() -> rs.getFloat("C"));

            expectPositioned(() -> rs.getDouble(1));
            expectPositioned(() -> rs.getDouble("C"));

            expectPositioned(() -> rs.getBigDecimal(1));
            expectPositioned(() -> rs.getBigDecimal("C"));

            //noinspection deprecation
            expectPositioned(() -> rs.getBigDecimal(1, 2));
            //noinspection deprecation
            expectPositioned(() -> rs.getBigDecimal("C", 3));

            expectPositioned(() -> rs.getDate(1));
            expectPositioned(() -> rs.getDate("C"));

            expectPositioned(() -> rs.getTime(1));
            expectPositioned(() -> rs.getTime("C"));

            expectPositioned(() -> rs.getTimestamp(1));
            expectPositioned(() -> rs.getTimestamp("C"));

            expectPositioned(() -> rs.getString(1));
            expectPositioned(() -> rs.getString("C"));

            expectPositioned(() -> rs.getNString(1));
            expectPositioned(() -> rs.getNString("C"));

            expectPositioned(() -> rs.getObject(1));
            expectPositioned(() -> rs.getObject("C"));

            expectPositioned(() -> rs.getObject(1, Boolean.class));
            expectPositioned(() -> rs.getObject("C", Boolean.class));

            expectPositioned(() -> rs.getBytes(1));
            expectPositioned(() -> rs.getBytes("C"));

            // Do not require positioning
            assertThat(rs.getType(), any(Integer.class));

            assertThat(rs.getConcurrency(), any(Integer.class));

            assertThat(rs.getFetchDirection(), any(Integer.class));

            assertThat(rs.getFetchSize(), any(Integer.class));

            assertThat(rs.getRow(), any(Integer.class));

            assertThat(rs.isClosed(), any(Boolean.class));

            assertThat(rs.getHoldability(), any(Integer.class));
        }
    }

    @Test
    public void warnings() throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), true)) {
            assertNull(rs.getWarnings());

            rs.clearWarnings();
            assertNull(rs.getWarnings());
        }
    }

    @Test
    public void getMetadata() throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), true)) {
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals(1, metaData.getColumnCount());
        }
    }

    @Test
    public void findColumn() throws SQLException {
        List<ColumnDefinition> columns = List.of(
                // Normalized - converted to uppercase
                new ColumnDefinition("COLUMN", ColumnType.BOOLEAN, 0, 0, false),
                // Metadata stores ids in unquoted form
                new ColumnDefinition("column", ColumnType.BOOLEAN, 0, 0, false),
                new ColumnDefinition("Column N", ColumnType.BOOLEAN, 0, 0, false),
                new ColumnDefinition(" ", ColumnType.BOOLEAN, 0, 0, false),
                new ColumnDefinition(":)", ColumnType.BOOLEAN, 0, 0, false)
        );

        List<List<Object>> rows = IntStream.range(0, columns.size())
                .mapToObj(i -> List.of((Object) true))
                .collect(Collectors.toList());

        try (ResultSet rs = createResultSet(null, columns, rows)) {
            assertEquals(1, rs.findColumn("COLUMN"));
            assertEquals(2, rs.findColumn("\"column\""));
            assertEquals(3, rs.findColumn("\"Column N\""));
            assertEquals(4, rs.findColumn("\" \""));
            assertEquals(5, rs.findColumn("\":)\""));

            expectSqlException(() -> rs.findColumn(" COLUMN "), "Column not found:  COLUMN ");
            expectSqlException(() -> rs.findColumn(" column "), "Column not found:  column ");
            expectSqlException(() -> rs.findColumn("x"), "Column not found: x");
            expectSqlException(() -> rs.findColumn(null), "Column not found: null");
            expectSqlException(() -> rs.findColumn(""), "Column not found: ");
        }
    }

    @Test
    public void close() throws SQLException {
        try (ResultSet rs = createResultSet(null,
                List.of(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false)),
                List.of(List.of())
        )) {
            assertFalse(rs.isClosed());

            rs.close();

            assertTrue(rs.isClosed());

            // Navigation
            expectClosed(rs::next);
            expectClosed(rs::previous);
            expectClosed(rs::first);
            expectClosed(rs::last);
            expectClosed(rs::beforeFirst);
            expectClosed(rs::afterLast);
            expectClosed(() -> rs.absolute(1));
            expectClosed(() -> rs.relative(1));

            // Position/state queries and settings
            expectClosed(rs::isBeforeFirst);
            expectClosed(rs::isAfterLast);
            expectClosed(rs::isFirst);
            expectClosed(rs::isLast);
            expectClosed(rs::getRow);
            expectClosed(() -> rs.setFetchDirection(ResultSet.FETCH_FORWARD));
            expectClosed(rs::getFetchDirection);
            expectClosed(() -> rs.setFetchSize(1));
            expectClosed(rs::getFetchSize);
            expectClosed(rs::getType);
            expectClosed(rs::getConcurrency);
            expectClosed(rs::rowUpdated);
            expectClosed(rs::rowInserted);
            expectClosed(rs::rowDeleted);
            expectClosed(rs::getHoldability);

            // Metadata and lookup
            expectClosed(rs::getMetaData);
            expectClosed(() -> rs.findColumn("C"));

            // Warnings and cursor name
            expectClosed(rs::getWarnings);
            expectClosed(rs::clearWarnings);
            expectClosed(rs::getCursorName);

            // Statement
            expectClosed(rs::getStatement);

            // Read methods
            expectClosed(() -> rs.getBoolean(1));
            expectClosed(() -> rs.getBoolean("C"));

            expectClosed(() -> rs.getByte(1));
            expectClosed(() -> rs.getByte("C"));

            expectClosed(() -> rs.getShort(1));
            expectClosed(() -> rs.getShort("C"));

            expectClosed(() -> rs.getInt(1));
            expectClosed(() -> rs.getInt("C"));

            expectClosed(() -> rs.getLong(1));
            expectClosed(() -> rs.getLong("C"));

            expectClosed(() -> rs.getFloat(1));
            expectClosed(() -> rs.getFloat("C"));

            expectClosed(() -> rs.getDouble(1));
            expectClosed(() -> rs.getDouble("C"));

            expectClosed(() -> rs.getBigDecimal(1));
            expectClosed(() -> rs.getBigDecimal("C"));

            //noinspection deprecation
            expectClosed(() -> rs.getBigDecimal(1, 2));
            //noinspection deprecation
            expectClosed(() -> rs.getBigDecimal("C", 3));

            expectClosed(() -> rs.getDate(1));
            expectClosed(() -> rs.getDate("C"));
            expectClosed(() -> rs.getDate(1, Calendar.getInstance()));
            expectClosed(() -> rs.getDate("C", Calendar.getInstance()));

            expectClosed(() -> rs.getTime(1));
            expectClosed(() -> rs.getTime("C"));

            expectClosed(() -> rs.getTime(1, Calendar.getInstance()));
            expectClosed(() -> rs.getTime("C", Calendar.getInstance()));

            expectClosed(() -> rs.getTimestamp(1));
            expectClosed(() -> rs.getTimestamp("C"));

            expectClosed(() -> rs.getTimestamp(1, Calendar.getInstance()));
            expectClosed(() -> rs.getTimestamp("C", Calendar.getInstance()));

            expectClosed(() -> rs.getString(1));
            expectClosed(() -> rs.getString("C"));

            expectClosed(() -> rs.getNString(1));
            expectClosed(() -> rs.getNString("C"));

            expectClosed(() -> rs.getObject(1));
            expectClosed(() -> rs.getObject("C"));

            expectClosed(() -> rs.getObject(1, Boolean.class));
            expectClosed(() -> rs.getObject("C", Boolean.class));

            expectClosed(() -> rs.getBytes(1));
            expectClosed(() -> rs.getBytes("C"));

            expectClosed(() -> rs.getURL(1));
            expectClosed(() -> rs.getURL("C"));

            expectClosed(() -> rs.getAsciiStream(1));
            expectClosed(() -> rs.getAsciiStream("C"));

            //noinspection deprecation
            expectClosed(() -> rs.getUnicodeStream(1));
            //noinspection deprecation
            expectClosed(() -> rs.getUnicodeStream("C"));

            expectClosed(() -> rs.getBinaryStream(1));
            expectClosed(() -> rs.getBinaryStream("C"));

            expectClosed(() -> rs.getCharacterStream(1));
            expectClosed(() -> rs.getCharacterStream("C"));

            expectClosed(() -> rs.getNCharacterStream(1));
            expectClosed(() -> rs.getNCharacterStream("C"));

            expectClosed(() -> rs.getClob(1));
            expectClosed(() -> rs.getClob("C"));

            expectClosed(() -> rs.getBlob(1));
            expectClosed(() -> rs.getBlob("C"));

            expectClosed(() -> rs.getRef(1));
            expectClosed(() -> rs.getRef("C"));

            expectClosed(() -> rs.getArray(1));
            expectClosed(() -> rs.getArray("C"));

            expectClosed(() -> rs.getRowId(1));
            expectClosed(() -> rs.getRowId("C"));

            expectClosed(() -> rs.getNClob(1));
            expectClosed(() -> rs.getNClob("C"));

            expectClosed(() -> rs.getSQLXML(1));
            expectClosed(() -> rs.getSQLXML("C"));

            expectClosed(rs::wasNull);

            // Update methods

            expectClosed(() -> rs.updateBoolean(1, true));
            expectClosed(() -> rs.updateBoolean("C", true));

            expectClosed(() -> rs.updateByte(1, (byte) 0));
            expectClosed(() -> rs.updateByte("C", (byte) 0));

            expectClosed(() -> rs.updateShort(1, (short) 0));
            expectClosed(() -> rs.updateShort("C", (short) 0));

            expectClosed(() -> rs.updateInt(1, 0));
            expectClosed(() -> rs.updateInt("C", 0));

            expectClosed(() -> rs.updateLong(1, 0));
            expectClosed(() -> rs.updateLong("C", 0));

            expectClosed(() -> rs.updateFloat(1, 0.0f));
            expectClosed(() -> rs.updateFloat("C", 0.0f));

            expectClosed(() -> rs.updateDouble(1, 0.0));
            expectClosed(() -> rs.updateDouble("C", 0.0));

            expectClosed(() -> rs.updateString(1, ""));
            expectClosed(() -> rs.updateString("C", ""));

            expectClosed(() -> rs.updateTime(1, new Time(0)));
            expectClosed(() -> rs.updateTime("C", new Time(0)));

            expectClosed(() -> rs.updateDate(1, new Date(0)));
            expectClosed(() -> rs.updateDate("C", new Date(0)));

            expectClosed(() -> rs.updateTimestamp(1, new Timestamp(0)));
            expectClosed(() -> rs.updateTimestamp("C", new Timestamp(0)));

            expectClosed(() -> rs.updateBytes(1, new byte[]{1}));
            expectClosed(() -> rs.updateBytes("C", new byte[]{1}));

            expectClosed(() -> rs.updateArray(1, null));
            expectClosed(() -> rs.updateArray("C", null));

            expectClosed(() -> rs.updateBlob(1, (Blob) null));
            expectClosed(() -> rs.updateBlob(1, (InputStream) null));
            expectClosed(() -> rs.updateBlob(1, null, 0L));
            expectClosed(() -> rs.updateBlob("C", (Blob) null));
            expectClosed(() -> rs.updateBlob("C", (InputStream) null));
            expectClosed(() -> rs.updateBlob("C", null, 0L));

            expectClosed(() -> rs.updateClob(1, (Clob) null));
            expectClosed(() -> rs.updateClob(1, (Reader) null));
            expectClosed(() -> rs.updateClob(1, null, 0L));
            expectClosed(() -> rs.updateClob("C", (Clob) null));
            expectClosed(() -> rs.updateClob("C", (Reader) null));
            expectClosed(() -> rs.updateClob("C", null, 0L));

            expectClosed(() -> rs.updateNClob(1, (NClob) null));
            expectClosed(() -> rs.updateNClob(1, (Reader) null));
            expectClosed(() -> rs.updateNClob(1, null, 0L));
            expectClosed(() -> rs.updateNClob("C", (NClob) null));
            expectClosed(() -> rs.updateNClob("C", (Reader) null));
            expectClosed(() -> rs.updateNClob("C", null, 0L));

            expectClosed(() -> rs.updateAsciiStream(1, null));
            expectClosed(() -> rs.updateAsciiStream(1, null, 0));
            expectClosed(() -> rs.updateAsciiStream(1, null, 0L));
            expectClosed(() -> rs.updateAsciiStream("C", null));
            expectClosed(() -> rs.updateAsciiStream("C", null, 0));
            expectClosed(() -> rs.updateAsciiStream("C", null, 0L));

            expectClosed(() -> rs.updateBinaryStream(1, null));
            expectClosed(() -> rs.updateBinaryStream(1, null, 0));
            expectClosed(() -> rs.updateBinaryStream(1, null, 0L));
            expectClosed(() -> rs.updateBinaryStream("C", null));
            expectClosed(() -> rs.updateBinaryStream("C", null, 0));
            expectClosed(() -> rs.updateBinaryStream("C", null, 0L));

            expectClosed(() -> rs.updateCharacterStream(1, null));
            expectClosed(() -> rs.updateCharacterStream(1, null, 0));
            expectClosed(() -> rs.updateCharacterStream(1, null, 0L));
            expectClosed(() -> rs.updateCharacterStream("C", null));
            expectClosed(() -> rs.updateCharacterStream("C", null, 0));
            expectClosed(() -> rs.updateCharacterStream("C", null, 0L));

            expectClosed(() -> rs.updateNCharacterStream(1, null));
            expectClosed(() -> rs.updateNCharacterStream(1, null, 0));
            expectClosed(() -> rs.updateNCharacterStream(1, null, 0L));
            expectClosed(() -> rs.updateNCharacterStream("C", null));
            expectClosed(() -> rs.updateNCharacterStream("C", null, 0));
            expectClosed(() -> rs.updateNCharacterStream("C", null, 0L));

            expectClosed(() -> rs.updateRef(1, null));
            expectClosed(() -> rs.updateRef("C", null));

            expectClosed(() -> rs.updateRowId(1, null));
            expectClosed(() -> rs.updateRowId("C", null));

            expectClosed(() -> rs.updateNString(1, null));
            expectClosed(() -> rs.updateNString("C", null));

            expectClosed(() -> rs.updateSQLXML(1, null));
            expectClosed(() -> rs.updateSQLXML("C", null));

            expectClosed(() -> rs.updateObject(1, null));
            expectClosed(() -> rs.updateObject(1, null, 0));
            expectClosed(() -> rs.updateObject("C", null));
            expectClosed(() -> rs.updateObject("C", null, 0));

            expectClosed(() -> rs.updateBigDecimal(1, null));
            expectClosed(() -> rs.updateBigDecimal("C", null));

            expectClosed(() -> rs.updateNull(1));
            expectClosed(() -> rs.updateNull("C"));

            expectClosed(rs::cancelRowUpdates);
            expectClosed(rs::updateRow);
            expectClosed(rs::deleteRow);
            expectClosed(rs::insertRow);
            expectClosed(rs::moveToInsertRow);
        }
    }

    private static void expectNotSupported(ResultSetMethod m) {
        assertThrows(SQLFeatureNotSupportedException.class, m::call);
    }

    protected static void expectSqlException(ResultSetMethod m, String message) {
        SQLException err = assertThrows(SQLException.class, m::call);
        assertThat(err.getMessage(), containsString(message));
    }

    private static void expectInvalidColumn(ResultSetPositionalMethod m, int column) {
        SQLException err = assertThrows(SQLException.class, () -> m.call(column));
        assertThat(err.getMessage(), containsString("Invalid column index: " + column));
    }

    private static void expectInvalidColumn(ResultSetNamedMethod m, String column) {
        SQLException err = assertThrows(SQLException.class, () -> m.call(column));
        assertThat(err.getMessage(), containsString("Column not found: " + column));
    }

    private static void expectPositioned(ResultSetMethod m) {
        SQLException err = assertThrows(SQLException.class, m::call);
        assertThat(err.getMessage(), containsString("Result set is not positioned on a row."));
    }

    private static void expectClosed(ResultSetMethod method) {
        expectSqlException(method, "Result set is closed.");
    }

    /** Result set function. */
    @FunctionalInterface
    protected interface ResultSetMethod {
        void call() throws SQLException;
    }

    /** Result set function. */
    @FunctionalInterface
    protected interface ResultSetPositionalMethod {
        void call(int column) throws SQLException;
    }

    /** Result set function. */
    @FunctionalInterface
    protected interface ResultSetNamedMethod {
        void call(String column) throws SQLException;
    }
}
