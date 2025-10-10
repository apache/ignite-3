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

import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.jdbc.ConnectionProperties;
import org.apache.ignite.internal.jdbc.ConnectionPropertiesImpl;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

/**
 * Unit tests for {@link JdbcPreparedStatement2}.
 */
public class JdbcPreparedStatement2SelfTest extends JdbcStatement2SelfTest {

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

    @Test
    public void assignAndClearParameters() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = stmt.unwrap(JdbcPreparedStatement2.class);

            stmt.setObject(1, 1);
            stmt.setObject(3, 10L);

            assertEquals(Arrays.asList(1, null, 10L), ps.getArguments());

            stmt.clearParameters();
            assertEquals(List.of(), ps.getArguments());

            stmt.setObject(2, 1);
            assertEquals(Arrays.asList(null, 1), ps.getArguments());

            stmt.setObject(2, 2);
            assertEquals(Arrays.asList(null, 2), ps.getArguments());

            stmt.setObject(3, 4);
            assertEquals(Arrays.asList(null, 2, 4), ps.getArguments());
        }
    }

    @ParameterizedTest
    @EnumSource(JDBCType.class)
    public void setNull(JDBCType type) throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            int jdbcSqlType = type.getVendorTypeNumber();

            if (SUPPORTED_TYPES.contains(type)) {
                stmt.setNull(1, jdbcSqlType);
                assertFalse(ps.getArguments().isEmpty());
                assertNull(ps.getArguments().get(0));

                // Spec: If the parameter does not have a user-defined or REF type, the given typeName is ignored.

                stmt.setNull(2, jdbcSqlType, null);
                assertFalse(ps.getArguments().isEmpty());
                assertNull(ps.getArguments().get(1));

                stmt.setNull(2, jdbcSqlType, "TypeName");
                assertFalse(ps.getArguments().isEmpty());
                assertNull(ps.getArguments().get(1));

            } else {
                String error = "SQL-specific types are not supported.";

                expectError(() -> stmt.setNull(1, jdbcSqlType), error);
                expectError(() -> stmt.setNull(1, jdbcSqlType, null), error);
                expectError(() -> stmt.setNull(1, jdbcSqlType, "TypeName"), error);
            }
        }
    }

    @Test
    public void setBoolean() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setBoolean(1, true);
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(Boolean.TRUE, ps.getArguments().get(0));
        }
    }

    @Test
    public void setByte() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setByte(1, (byte) 7);
            assertFalse(ps.getArguments().isEmpty());
            assertEquals((byte) 7, ps.getArguments().get(0));
        }
    }

    @Test
    public void setShort() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setShort(1, (short) 123);
            assertFalse(ps.getArguments().isEmpty());
            assertEquals((short) 123, ps.getArguments().get(0));
        }
    }

    @Test
    public void setInt() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setInt(1, 42);
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(42, ps.getArguments().get(0));
        }
    }

    @Test
    public void setLong() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setLong(1, 1322L);
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(1322L, ps.getArguments().get(0));
        }
    }

    @Test
    public void setFloat() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setFloat(1, 1.5f);
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(1.5f, ps.getArguments().get(0));
        }
    }

    @Test
    public void setDouble() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setDouble(1, 2.75d);
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(2.75d, ps.getArguments().get(0));
        }
    }

    @Test
    public void setDecimal() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            BigDecimal dec = new BigDecimal("1234.5678");
            stmt.setBigDecimal(1, dec);
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(dec, ps.getArguments().get(0));

            stmt.setBigDecimal(2, null);
            assertTrue(ps.getArguments().size() >= 2);
            assertNull(ps.getArguments().get(1));
        }
    }

    @Test
    public void setString() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setString(1, "hello");
            assertFalse(ps.getArguments().isEmpty());
            assertEquals("hello", ps.getArguments().get(0));

            stmt.setString(2, null);
            assertTrue(ps.getArguments().size() >= 2);
            assertNull(ps.getArguments().get(1));
        }
    }

    @Test
    public void setNationalString() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setNString(1, "Hello");
            assertFalse(ps.getArguments().isEmpty());
            assertEquals("Hello", ps.getArguments().get(0));

            stmt.setNString(2, null);
            assertTrue(ps.getArguments().size() >= 2);
            assertNull(ps.getArguments().get(1));
        }
    }

    @Test
    public void setBytes() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            byte[] bytes = {1, 2, 3, 4};
            stmt.setBytes(1, bytes);
            assertFalse(ps.getArguments().isEmpty());
            assertArrayEquals(bytes, (byte[]) ps.getArguments().get(0));

            stmt.setBytes(2, null);
            assertTrue(ps.getArguments().size() >= 2);
            assertNull(ps.getArguments().get(1));
        }
    }

    @Test
    public void setDate() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            LocalDate ld = LocalDate.of(2020, 1, 2);
            stmt.setDate(1, Date.valueOf(ld));
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(ld, ps.getArguments().get(0));

            stmt.setDate(2, null);
            assertTrue(ps.getArguments().size() >= 2);
            assertNull(ps.getArguments().get(1));

            stmt.setDate(3, Date.valueOf(ld), Calendar.getInstance());
            assertTrue(ps.getArguments().size() >= 3);
            assertEquals(ld, ps.getArguments().get(2));
        }
    }

    @Test
    public void setTime() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            LocalTime lt = LocalTime.of(11, 22, 33);
            stmt.setTime(1, Time.valueOf(lt));
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(lt, ps.getArguments().get(0));

            stmt.setTime(2, null);
            assertTrue(ps.getArguments().size() >= 2);
            assertNull(ps.getArguments().get(1));

            stmt.setTime(3, Time.valueOf(lt), Calendar.getInstance());
            assertTrue(ps.getArguments().size() >= 3);
            assertEquals(lt, ps.getArguments().get(2));
        }
    }

    @Test
    public void setTimestamp() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            LocalDateTime ldt = LocalDateTime.of(2021, 5, 6, 7, 8, 9);
            stmt.setTimestamp(1, Timestamp.valueOf(ldt));
            assertFalse(ps.getArguments().isEmpty());
            assertEquals(ldt, ps.getArguments().get(0));

            stmt.setTimestamp(2, null);
            assertTrue(ps.getArguments().size() >= 2);
            assertNull(ps.getArguments().get(1));

            stmt.setTimestamp(3, Timestamp.valueOf(ldt), Calendar.getInstance());
            assertTrue(ps.getArguments().size() >= 3);
            assertEquals(ldt, ps.getArguments().get(2));
        }
    }

    @Test
    public void setObject() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            JdbcPreparedStatement2 ps = (JdbcPreparedStatement2) stmt;

            stmt.setObject(1, null);
            assertNull(ps.getArguments().get(0));

            stmt.setObject(2, (byte) 1);
            assertEquals((byte) 1, ps.getArguments().get(1));

            stmt.setObject(3, (short) 2);
            assertEquals((short) 2, ps.getArguments().get(2));

            stmt.setObject(4, 42);
            assertEquals(42, ps.getArguments().get(3));

            stmt.setObject(5, 9L);
            assertEquals(9L, ps.getArguments().get(4));

            stmt.setObject(6, 1.0f);
            assertEquals(1.0f, ps.getArguments().get(5));

            stmt.setObject(7, 2.0d);
            assertEquals(2.0d, ps.getArguments().get(6));

            stmt.setObject(8, BigDecimal.ONE);
            assertEquals(BigDecimal.ONE, ps.getArguments().get(7));

            stmt.setObject(9, "str");
            assertEquals("str", ps.getArguments().get(8));

            stmt.setObject(10, "123".getBytes(StandardCharsets.UTF_8));
            assertArrayEquals("123".getBytes(StandardCharsets.UTF_8), (byte[]) ps.getArguments().get(9));

            Date sqlDate = Date.valueOf(LocalDate.now());
            stmt.setObject(11, sqlDate);
            assertEquals(sqlDate.toLocalDate(), ps.getArguments().get(10));

            Time sqlTime = Time.valueOf(LocalTime.now());
            stmt.setObject(12, sqlTime);
            assertEquals(sqlTime.toLocalTime(), ps.getArguments().get(11));

            Timestamp sqlTimestamp = Timestamp.valueOf(LocalDateTime.now());
            stmt.setObject(13, sqlTimestamp);
            assertEquals(sqlTimestamp.toLocalDateTime(), ps.getArguments().get(12));

            UUID uuid = UUID.randomUUID();
            stmt.setObject(14, uuid);
            assertEquals(uuid, ps.getArguments().get(13));

            // java.time classes

            LocalDate date = LocalDate.now();
            stmt.setObject(15, date);
            assertEquals(date, ps.getArguments().get(14));

            LocalTime time = LocalTime.now();
            stmt.setObject(16, time);
            assertEquals(time, ps.getArguments().get(15));

            LocalDateTime dateTime = LocalDateTime.now();
            stmt.setObject(17, dateTime);
            assertEquals(dateTime, ps.getArguments().get(16));

            Instant instant = Instant.now();
            stmt.setObject(18, instant);
            assertEquals(instant, ps.getArguments().get(17));

            // Unsupported
            expectError(
                    () -> stmt.setObject(1, new Abc()),
                    "Parameter type is not supported: " + Abc.class.getName()
            );
        }
    }

    private static final class Abc {

    }

    @Test
    @Override
    public void close() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {
            assertFalse(stmt.isClosed());

            stmt.close();
            assertTrue(stmt.isClosed());

            expectClosed(() -> stmt.setMaxFieldSize(100_000));
            expectClosed(stmt::getMaxFieldSize);

            expectClosed(() -> stmt.setEscapeProcessing(true));
            expectClosed(() -> stmt.setEscapeProcessing(false));

            expectClosed(() -> stmt.setQueryTimeout(-1));
            expectClosed(() -> stmt.setQueryTimeout(1));
            expectClosed(stmt::getQueryTimeout);

            JdbcStatement2 stmt2 = (JdbcStatement2) stmt;
            expectClosed(() -> stmt2.setQueryTimeout(-1));
            expectClosed(() -> stmt2.setQueryTimeout(0));
            expectClosed(() -> stmt2.setQueryTimeout(1));

            expectClosed(stmt::cancel);

            expectClosed(stmt::getWarnings);
            expectClosed(stmt::clearWarnings);

            expectClosed(() -> stmt.setCursorName("C"));
            expectClosed(() -> stmt.setCursorName(null));

            expectClosed(stmt::getResultSet);

            expectClosed(stmt::getUpdateCount);

            expectClosed(stmt::getMoreResults);

            expectClosed(() -> stmt.setFetchDirection(ResultSet.FETCH_FORWARD));
            expectClosed(() -> stmt.setFetchDirection(ResultSet.FETCH_REVERSE));
            expectClosed(() -> stmt.setFetchDirection(ResultSet.FETCH_UNKNOWN));
            expectClosed(stmt::getFetchDirection);

            expectClosed(() -> stmt.setFetchSize(-1));
            expectClosed(() -> stmt.setFetchSize(0));
            expectClosed(() -> stmt.setFetchSize(1));
            expectClosed(stmt::getFetchSize);

            expectClosed(stmt::getResultSetConcurrency);
            expectClosed(stmt::getResultSetType);

            expectClosed(stmt::clearBatch);

            expectClosed(stmt::executeBatch);

            expectClosed(stmt::getConnection);

            expectClosed(stmt::getMoreResults);

            expectClosed(() -> stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            expectClosed(() -> stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            expectClosed(() -> stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

            expectClosed(stmt::getGeneratedKeys);

            expectClosed(stmt::getResultSetHoldability);

            expectClosed(() -> stmt.setPoolable(true));
            expectClosed(() -> stmt.setPoolable(false));
            expectClosed(stmt::isPoolable);

            expectClosed(stmt::closeOnCompletion);
            expectClosed(stmt::isCloseOnCompletion);

            // PreparedStatement specific methods

            expectClosed(stmt::executeQuery);
            expectClosed(stmt::executeUpdate);

            expectClosed(() -> stmt.setNull(1, JDBCType.INTEGER.getVendorTypeNumber()));
            expectClosed(() -> stmt.setNull(1, JDBCType.INTEGER.getVendorTypeNumber(), "TypeName"));
            expectClosed(() -> stmt.setBoolean(1, true));
            expectClosed(() -> stmt.setByte(1, (byte) 1));
            expectClosed(() -> stmt.setShort(1, (short) 1));
            expectClosed(() -> stmt.setInt(1, 1));
            expectClosed(() -> stmt.setLong(1, 1));
            expectClosed(() -> stmt.setFloat(1, 1));
            expectClosed(() -> stmt.setDouble(1, 1));
            expectClosed(() -> stmt.setBigDecimal(1, BigDecimal.ZERO));
            expectClosed(() -> stmt.setString(1, "1"));
            expectClosed(() -> stmt.setBytes(1, new byte[]{1}));

            expectClosed(() -> stmt.setDate(1, Date.valueOf(LocalDate.now())));
            expectClosed(() -> stmt.setDate(1, Date.valueOf(LocalDate.now()), Calendar.getInstance()));

            expectClosed(() -> stmt.setTime(1, Time.valueOf(LocalTime.now())));
            expectClosed(() -> stmt.setTime(1, Time.valueOf(LocalTime.now()), Calendar.getInstance()));

            expectClosed(() -> stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now())));
            expectClosed(() -> stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()), Calendar.getInstance()));

            expectClosed(() -> stmt.setAsciiStream(1, InputStream.nullInputStream()));
            expectClosed(() -> stmt.setAsciiStream(1, InputStream.nullInputStream(), 1));
            expectClosed(() -> stmt.setAsciiStream(1, InputStream.nullInputStream(), 1L));

            //noinspection deprecation
            expectClosed(() -> stmt.setUnicodeStream(1, InputStream.nullInputStream(), 1));

            expectClosed(() -> stmt.setBinaryStream(1, InputStream.nullInputStream()));
            expectClosed(() -> stmt.setBinaryStream(1, InputStream.nullInputStream(), 10L));
            expectClosed(() -> stmt.setBinaryStream(1, InputStream.nullInputStream(), 10));

            expectClosed(stmt::clearParameters);
            expectClosed(() -> stmt.setObject(1, 1));
            // not supported
            expectClosed(() -> stmt.setObject(1, 1, JDBCType.INTEGER.getVendorTypeNumber()));
            // // not supported
            expectClosed(() -> stmt.setObject(1, 1, JDBCType.INTEGER));
            // not supported
            expectClosed(() -> stmt.setObject(1, 1, JDBCType.VARCHAR.getVendorTypeNumber(), 10));
            // not supported
            expectClosed(() -> stmt.setObject(1, 1, JDBCType.VARCHAR, 10));

            expectClosed(() -> stmt.setURL(1, new URL("https://test.com")));

            expectClosed(() -> stmt.setCharacterStream(1, new StringReader("1")));
            expectClosed(() -> stmt.setCharacterStream(1, new StringReader("1"), 10));
            expectClosed(() -> stmt.setCharacterStream(1, new StringReader("1"), 10L));

            expectClosed(() -> stmt.setRef(1, Mockito.mock(Ref.class)));

            expectClosed(() -> stmt.setBlob(1, Mockito.mock(Blob.class)));
            expectClosed(() -> stmt.setBlob(1, InputStream.nullInputStream()));
            expectClosed(() -> stmt.setBlob(1, InputStream.nullInputStream(), 10));

            expectClosed(() -> stmt.setClob(1, Mockito.mock(Clob.class)));
            expectClosed(() -> stmt.setClob(1, new StringReader("1")));
            expectClosed(() -> stmt.setClob(1, new StringReader("1"), 10));

            expectClosed(() -> stmt.setArray(1, Mockito.mock(Array.class)));

            expectClosed(stmt::getMetaData);
            expectClosed(stmt::getParameterMetaData);

            expectClosed(() -> stmt.setRowId(1, Mockito.mock(RowId.class)));

            expectClosed(() -> stmt.setNCharacterStream(1, new StringReader("1")));
            expectClosed(() -> stmt.setNCharacterStream(1, new StringReader("1"), 10));

            expectClosed(() -> stmt.setNClob(1, new StringReader("1")));
            expectClosed(() -> stmt.setNClob(1, new StringReader("1"), 10));
            expectClosed(() -> stmt.setNClob(1, Mockito.mock(NClob.class)));

            expectClosed(() -> stmt.setSQLXML(1, Mockito.mock(SQLXML.class)));

            expectClosed(stmt::executeLargeUpdate);
            expectClosed(stmt::executeLargeBatch);
        }
    }

    @Test
    public void executeSql() throws SQLException {
        try (Statement stmt = createStatement()) {
            expectError(() -> stmt.execute("UPDATE t SET c = 1"),
                    "The method 'execute(String)' is called on PreparedStatement instance.");

            expectError(() -> stmt.execute("UPDATE t SET c = 1", Statement.RETURN_GENERATED_KEYS),
                    "The method 'execute(String, int)' is called on PreparedStatement instance.");

            expectError(() -> stmt.execute("UPDATE t SET c = 1", new int[]{1}),
                    "The method 'execute(String, int[])' is called on PreparedStatement instance.");

            expectError(() -> stmt.execute("UPDATE t SET c = 1", new String[]{"id"}),
                    "The method 'execute(String, String[]) is called on PreparedStatement instance.");
        }
    }

    @Test
    @Override
    public void updateWithColumns() throws SQLException {
        try (Statement stmt = createStatement()) {
            expectError(() -> stmt.executeUpdate("UPDATE t SET c = 1"),
                    "The method 'executeUpdate(String)' is called on PreparedStatement instance.");

            expectError(() -> stmt.executeUpdate("UPDATE t SET c = 1", Statement.RETURN_GENERATED_KEYS),
                    "The method 'executeUpdate(String, int)' is called on PreparedStatement instance.");

            expectError(() -> stmt.executeUpdate("UPDATE t SET c = 1", new int[]{1}),
                    "The method 'executeUpdate(String, int[])' is called on PreparedStatement instance.");

            expectError(() -> stmt.executeUpdate("UPDATE t SET c = 1", new String[]{"id"}),
                    "The method 'executeUpdate(String, String[])' is called on PreparedStatement instance.");
        }
    }

    @Test
    public void notSupportedMethods() throws SQLException {
        try (PreparedStatement stmt = createStatement()) {

            String conversionError = "Conversion to target sql type is not supported.";
            expectError(() -> stmt.setObject(1, 1, JDBCType.INTEGER.getVendorTypeNumber()), conversionError);
            expectError(() -> stmt.setObject(1, 1, JDBCType.INTEGER), conversionError);
            expectError(() -> stmt.setObject(1, 1, JDBCType.VARCHAR.getVendorTypeNumber(), 10), conversionError);
            expectError(() -> stmt.setObject(1, 1, JDBCType.VARCHAR, 10), conversionError);

            String sqlTypeError = "SQL-specific types are not supported.";
            expectError(() -> stmt.setURL(1, new URL("https://test.com")), sqlTypeError);

            expectError(() -> stmt.setRef(1, Mockito.mock(Ref.class)), sqlTypeError);

            expectError(() -> stmt.setBlob(1, Mockito.mock(Blob.class)), sqlTypeError);
            expectError(() -> stmt.setBlob(1, InputStream.nullInputStream()), sqlTypeError);
            expectError(() -> stmt.setBlob(1, InputStream.nullInputStream(), 10), sqlTypeError);

            expectError(() -> stmt.setClob(1, Mockito.mock(Clob.class)), sqlTypeError);
            expectError(() -> stmt.setClob(1, new StringReader("1")), sqlTypeError);
            expectError(() -> stmt.setClob(1, new StringReader("1"), 10), sqlTypeError);

            expectError(() -> stmt.setArray(1, Mockito.mock(Array.class)), sqlTypeError);

            expectError(() -> stmt.setRowId(1, Mockito.mock(RowId.class)), sqlTypeError);

            expectError(() -> stmt.setNClob(1, new StringReader("1")), sqlTypeError);
            expectError(() -> stmt.setNClob(1, new StringReader("1"), 10), sqlTypeError);
            expectError(() -> stmt.setNClob(1, Mockito.mock(NClob.class)), sqlTypeError);

            expectError(() -> stmt.setSQLXML(1, Mockito.mock(SQLXML.class)), sqlTypeError);

            String streamTypeError = "Streams are not supported";
            expectError(() -> stmt.setCharacterStream(1, new StringReader("1")), sqlTypeError);
            expectError(() -> stmt.setCharacterStream(1, new StringReader("1"), 10), sqlTypeError);
            expectError(() -> stmt.setCharacterStream(1, new StringReader("1"), 10L), sqlTypeError);

            expectError(() -> stmt.setNCharacterStream(1, new StringReader("1")), streamTypeError);
            expectError(() -> stmt.setNCharacterStream(1, new StringReader("1"), 10), streamTypeError);
            expectError(() -> stmt.setNCharacterStream(1, new StringReader("1"), 10L), streamTypeError);

            expectError(() -> stmt.setAsciiStream(1, InputStream.nullInputStream()), streamTypeError);
            expectError(() -> stmt.setAsciiStream(1, InputStream.nullInputStream(), 10), streamTypeError);
            expectError(() -> stmt.setAsciiStream(1, InputStream.nullInputStream(), 10L), streamTypeError);

            expectError(() -> stmt.setBinaryStream(1, InputStream.nullInputStream()), streamTypeError);
            expectError(() -> stmt.setBinaryStream(1, InputStream.nullInputStream(), 10), streamTypeError);
            expectError(() -> stmt.setBinaryStream(1, InputStream.nullInputStream(), 10L), streamTypeError);

            //noinspection deprecation
            expectError(() -> stmt.setUnicodeStream(1, InputStream.nullInputStream(), 10), streamTypeError);

            expectError(stmt::getMetaData, "ResultSet metadata for prepared statement is not supported.");
            expectError(stmt::getParameterMetaData, "Parameter metadata is not supported.");

            expectError(stmt::executeLargeUpdate, "executeLargeUpdate not implemented.");
            expectError(stmt::executeLargeBatch, "executeLargeBatch not implemented.");
        }
    }

    @Test
    @Override
    public void largeUpdateMethods() throws SQLException {
        try (Statement stmt = createStatement()) {
            expectError(() -> stmt.executeLargeUpdate("UPDATE t SET val=2"),
                    "The method 'executeLargeUpdate(String)' is called on PreparedStatement instance.");

            expectError(() -> stmt.executeLargeUpdate("UPDATE t SET val=2", Statement.RETURN_GENERATED_KEYS),
                    "The method 'executeLargeUpdate(String, int)' is called on PreparedStatement instance.");

            expectError(() -> stmt.executeLargeUpdate("UPDATE t SET val=2", new int[]{0}),
                    "The method 'executeLargeUpdate(String, int[])' is called on PreparedStatement instance.");

            expectError(() -> stmt.executeLargeUpdate("UPDATE t SET val=2", new String[]{"C1"}),
                    "The method 'executeLargeUpdate(String, String[])' is called on PreparedStatement instance.");
        }
    }

    private static void expectError(Executable executable, String message) {
        assertThrowsSqlException(SQLException.class, message, executable);
    }

    @Override
    protected PreparedStatement createStatement() throws SQLException {
        Connection connection = Mockito.mock(Connection.class);
        JdbcConnection2 connection2 = Mockito.mock(JdbcConnection2.class);

        ConnectionProperties properties = new ConnectionPropertiesImpl();

        when(connection.unwrap(JdbcConnection2.class)).thenReturn(connection2);
        when(connection2.properties()).thenReturn(properties);

        return createStatement(connection);
    }

    @Override
    protected PreparedStatement createStatement(Connection connection) {
        IgniteSql igniteSql = Mockito.mock(IgniteSql.class);
        return new JdbcPreparedStatement2(connection, igniteSql, "PUBLIC", ResultSet.HOLD_CURSORS_OVER_COMMIT, "SELECT 1");
    }
}
