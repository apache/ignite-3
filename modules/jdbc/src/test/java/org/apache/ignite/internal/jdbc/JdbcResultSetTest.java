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

import static org.apache.ignite.internal.jdbc.proto.IgniteQueryErrorCode.codeToSqlState;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryCursorHandler;
import org.apache.ignite.internal.jdbc.proto.event.JdbcColumnMeta;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQuerySingleResult;
import org.apache.ignite.internal.jdbc.proto.event.Response;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit test for JdbcResultSet. */
@ExtendWith(MockitoExtension.class)
public class JdbcResultSetTest extends BaseIgniteAbstractTest {
    @Test
    public void exceptionIsPropagatedFromGetNextResultResponse() throws SQLException {
        String errorStr = "Failed to fetch query results";

        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = createResultSet(handler, stmt, true);

        when(handler.getMoreResultsAsync(any())).thenReturn(CompletableFuture.completedFuture(
                new JdbcQuerySingleResult(Response.STATUS_FAILED, errorStr)));

        SQLException ex = assertThrows(SQLException.class, rs::getNextResultSet);

        String actualMessage = ex.getMessage();

        assertEquals(errorStr, actualMessage);
        assertEquals(codeToSqlState(Response.STATUS_FAILED), ex.getSQLState());

        verify(handler).getMoreResultsAsync(any());
        verifyNoMoreInteractions(handler);

        assertTrue(rs.isClosed());
    }

    @Test
    public void getNextResultWhenNextResultAvailable() throws SQLException {
        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = createResultSet(handler, stmt, true);

        int expectedUpdateCount = 10;

        when(handler.getMoreResultsAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(new JdbcQuerySingleResult(null, expectedUpdateCount, false)));

        JdbcResultSet nextRs = rs.getNextResultSet();

        assertNotNull(nextRs);
        assertEquals(expectedUpdateCount, nextRs.updatedCount());

        verify(handler).getMoreResultsAsync(any());
        verifyNoMoreInteractions(handler);

        assertTrue(rs.isClosed());
    }

    @Test
    public void getNextResultWhenNextResultIsNotAvailable() throws SQLException {
        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = createResultSet(handler, stmt, false);

        when(handler.closeAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(new JdbcQueryCloseResult()));

        JdbcResultSet nextRs = rs.getNextResultSet();

        assertNull(nextRs);

        verify(handler).closeAsync(any());
        verifyNoMoreInteractions(handler);

        assertTrue(rs.isClosed());
    }

    @ParameterizedTest(name = "hasNextResult: {0}")
    @ValueSource(booleans = {true, false})
    public void checkClose(boolean hasNextResult) throws SQLException {
        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcResultSet rs = createResultSet(handler, stmt, hasNextResult);

        when(handler.closeAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(new JdbcQueryCloseResult()));

        rs.close();

        ArgumentCaptor<JdbcQueryCloseRequest> argument = ArgumentCaptor.forClass(JdbcQueryCloseRequest.class);

        verify(handler).closeAsync(argument.capture());

        assertEquals(!hasNextResult, argument.getValue().removeFromResources());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Europe/Paris", "America/Los_Angeles", "Asia/Tokyo"})
    public void getTime(String zone) throws SQLException {
        ZoneId zoneId = ZoneId.of(zone);

        Clock clock = Clock.fixed(Instant.now().truncatedTo(ChronoUnit.SECONDS), ZoneId.systemDefault());
        Instant instant = clock.instant();
        LocalDateTime dateTime = LocalDateTime.now(clock);
        LocalTime time = LocalTime.now(clock);
        LocalDate date = LocalDate.now(clock);

        List<ColumnType> columns = List.of(ColumnType.TIME, ColumnType.DATE, ColumnType.DATETIME, ColumnType.TIMESTAMP);

        try (ResultSet resultSet = createResultSet(zoneId, columns, (row) -> {
            row.appendTime(time);
            row.appendDate(date);
            row.appendDateTime(dateTime);
            row.appendTimestamp(instant);
        })) {
            resultSet.next();

            // from time
            {
                Time t = resultSet.getTime(1);
                assertEquals(time, t.toLocalTime());
            }

            // from date
            {
                Time t = resultSet.getTime(2);
                assertEquals(LocalTime.of(0, 0, 0), t.toLocalTime());
            }

            // from datetime
            {
                Time t = resultSet.getTime(3);
                assertEquals(dateTime.toLocalTime(), t.toLocalTime());
            }

            // from instant / timestamp ltz
            {
                Time t = resultSet.getTime(4);
                // Timestamp is local time adjusted.
                Timestamp ts = resultSet.getTimestamp(4);
                assertEquals(ts.toLocalDateTime().toLocalTime(), t.toLocalTime());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"Europe/Paris", "America/Los_Angeles", "Asia/Tokyo"})
    public void getDate(String zone) throws SQLException {
        ZoneId zoneId = ZoneId.of(zone);

        Clock clock = Clock.fixed(Instant.now().truncatedTo(ChronoUnit.SECONDS), ZoneId.systemDefault());
        Instant instant = clock.instant();
        LocalDateTime dateTime = LocalDateTime.now(clock);
        LocalTime time = LocalTime.now(clock);
        LocalDate date = LocalDate.now(clock);

        List<ColumnType> columns = List.of(ColumnType.TIME, ColumnType.DATE, ColumnType.DATETIME, ColumnType.TIMESTAMP);

        try (ResultSet resultSet = createResultSet(zoneId, columns, (row) -> {
            row.appendTime(time);
            row.appendDate(date);
            row.appendDateTime(dateTime);
            row.appendTimestamp(instant);
        })) {
            resultSet.next();

            // from time
            {
                Date t = resultSet.getDate(1);
                assertEquals(LocalDate.of(1970, 1, 1), t.toLocalDate());
            }

            // from date
            {
                Date t = resultSet.getDate(2);
                assertEquals(date, t.toLocalDate());
            }

            // from datetime
            {
                Date t = resultSet.getDate(3);
                assertEquals(dateTime.toLocalDate(), t.toLocalDate());
            }

            // from timestamp with local timezone
            {
                Date t = resultSet.getDate(4);
                Timestamp ts = resultSet.getTimestamp(4);
                assertEquals(ts.toLocalDateTime().toLocalDate(), t.toLocalDate());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"Europe/Paris", "America/Los_Angeles", "Asia/Tokyo"})
    public void getTimestamp(String zone) throws SQLException {
        ZoneId zoneId = ZoneId.of(zone);

        Clock clock = Clock.fixed(Instant.now().truncatedTo(ChronoUnit.SECONDS), ZoneId.systemDefault());
        Instant instant = clock.instant();
        LocalDate date = LocalDate.now(clock);
        LocalDateTime dateTime = LocalDateTime.now(clock);
        LocalTime time = LocalTime.now(clock);

        List<ColumnType> columns = List.of(ColumnType.TIME, ColumnType.DATE, ColumnType.DATETIME, ColumnType.TIMESTAMP);

        try (ResultSet resultSet = createResultSet(zoneId, columns, (row) -> {
            row.appendTime(time);
            row.appendDate(date);
            row.appendDateTime(dateTime);
            row.appendTimestamp(instant);
        })) {
            resultSet.next();

            // from time
            {
                Timestamp ts = resultSet.getTimestamp(1);
                assertEquals(LocalDateTime.of(LocalDate.of(1970, 1, 1), time), ts.toLocalDateTime());
            }

            // from date
            {
                Timestamp ts = resultSet.getTimestamp(2);
                assertEquals(LocalDateTime.of(date, LocalTime.of(0, 0, 0)), ts.toLocalDateTime());
            }

            // from datetime
            {
                Timestamp ts = resultSet.getTimestamp(3);
                assertEquals(dateTime, ts.toLocalDateTime());
            }

            // from instant / timestamp ltz
            {
                Timestamp ts = resultSet.getTimestamp(4);
                assertEquals(LocalDateTime.ofInstant(instant, zoneId), ts.toLocalDateTime());
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            "Europe/Paris,        2009-02-14 00:31:38.765",
            "America/Los_Angeles, 2009-02-13 15:31:38.765",
            "Asia/Tokyo,          2009-02-14 08:31:38.765"
    })
    public void getStringDateTimeTypes(String zone, String localDateTime) throws SQLException {
        ZoneId zoneId = ZoneId.of(zone);

        Instant now = Instant.ofEpochMilli(1234567898765L);
        Clock clock = Clock.fixed(now, ZoneId.of(zone));
        Instant instant = clock.instant();
        LocalDate date = LocalDate.now(clock);
        LocalDateTime dateTime = LocalDateTime.now(clock);
        LocalTime time = LocalTime.now(clock);

        List<ColumnType> columns = List.of(ColumnType.TIME, ColumnType.DATE, ColumnType.DATETIME, ColumnType.TIMESTAMP);

        try (ResultSet resultSet = createResultSet(zoneId, columns, (row) -> {
            row.appendTime(time);
            row.appendDate(date);
            row.appendDateTime(dateTime);
            row.appendTimestamp(instant);
        })) {
            resultSet.next();

            String actual = resultSet.getString(1)
                    + " | " + resultSet.getString(2)
                    + " | " + resultSet.getString(3)
                    + " | " + resultSet.getString(4);

            String expected = time.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))
                    + " | " + date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                    + " | " + dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
                    + " | " + localDateTime;

            assertEquals(expected, actual);
        }
    }

    @Test
    public void getString() throws SQLException {
        List<ColumnType> columns = List.of(
                ColumnType.NULL,
                ColumnType.BOOLEAN,
                ColumnType.INT8,
                ColumnType.INT16,
                ColumnType.INT32,
                ColumnType.INT64,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.DECIMAL,
                ColumnType.STRING,
                ColumnType.BYTE_ARRAY,
                ColumnType.UUID
        );

        Random random = new Random();
        long seed = System.nanoTime();
        random.setSeed(seed);

        log.info("Seed: {}", seed);

        // getString converts to JDBC types.
        Map<ColumnType, Object> javaValues = new EnumMap<>(ColumnType.class);
        javaValues.put(ColumnType.NULL, null);
        javaValues.put(ColumnType.BOOLEAN, random.nextBoolean());
        javaValues.put(ColumnType.INT8, (byte) nextInt(random, Byte.MIN_VALUE, Byte.MAX_VALUE + 1));
        javaValues.put(ColumnType.INT16, (short) nextInt(random, Short.MIN_VALUE, Short.MAX_VALUE + 1));
        javaValues.put(ColumnType.INT32, random.nextInt());
        javaValues.put(ColumnType.INT64, random.nextLong());
        javaValues.put(ColumnType.FLOAT, random.nextFloat());
        javaValues.put(ColumnType.DOUBLE, random.nextDouble());
        // toString on bigdecimals that are exact power of 10 uses scientific format
        // Use values from [0, 9] interval to prevent that.
        javaValues.put(ColumnType.DECIMAL, BigDecimal.valueOf(nextInt(random, 0, 10)));
        javaValues.put(ColumnType.STRING, String.valueOf(random.nextInt()));
        javaValues.put(ColumnType.BYTE_ARRAY, String.valueOf(random.nextInt()).getBytes(StandardCharsets.US_ASCII));
        javaValues.put(ColumnType.UUID, UUID.randomUUID());

        // String values
        Map<ColumnType, String> stringValues = new EnumMap<>(ColumnType.class);
        for (Map.Entry<ColumnType, Object> e : javaValues.entrySet()) {
            ColumnType columnType = e.getKey();
            Object value = e.getValue();

            if (columnType == ColumnType.NULL) {
                // getString contract for a null value
                stringValues.put(ColumnType.NULL, null);
            } else if (columnType == ColumnType.BYTE_ARRAY) {
                stringValues.put(ColumnType.BYTE_ARRAY, StringUtils.toHexString((byte[]) value));
            } else {
                stringValues.put(columnType, String.valueOf(value));
            }
        }

        try (ResultSet resultSet = createResultSet(null, columns, (row) -> {
            row.appendNull();
            row.appendBoolean((Boolean) javaValues.get(ColumnType.BOOLEAN));
            row.appendByte((Byte) javaValues.get(ColumnType.INT8));
            row.appendShort((Short) javaValues.get(ColumnType.INT16));
            row.appendInt((Integer) javaValues.get(ColumnType.INT32));
            row.appendLong((Long) javaValues.get(ColumnType.INT64));
            row.appendFloat((Float) javaValues.get(ColumnType.FLOAT));
            row.appendDouble((Double) javaValues.get(ColumnType.DOUBLE));
            row.appendDecimal((BigDecimal) javaValues.get(ColumnType.DECIMAL), 1);
            row.appendString((String) javaValues.get(ColumnType.STRING));
            row.appendBytes((byte[]) javaValues.get(ColumnType.BYTE_ARRAY));
            row.appendUuid((UUID) javaValues.get(ColumnType.UUID));
        })) {
            resultSet.next();

            for (int i = 0; i < columns.size(); i++) {
                ColumnType columnType = columns.get(i);
                String actual = resultSet.getString(i + 1);
                String expected = stringValues.get(columnType);

                assertEquals(expected, actual, "column-zero based #" + i + " " + columnType);
            }
        }
    }

    @Test
    public void getObject() throws SQLException {
        Clock clock = Clock.fixed(Instant.now().truncatedTo(ChronoUnit.SECONDS), ZoneId.systemDefault());

        Instant instant = clock.instant();
        LocalDate date = LocalDate.now(clock);
        LocalDateTime dateTime = LocalDateTime.now(clock).withNano(0);
        LocalTime time = LocalTime.now(clock);

        List<ColumnType> columns = List.of(
                ColumnType.NULL,
                ColumnType.BOOLEAN,
                ColumnType.INT8,
                ColumnType.INT16,
                ColumnType.INT32,
                ColumnType.INT64,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.DECIMAL,
                ColumnType.TIMESTAMP,
                ColumnType.DATETIME,
                ColumnType.DATE,
                ColumnType.TIME,
                ColumnType.STRING,
                ColumnType.BYTE_ARRAY,
                ColumnType.UUID
        );

        Random random = new Random();
        long seed = System.nanoTime();
        random.setSeed(seed);

        log.info("Seed: {}", seed);

        // getObject does not convert to JDBC types.
        Map<ColumnType, Object> javaValues = new EnumMap<>(ColumnType.class);
        javaValues.put(ColumnType.NULL, null);
        javaValues.put(ColumnType.BOOLEAN, random.nextBoolean());
        javaValues.put(ColumnType.INT8, (byte) nextInt(random, Byte.MIN_VALUE, Byte.MAX_VALUE + 1));
        javaValues.put(ColumnType.INT16, (short) nextInt(random, Short.MIN_VALUE, Short.MAX_VALUE + 1));
        javaValues.put(ColumnType.INT32, random.nextInt());
        javaValues.put(ColumnType.INT64, random.nextLong());
        javaValues.put(ColumnType.FLOAT, random.nextFloat());
        javaValues.put(ColumnType.DOUBLE, random.nextDouble());
        // Use small numbers to avoid problems cause by comparing 80 and 8E+1
        javaValues.put(ColumnType.DECIMAL, BigDecimal.valueOf(nextInt(random, 0, 10)));
        javaValues.put(ColumnType.TIMESTAMP, instant);
        javaValues.put(ColumnType.DATETIME, dateTime);
        javaValues.put(ColumnType.DATE, date);
        javaValues.put(ColumnType.TIME, time);
        javaValues.put(ColumnType.STRING, String.valueOf(random.nextInt()));
        javaValues.put(ColumnType.BYTE_ARRAY, String.valueOf(random.nextInt()).getBytes(StandardCharsets.US_ASCII));
        javaValues.put(ColumnType.UUID, UUID.randomUUID());

        try (ResultSet resultSet = createResultSet(null, columns, (row) -> {
            row.appendNull();
            row.appendBoolean((Boolean) javaValues.get(ColumnType.BOOLEAN));
            row.appendByte((Byte) javaValues.get(ColumnType.INT8));
            row.appendShort((Short) javaValues.get(ColumnType.INT16));
            row.appendInt((Integer) javaValues.get(ColumnType.INT32));
            row.appendLong((Long) javaValues.get(ColumnType.INT64));
            row.appendFloat((Float) javaValues.get(ColumnType.FLOAT));
            row.appendDouble((Double) javaValues.get(ColumnType.DOUBLE));
            row.appendDecimal((BigDecimal) javaValues.get(ColumnType.DECIMAL), 0);
            row.appendTimestamp((Instant) javaValues.get(ColumnType.TIMESTAMP));
            row.appendDateTime((LocalDateTime) javaValues.get(ColumnType.DATETIME));
            row.appendDate((LocalDate) javaValues.get(ColumnType.DATE));
            row.appendTime((LocalTime) javaValues.get(ColumnType.TIME));
            row.appendString((String) javaValues.get(ColumnType.STRING));
            row.appendBytes((byte[]) javaValues.get(ColumnType.BYTE_ARRAY));
            row.appendUuid((UUID) javaValues.get(ColumnType.UUID));
        })) {
            resultSet.next();

            for (int i = 0; i < columns.size(); i++) {
                ColumnType columnType = columns.get(i);
                Object actual = resultSet.getObject(i + 1);
                Object expected = javaValues.get(columnType);
                String message = "column-zero based #" + i + " " + columnType;

                if (columnType == ColumnType.BYTE_ARRAY) {
                    byte[] actualBytes = (byte[]) actual;
                    byte[] expectedBytes = (byte[]) expected;

                    assertArrayEquals(expectedBytes, actualBytes, message);
                } else {
                    assertEquals(expected, actual, message);
                }
            }
        }
    }

    private static JdbcResultSet createResultSet(
            JdbcQueryCursorHandler handler,
            JdbcStatement statement,
            boolean hasNextResult
    ) {
        return new JdbcResultSet(handler, statement, 1L, 1, true, List.of(), List.of(), true, hasNextResult, 0, false, 1, r -> List.of());
    }

    private static JdbcResultSet createResultSet(
            @Nullable ZoneId zoneId,
            List<ColumnType> columnTypes,
            Consumer<BinaryTupleBuilder> row
    ) throws SQLException {
        JdbcQueryCursorHandler handler = mock(JdbcQueryCursorHandler.class);
        JdbcStatement stmt = mock(JdbcStatement.class);

        JdbcConnection conn = mock(JdbcConnection.class);

        if (zoneId != null) {
            when(stmt.getConnection()).thenReturn(conn);
            ConnectionPropertiesImpl connectionProperties = new ConnectionPropertiesImpl();
            connectionProperties.setConnectionTimeZone(zoneId);
            when(conn.connectionProperties()).thenReturn(connectionProperties);
        }

        when(handler.closeAsync(any())).thenReturn(CompletableFuture.completedFuture(new JdbcQueryCloseResult()));

        List<JdbcColumnMeta> columns = new ArrayList<>();
        for (ColumnType columnType : columnTypes) {
            columns.add(new JdbcColumnMeta("C" + columns.size(), columnType));
        }

        BinaryTupleBuilder builder = new BinaryTupleBuilder(columnTypes.size());
        row.accept(builder);
        List<BinaryTupleReader> rows = List.of(new BinaryTupleReader(columnTypes.size(), builder.build()));

        return new JdbcResultSet(handler,
                stmt,
                1L,
                1,
                true,
                rows,
                columns,
                true,
                false,
                0,
                false,
                columns.size(),
                JdbcResultSet.createTransformer(columns)
        );
    }

    private static int nextInt(Random random, int minValue, int bound) {
        return random.nextInt(bound - minValue) + minValue;
    }
}
