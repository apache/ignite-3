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
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
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
            "1234567898765, Europe/Paris,        2009-02-14 00:31:38.765, 2009-02-14, 00:31:38.765",
            "1234567898765, America/Los_Angeles, 2009-02-13 15:31:38.765, 2009-02-13, 15:31:38.765",
            "1234567898765, Asia/Tokyo,          2009-02-14 08:31:38.765, 2009-02-14, 08:31:38.765",

            "-1234567894, Europe/Paris,          1969-12-17 18:03:52.106, 1969-12-17, 18:03:52.106",
            "-1234567894, America/Los_Angeles,   1969-12-17 09:03:52.106, 1969-12-17, 09:03:52.106",
            "-1234567894, Asia/Tokyo,            1969-12-18 02:03:52.106, 1969-12-18, 02:03:52.106",
    })
    public void getStringDateTimeTypes(
            long input,  String zone, 
            String localDateTimeStr, String dateStr, String timeStr
    ) throws SQLException {
        ZoneId zoneId = ZoneId.of(zone);
        int precision = 3;

        Instant now = Instant.ofEpochMilli(input);
        Clock clock = Clock.fixed(now, zoneId);
        Instant instant = clock.instant();
        LocalDate date = LocalDate.now(clock);
        LocalDateTime dateTime = LocalDateTime.now(clock);
        LocalTime time = LocalTime.now(clock);

        List<ColumnType> columns = List.of(ColumnType.TIME, ColumnType.DATE, ColumnType.DATETIME, ColumnType.TIMESTAMP);

        Map<Integer, Integer> precisions = IntStream.range(0, columns.size())
                .mapToObj(i -> Map.entry(i + 1, precision))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue)); 

        try (ResultSet resultSet = createResultSet(zoneId, columns, precisions, (row) -> {
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

            String expected = timeStr
                    + " | " + dateStr
                    + " | " + localDateTimeStr
                    + " | " + localDateTimeStr;

            assertEquals(expected, actual);
        }
    }

    private static Stream<Arguments> getStringDateValues() {
        return Stream.of(
                Arguments.of(LocalDate.of(2100, 1, 4), "2100-01-04"),
                Arguments.of(LocalDate.of(2000, 1, 4), "2000-01-04"),
                Arguments.of(LocalDate.of(1000, 1, 4), "1000-01-04"),
                Arguments.of(LocalDate.of(200, 1, 4), "0200-01-04")
        );
    }

    @ParameterizedTest
    @MethodSource("getStringDateValues")
    public void getStringDate(LocalDate date, String expected) throws SQLException {
        List<ColumnType> columns = List.of(ColumnType.DATE);

        try (ResultSet resultSet = createResultSet(null, columns, (row) -> {
            row.appendDate(date);
        })) {
            resultSet.next();
            String actual = resultSet.getString(1);
            assertEquals(expected, actual);
        }
    }

    private static Stream<Arguments> getStringTimeValues() {
        return Stream.of(
                Arguments.of(LocalTime.of(0, 0, 0, 0), 0, "00:00:00"),
                Arguments.of(LocalTime.of(0, 0, 0, 0), 1, "00:00:00.0"),
                Arguments.of(LocalTime.of(0, 0, 0, 0), 2, "00:00:00.00"),
                Arguments.of(LocalTime.of(0, 0, 0, 0), 3, "00:00:00.000"),
                Arguments.of(LocalTime.of(0, 0, 0, 0), 6, "00:00:00.000000"),
                Arguments.of(LocalTime.of(0, 0, 0, 0), 9, "00:00:00.000000000"),

                Arguments.of(LocalTime.of(13, 5, 2, 123_456), 0, "13:05:02"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_456), 1, "13:05:02.0"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_456), 2, "13:05:02.00"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_456), 3, "13:05:02.000"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_456), 5, "13:05:02.00012"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_456), 6, "13:05:02.000123"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_456), 9, "13:05:02.000123456"),

                Arguments.of(LocalTime.of(13, 5, 2, 12345600), 0, "13:05:02"),
                Arguments.of(LocalTime.of(13, 5, 2, 12345600), 1, "13:05:02.0"),
                Arguments.of(LocalTime.of(13, 5, 2, 12345600), 2, "13:05:02.01"),
                Arguments.of(LocalTime.of(13, 5, 2, 12345600), 3, "13:05:02.012"),
                Arguments.of(LocalTime.of(13, 5, 2, 12345600), 5, "13:05:02.01234"),
                Arguments.of(LocalTime.of(13, 5, 2, 12345600), 6, "13:05:02.012345"),
                Arguments.of(LocalTime.of(13, 5, 2, 12345600), 9, "13:05:02.012345600"),

                Arguments.of(LocalTime.of(13, 5, 2, 123_000_000), 0, "13:05:02"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_000_000), 1, "13:05:02.1"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_000_000), 2, "13:05:02.12"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_000_000), 3, "13:05:02.123"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_000_000), 6, "13:05:02.123000"),
                Arguments.of(LocalTime.of(13, 5, 2, 123_000_000), 9, "13:05:02.123000000"),

                Arguments.of(LocalTime.of(13, 5, 2, 123456789), 0, "13:05:02"),
                Arguments.of(LocalTime.of(13, 5, 2, 123456789), 1, "13:05:02.1"),
                Arguments.of(LocalTime.of(13, 5, 2, 123456789), 2, "13:05:02.12"),
                Arguments.of(LocalTime.of(13, 5, 2, 123456789), 3, "13:05:02.123"),
                Arguments.of(LocalTime.of(13, 5, 2, 123456789), 6, "13:05:02.123456"),
                Arguments.of(LocalTime.of(13, 5, 2, 123456789), 9, "13:05:02.123456789")
        );
    }

    @ParameterizedTest
    @MethodSource("getStringTimeValues")
    public void getStringTimeWithPrecision(LocalTime time, int precision, String expected) throws SQLException {
        List<ColumnType> columns = List.of(ColumnType.TIME);
        Map<Integer, Integer> precisions = Map.of(1, precision);

        try (ResultSet resultSet = createResultSet(null, columns, precisions, (row) -> {
            row.appendTime(time);
        })) {
            resultSet.next();
            String actual = resultSet.getString(1);
            assertEquals(expected, actual);
        }
    }

    private static Stream<Arguments> getStringDateTimeValues() {
        LocalTime time = LocalTime.of(13, 5, 2, 12345600);

        LocalDate date2 = LocalDate.of(2, 5, 17);
        LocalDate date20 = LocalDate.of(20, 5, 17);
        LocalDate date200 = LocalDate.of(200, 5, 17);
        LocalDate date2000 = LocalDate.of(2000, 5, 17);

        return Stream.of(
                // 2
                Arguments.of(LocalDateTime.of(date2, time), 0, "0002-05-17 13:05:02"),
                Arguments.of(LocalDateTime.of(date2, time), 1, "0002-05-17 13:05:02.0"),
                Arguments.of(LocalDateTime.of(date2, time), 2, "0002-05-17 13:05:02.01"),
                Arguments.of(LocalDateTime.of(date2, time), 3, "0002-05-17 13:05:02.012"),
                Arguments.of(LocalDateTime.of(date2, time), 5, "0002-05-17 13:05:02.01234"),
                Arguments.of(LocalDateTime.of(date2, time), 6, "0002-05-17 13:05:02.012345"),
                Arguments.of(LocalDateTime.of(date2, time), 9, "0002-05-17 13:05:02.012345600"),

                // 20
                Arguments.of(LocalDateTime.of(date20, time), 0, "0020-05-17 13:05:02"),
                Arguments.of(LocalDateTime.of(date20, time), 1, "0020-05-17 13:05:02.0"),
                Arguments.of(LocalDateTime.of(date20, time), 2, "0020-05-17 13:05:02.01"),
                Arguments.of(LocalDateTime.of(date20, time), 3, "0020-05-17 13:05:02.012"),
                Arguments.of(LocalDateTime.of(date20, time), 5, "0020-05-17 13:05:02.01234"),
                Arguments.of(LocalDateTime.of(date20, time), 6, "0020-05-17 13:05:02.012345"),
                Arguments.of(LocalDateTime.of(date20, time), 9, "0020-05-17 13:05:02.012345600"),

                // 200
                Arguments.of(LocalDateTime.of(date200, time), 0, "0200-05-17 13:05:02"),
                Arguments.of(LocalDateTime.of(date200, time), 1, "0200-05-17 13:05:02.0"),
                Arguments.of(LocalDateTime.of(date200, time), 2, "0200-05-17 13:05:02.01"),
                Arguments.of(LocalDateTime.of(date200, time), 3, "0200-05-17 13:05:02.012"),
                Arguments.of(LocalDateTime.of(date200, time), 5, "0200-05-17 13:05:02.01234"),
                Arguments.of(LocalDateTime.of(date200, time), 6, "0200-05-17 13:05:02.012345"),
                Arguments.of(LocalDateTime.of(date200, time), 9, "0200-05-17 13:05:02.012345600"),

                // 2000
                Arguments.of(LocalDateTime.of(date2000, time), 0, "2000-05-17 13:05:02"),
                Arguments.of(LocalDateTime.of(date2000, time), 1, "2000-05-17 13:05:02.0"),
                Arguments.of(LocalDateTime.of(date2000, time), 2, "2000-05-17 13:05:02.01"),
                Arguments.of(LocalDateTime.of(date2000, time), 3, "2000-05-17 13:05:02.012"),
                Arguments.of(LocalDateTime.of(date2000, time), 5, "2000-05-17 13:05:02.01234"),
                Arguments.of(LocalDateTime.of(date2000, time), 6, "2000-05-17 13:05:02.012345"),
                Arguments.of(LocalDateTime.of(date2000, time), 9, "2000-05-17 13:05:02.012345600")
        );
    }

    @ParameterizedTest
    @MethodSource("getStringDateTimeValues")
    public void getStringDateTimeWithPrecision(LocalDateTime value, int precision, String expected) throws SQLException {
        List<ColumnType> columns = List.of(ColumnType.DATETIME);
        Map<Integer, Integer> precisions = Map.of(1, precision);

        try (ResultSet resultSet = createResultSet(null, columns, precisions, (row) -> {
            row.appendDateTime(value);
        })) {
            resultSet.next();
            String actual = resultSet.getString(1);
            assertEquals(expected, actual);
        }
    }

    @ParameterizedTest
    @MethodSource("getStringDateTimeValues")
    public void getStringTimestampWithPrecision(LocalDateTime value, int precision, String expected) throws SQLException {
        ZoneOffset zoneOffset = ZoneOffset.ofHoursMinutes(3, 30);

        List<ColumnType> columns = List.of(ColumnType.TIMESTAMP);
        Map<Integer, Integer> precisions = Map.of(1, precision);

        try (ResultSet resultSet = createResultSet(zoneOffset, columns, precisions, (row) -> {
            Instant instant = value.toInstant(zoneOffset);
            row.appendTimestamp(instant);
        })) {
            resultSet.next();
            String actual = resultSet.getString(1);
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
        return createResultSet(zoneId, columnTypes, Map.of(), row);
    }

    private static JdbcResultSet createResultSet(
            @Nullable ZoneId zoneId,
            List<ColumnType> columnTypes,
            Map<Integer, Integer> precisions,
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
            String columnName = "C" + columns.size();
            int precision = precisions.getOrDefault(columns.size() + 1, -1);

            JdbcColumnMeta columnMeta = new JdbcColumnMeta(
                    columnName, 
                    "Schema", 
                    "Table",
                    columnName,
                    columnType,
                    precision, 
                    -1,
                    true
            );
            columns.add(columnMeta);
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
