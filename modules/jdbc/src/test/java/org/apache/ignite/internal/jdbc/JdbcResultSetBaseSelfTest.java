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
import java.math.MathContext;
import java.math.RoundingMode;
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
import java.util.stream.Stream;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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

    private static ColumnType columnTypefromObject(Object value) {
        if (value == null) {
            return ColumnType.NULL;
        }
        for (ColumnType columnType : ColumnType.values()) {
            if (columnType.javaClass().equals(value.getClass())) {
                return columnType;
            }
        }
        throw new IllegalArgumentException("No column type for " + value);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void getBoolean(boolean boolValue) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), boolValue)) {
            assertTrue(rs.next());

            String strVal = boolValue ? "true" : "false";
            int value = boolValue ? 1 : 0;

            assertEquals(boolValue, rs.getBoolean(1));
            assertEquals(boolValue, rs.getBoolean("C"));

            assertEquals(value, rs.getByte(1));
            assertEquals(value, rs.getByte("C"));

            assertEquals(value, rs.getShort(1));
            assertEquals(value, rs.getShort("C"));

            assertEquals(value, rs.getInt(1));
            assertEquals(value, rs.getInt("C"));

            assertEquals(value, rs.getLong(1));
            assertEquals(value, rs.getLong("C"));

            expectSqlConversionError(() -> rs.getFloat(1), "float");
            expectSqlConversionError(() -> rs.getFloat("C"), "float");

            expectSqlConversionError(() -> rs.getDouble(1), "double");
            expectSqlConversionError(() -> rs.getDouble("C"), "double");

            expectSqlConversionError(() -> rs.getBigDecimal(1), "BigDecimal");
            expectSqlConversionError(() -> rs.getBigDecimal("C"), "BigDecimal");

            //noinspection deprecation
            expectSqlConversionError(() -> rs.getBigDecimal(1, 2), "BigDecimal");
            //noinspection deprecation
            expectSqlConversionError(() -> rs.getBigDecimal("C", 2), "BigDecimal");

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(boolValue, rs.getObject(1));
            assertEquals(boolValue, rs.getObject("C"));

            assertEquals(Byte.valueOf((byte) value), rs.getObject(1, Byte.class));
            assertEquals(Byte.valueOf((byte) value), rs.getObject("C", Byte.class));

            assertEquals(Short.valueOf((short) value), rs.getObject(1, Short.class));
            assertEquals(Short.valueOf((short) value), rs.getObject("C", Short.class));

            assertEquals(Integer.valueOf(value), rs.getObject(1, Integer.class));
            assertEquals(Integer.valueOf(value), rs.getObject("C", Integer.class));

            assertEquals(Long.valueOf(value), rs.getObject(1, Long.class));
            assertEquals(Long.valueOf(value), rs.getObject("C", Long.class));

            expectSqlConversionError(() -> rs.getObject(1, Float.class), "float");
            expectSqlConversionError(() -> rs.getObject("C", Float.class), "float");

            expectSqlConversionError(() -> rs.getObject(1, Double.class), "double");
            expectSqlConversionError(() -> rs.getObject("C", Double.class), "double");

            expectSqlConversionError(() -> rs.getObject(1, BigDecimal.class), "BigDecimal");
            expectSqlConversionError(() -> rs.getObject("C", BigDecimal.class), "BigDecimal");

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "true, 1, true",
            "true, 0, false",
            "false, -1, ",
            "true, 0, ",
            "false, -129, ",
            "false, 128, ",
            "false, xyz, ",
            "false,, "
    })
    public void getBooleanFromString(boolean valid, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                String error = "Cannot convert to boolean: " + value;

                expectSqlException(() -> rs.getBoolean(1), error);
                expectSqlException(() -> rs.getBoolean("C"), error);

                expectSqlException(() -> rs.getObject(1, Boolean.class), error);
                expectSqlException(() -> rs.getObject("C", Boolean.class), error);

            } else {
                boolean expected = Boolean.parseBoolean(result);

                assertEquals(expected, rs.getBoolean(1));
                assertEquals(expected, rs.getBoolean("C"));

                assertEquals(expected, rs.getObject(1, Boolean.class));
                assertEquals(expected, rs.getObject("C", Boolean.class));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {-42, -1, 0, 1, 42})
    public void getByte(byte value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.INT8, 0, 0, false), value)) {
            assertTrue(rs.next());

            BigDecimal decimalVal = BigDecimal.valueOf(value);
            BigDecimal decimalScaledVal = BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
            String strVal = String.valueOf(value);

            if (value == 0) {
                assertFalse(rs.getBoolean(1));
                assertFalse(rs.getBoolean("C"));
            } else if (value == 1) {
                assertTrue(rs.getBoolean(1));
                assertTrue(rs.getBoolean("C"));
            } else {
                expectSqlConversionError(() -> rs.getBoolean(1), "boolean");
                expectSqlConversionError(() -> rs.getBoolean("C"), "boolean");
            }

            assertEquals(value, rs.getByte(1));
            assertEquals(value, rs.getByte("C"));

            assertEquals(value, rs.getShort(1));
            assertEquals(value, rs.getShort("C"));

            assertEquals(value, rs.getInt(1));
            assertEquals(value, rs.getInt("C"));

            assertEquals(value, rs.getLong(1));
            assertEquals(value, rs.getLong("C"));

            assertEquals(value, rs.getFloat(1));
            assertEquals(value, rs.getFloat("C"));

            assertEquals(value, rs.getDouble(1));
            assertEquals(value, rs.getDouble("C"));

            assertEquals(decimalVal, rs.getBigDecimal(1));
            assertEquals(decimalVal, rs.getBigDecimal("C"));

            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal(1, 2));
            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal("C", 2));

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(value, rs.getObject(1));
            assertEquals(value, rs.getObject("C"));

            assertEquals(Byte.valueOf(value), rs.getObject(1, Byte.class));
            assertEquals(Byte.valueOf(value), rs.getObject("C", Byte.class));

            assertEquals(Short.valueOf(value), rs.getObject(1, Short.class));
            assertEquals(Short.valueOf(value), rs.getObject("C", Short.class));

            assertEquals(Integer.valueOf(value), rs.getObject(1, Integer.class));
            assertEquals(Integer.valueOf(value), rs.getObject("C", Integer.class));

            assertEquals(Long.valueOf(value), rs.getObject(1, Long.class));
            assertEquals(Long.valueOf(value), rs.getObject("C", Long.class));

            assertEquals(Float.valueOf(value), rs.getObject(1, Float.class));
            assertEquals(Float.valueOf(value), rs.getObject("C", Float.class));

            assertEquals(Double.valueOf(value), rs.getObject(1, Double.class));
            assertEquals(Double.valueOf(value), rs.getObject("C", Double.class));

            assertEquals(decimalVal, rs.getObject(1, BigDecimal.class));
            assertEquals(decimalVal, rs.getObject("C", BigDecimal.class));

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "true, -128, -128",
            "true, 127, 127",
            "true, -42, -42",
            "true, 42, 42",
            "false, -129, ",
            "false, 128, ",
            "false, xyz, ",
            "false,, "
    })
    public void getByteFromString(boolean valid, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                String error = "Cannot convert to byte: " + value;

                expectSqlException(() -> rs.getByte(1), error);
                expectSqlException(() -> rs.getByte("C"), error);

                expectSqlException(() -> rs.getObject(1, Byte.class), error);
                expectSqlException(() -> rs.getObject("C", Byte.class), error);
            } else {
                byte expected = Byte.parseByte(result);
                assertEquals(expected, rs.getByte(1));
                assertEquals(expected, rs.getByte("C"));

                assertEquals(expected, rs.getObject(1, Byte.class));
                assertEquals(expected, rs.getObject("C", Byte.class));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void getByteFromBoolean(boolean value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (value) {
                assertEquals(1, rs.getByte(1));
                assertEquals(1, rs.getByte("C"));
            } else {
                assertEquals(0, rs.getByte(1));
                assertEquals(0, rs.getByte("C"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getByteNumbers")
    public void getByteFromNumber(boolean valid, Number value) throws SQLException {
        ColumnType columnType = columnTypefromObject(value);

        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (valid) {
                assertEquals(value.byteValue(), rs.getByte(1));
                assertEquals(value.byteValue(), rs.getByte("C"));

                assertEquals(value.byteValue(), rs.getObject(1, Byte.class));
                assertEquals(value.byteValue(), rs.getObject("C", Byte.class));
            } else {
                expectSqlConversionError(() -> rs.getByte(1), "byte");
                expectSqlConversionError(() -> rs.getByte("C"), "byte");

                expectSqlConversionError(() -> rs.getObject(1, Byte.class), "byte");
                expectSqlConversionError(() -> rs.getObject("C", Byte.class), "byte");
            }
        }
    }

    private static Stream<Arguments> getByteNumbers() {
        return Stream.of(
                Arguments.of(true, Byte.MAX_VALUE),
                Arguments.of(true, Byte.MIN_VALUE),

                Arguments.of(true, (short) 42),
                Arguments.of(false, (short) (Byte.MAX_VALUE + 1)),
                Arguments.of(false, (short) (Byte.MIN_VALUE - 1)),

                Arguments.of(true, 42),
                Arguments.of(true, 42L),

                Arguments.of(true, 42.0f),
                Arguments.of(false, (float) (Byte.MAX_VALUE + 1L)),
                Arguments.of(false, (float) (Byte.MIN_VALUE - 1L)),
                Arguments.of(false, Float.NaN),
                Arguments.of(false, Float.NEGATIVE_INFINITY),
                Arguments.of(false, Float.POSITIVE_INFINITY),

                Arguments.of(true, 42.0d),
                Arguments.of(false, (double) (Byte.MAX_VALUE + 1L)),
                Arguments.of(false, (double) (Byte.MIN_VALUE - 1L)),
                Arguments.of(false, Double.NaN),
                Arguments.of(false, Double.NEGATIVE_INFINITY),
                Arguments.of(false, Double.POSITIVE_INFINITY),

                Arguments.of(true, BigDecimal.valueOf(42.1)),
                Arguments.of(true, BigDecimal.valueOf(42.9)),
                Arguments.of(true, BigDecimal.valueOf(Byte.MAX_VALUE)),
                Arguments.of(true, BigDecimal.valueOf(Byte.MIN_VALUE)),
                Arguments.of(false, BigDecimal.valueOf((short) (Byte.MAX_VALUE + 1))),
                Arguments.of(false, BigDecimal.valueOf((short) (Byte.MIN_VALUE - 1)))
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {-42, -1, 0, 1, 42})
    public void getShort(short value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.INT16, 0, 0, false), value)) {
            assertTrue(rs.next());

            BigDecimal decimalVal = BigDecimal.valueOf(value);
            BigDecimal decimalScaledVal = BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
            String strVal = String.valueOf(value);

            if (value == 0) {
                assertFalse(rs.getBoolean(1));
                assertFalse(rs.getBoolean("C"));
            } else if (value == 1) {
                assertTrue(rs.getBoolean(1));
                assertTrue(rs.getBoolean("C"));
            } else {
                expectSqlConversionError(() -> rs.getBoolean(1), "boolean");
                expectSqlConversionError(() -> rs.getBoolean("C"), "boolean");
            }

            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte("C"));

            assertEquals(value, rs.getShort(1));
            assertEquals(value, rs.getShort("C"));

            assertEquals(value, rs.getInt(1));
            assertEquals(value, rs.getInt("C"));

            assertEquals(value, rs.getLong(1));
            assertEquals(value, rs.getLong("C"));

            assertEquals(value, rs.getFloat(1));
            assertEquals(value, rs.getFloat("C"));

            assertEquals(value, rs.getDouble(1));
            assertEquals(value, rs.getDouble("C"));

            assertEquals(decimalVal, rs.getBigDecimal(1));
            assertEquals(decimalVal, rs.getBigDecimal("C"));

            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal(1, 2));
            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal("C", 2));

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(value, rs.getObject(1));
            assertEquals(value, rs.getObject("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject(1, Byte.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject("C", Byte.class));

            assertEquals(Short.valueOf(value), rs.getObject(1, Short.class));
            assertEquals(Short.valueOf(value), rs.getObject("C", Short.class));

            assertEquals(Integer.valueOf(value), rs.getObject(1, Integer.class));
            assertEquals(Integer.valueOf(value), rs.getObject("C", Integer.class));

            assertEquals(Long.valueOf(value), rs.getObject(1, Long.class));
            assertEquals(Long.valueOf(value), rs.getObject("C", Long.class));

            assertEquals(Float.valueOf(value), rs.getObject(1, Float.class));
            assertEquals(Float.valueOf(value), rs.getObject("C", Float.class));

            assertEquals(Double.valueOf(value), rs.getObject(1, Double.class));
            assertEquals(Double.valueOf(value), rs.getObject("C", Double.class));

            assertEquals(decimalVal, rs.getObject(1, BigDecimal.class));
            assertEquals(decimalVal, rs.getObject("C", BigDecimal.class));

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "true, -32768, -32768",
            "true, 32767, 32767",
            "true, -42, -42",
            "true, 42, 42",
            "false, -32769, ",
            "false, 32768, ",
            "false, xyz, ",
            "false,, "
    })
    public void getShortFromString(boolean valid, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                String error = "Cannot convert to short: " + value;

                expectSqlException(() -> rs.getShort(1), error);
                expectSqlException(() -> rs.getShort("C"), error);

                expectSqlException(() -> rs.getObject(1, Short.class), error);
                expectSqlException(() -> rs.getObject("C", Short.class), error);
            } else {
                short expected = Short.parseShort(result);

                assertEquals(expected, rs.getShort(1));
                assertEquals(expected, rs.getShort("C"));

                assertEquals(expected, rs.getObject(1, Short.class));
                assertEquals(expected, rs.getObject("C", Short.class));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void getShortFromBoolean(boolean value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (value) {
                assertEquals(1, rs.getShort(1));
                assertEquals(1, rs.getShort("C"));
            } else {
                assertEquals(0, rs.getShort(1));
                assertEquals(0, rs.getShort("C"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getShortNumbers")
    public void getShortFromNumber(boolean valid, Number value) throws SQLException {
        ColumnType columnType = columnTypefromObject(value);

        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (valid) {
                assertEquals(value.shortValue(), rs.getShort(1));
                assertEquals(value.shortValue(), rs.getShort("C"));

                assertEquals(value.shortValue(), rs.getObject(1, Short.class));
                assertEquals(value.shortValue(), rs.getObject("C", Short.class));
            } else {
                expectSqlConversionError(() -> rs.getShort(1), "short");
                expectSqlConversionError(() -> rs.getShort("C"), "short");

                expectSqlConversionError(() -> rs.getObject(1, Short.class), "short");
                expectSqlConversionError(() -> rs.getObject("C", Short.class), "short");
            }
        }
    }

    private static Stream<Arguments> getShortNumbers() {
        return Stream.of(
                Arguments.of(true, (byte) 42),

                Arguments.of(true, Short.MAX_VALUE),
                Arguments.of(true, Short.MIN_VALUE),

                Arguments.of(false, Short.MAX_VALUE + 1),
                Arguments.of(false, Short.MIN_VALUE - 1),

                Arguments.of(true, 42),
                Arguments.of(true, 42L),

                Arguments.of(true, 42.0f),
                Arguments.of(false, (float) (Short.MAX_VALUE + 1L)),
                Arguments.of(false, (float) (Short.MIN_VALUE - 1L)),
                Arguments.of(false, Float.NaN),
                Arguments.of(false, Float.NEGATIVE_INFINITY),
                Arguments.of(false, Float.POSITIVE_INFINITY),

                Arguments.of(true, 42.0d),
                Arguments.of(false, (double) (Short.MAX_VALUE + 1L)),
                Arguments.of(false, (double) (Short.MIN_VALUE - 1L)),
                Arguments.of(false, Double.NaN),
                Arguments.of(false, Double.NEGATIVE_INFINITY),
                Arguments.of(false, Double.POSITIVE_INFINITY),

                Arguments.of(true, BigDecimal.valueOf(42.1)),
                Arguments.of(true, BigDecimal.valueOf(42.9)),
                Arguments.of(true, BigDecimal.valueOf(Short.MAX_VALUE)),
                Arguments.of(true, BigDecimal.valueOf(Short.MIN_VALUE)),
                Arguments.of(false, BigDecimal.valueOf(Short.MAX_VALUE + 1)),
                Arguments.of(false, BigDecimal.valueOf(Short.MIN_VALUE - 1))
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {-42, -1, 0, 1, 42})
    public void getInt(int value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.INT32, 0, 0, false), value)) {
            assertTrue(rs.next());

            BigDecimal decimalVal = BigDecimal.valueOf(value);
            BigDecimal decimalScaledVal = BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
            String strVal = String.valueOf(value);

            if (value == 0) {
                assertFalse(rs.getBoolean(1));
                assertFalse(rs.getBoolean("C"));
            } else if (value == 1) {
                assertTrue(rs.getBoolean(1));
                assertTrue(rs.getBoolean("C"));
            } else {
                expectSqlConversionError(() -> rs.getBoolean(1), "boolean");
                expectSqlConversionError(() -> rs.getBoolean("C"), "boolean");
            }

            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((short) value, rs.getShort(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((short) value, rs.getShort("C"));

            assertEquals(value, rs.getInt(1));
            assertEquals(value, rs.getInt("C"));

            assertEquals(value, rs.getLong(1));
            assertEquals(value, rs.getLong("C"));

            assertEquals(value, rs.getFloat(1));
            assertEquals(value, rs.getFloat("C"));

            assertEquals(value, rs.getDouble(1));
            assertEquals(value, rs.getDouble("C"));

            assertEquals(decimalVal, rs.getBigDecimal(1));
            assertEquals(decimalVal, rs.getBigDecimal("C"));

            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal(1, 2));
            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal("C", 2));

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(value, rs.getObject(1));
            assertEquals(value, rs.getObject("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject(1, Byte.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject("C", Byte.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Short.valueOf((short) value), rs.getObject(1, Short.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Short.valueOf((short) value), rs.getObject("C", Short.class));

            assertEquals(Integer.valueOf(value), rs.getObject(1, Integer.class));
            assertEquals(Integer.valueOf(value), rs.getObject("C", Integer.class));

            assertEquals(Long.valueOf(value), rs.getObject(1, Long.class));
            assertEquals(Long.valueOf(value), rs.getObject("C", Long.class));

            assertEquals(Float.valueOf(value), rs.getObject(1, Float.class));
            assertEquals(Float.valueOf(value), rs.getObject("C", Float.class));

            assertEquals(Double.valueOf(value), rs.getObject(1, Double.class));
            assertEquals(Double.valueOf(value), rs.getObject("C", Double.class));

            assertEquals(decimalVal, rs.getObject(1, BigDecimal.class));
            assertEquals(decimalVal, rs.getObject("C", BigDecimal.class));

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "true, -2147483648, -2147483648",
            "true, 2147483647, 2147483647",
            "true, -42, -42",
            "true, 42, 42",
            "false, -2147483649, ",
            "false, 2147483648, ",
            "false, xyz, ",
            "false,, "
    })
    public void getIntFromString(boolean valid, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                String error = "Cannot convert to int: " + value;

                expectSqlException(() -> rs.getInt(1), error);
                expectSqlException(() -> rs.getInt("C"), error);

                expectSqlException(() -> rs.getObject(1, Integer.class), error);
                expectSqlException(() -> rs.getObject("C", Integer.class), error);
            } else {
                int expected = Integer.parseInt(result);

                assertEquals(expected, rs.getInt(1));
                assertEquals(expected, rs.getInt("C"));

                assertEquals(expected, rs.getObject(1, Integer.class));
                assertEquals(expected, rs.getObject("C", Integer.class));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void getIntFromBoolean(boolean value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (value) {
                assertEquals(1, rs.getInt(1));
                assertEquals(1, rs.getInt("C"));
            } else {
                assertEquals(0, rs.getInt(1));
                assertEquals(0, rs.getInt("C"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getIntNumbers")
    public void getIntFromNumber(boolean valid, Number value) throws SQLException {
        ColumnType columnType = columnTypefromObject(value);

        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (valid) {
                assertEquals(value.intValue(), rs.getInt(1));
                assertEquals(value.intValue(), rs.getInt("C"));

                assertEquals(value.intValue(), rs.getObject(1, Integer.class));
                assertEquals(value.intValue(), rs.getObject("C", Integer.class));
            } else {
                expectSqlConversionError(() -> rs.getInt(1), "int");
                expectSqlConversionError(() -> rs.getInt("C"), "int");

                expectSqlConversionError(() -> rs.getObject(1, Integer.class), "int");
                expectSqlConversionError(() -> rs.getObject("C", Integer.class), "int");
            }
        }
    }

    private static Stream<Arguments> getIntNumbers() {
        return Stream.of(
                Arguments.of(true, (byte) 42),
                Arguments.of(true, (short) 42),

                Arguments.of(true, Integer.MAX_VALUE),
                Arguments.of(true, Integer.MIN_VALUE),

                Arguments.of(true, 42L),
                Arguments.of(false, Integer.MAX_VALUE + 1L),
                Arguments.of(false, Integer.MIN_VALUE - 1L),

                Arguments.of(true, 42.0f),
                Arguments.of(false, Float.NaN),
                Arguments.of(false, Float.NEGATIVE_INFINITY),
                Arguments.of(false, Float.POSITIVE_INFINITY),

                Arguments.of(true, 42.0d),
                Arguments.of(false, (double) (Integer.MAX_VALUE + 1L)),
                Arguments.of(false, (double) (Integer.MIN_VALUE - 1L)),
                Arguments.of(false, Double.NaN),
                Arguments.of(false, Double.NEGATIVE_INFINITY),
                Arguments.of(false, Double.POSITIVE_INFINITY),

                Arguments.of(true, BigDecimal.valueOf(42)),
                Arguments.of(true, BigDecimal.valueOf(42.1)),
                Arguments.of(true, BigDecimal.valueOf(42.9)),
                Arguments.of(true, BigDecimal.valueOf(Integer.MAX_VALUE)),
                Arguments.of(true, BigDecimal.valueOf(Integer.MIN_VALUE)),
                Arguments.of(false, BigDecimal.valueOf(Integer.MAX_VALUE + 1L)),
                Arguments.of(false, BigDecimal.valueOf(Integer.MIN_VALUE - 1L))
        );
    }

    @ParameterizedTest
    @ValueSource(longs = {-42, -1, 0, 1, 42})
    public void getLong(long value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.INT64, 0, 0, false), value)) {
            assertTrue(rs.next());

            BigDecimal decimalVal = BigDecimal.valueOf(value);
            BigDecimal decimalScaledVal = BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
            String strVal = String.valueOf(value);

            if (value == 0) {
                assertFalse(rs.getBoolean(1));
                assertFalse(rs.getBoolean("C"));
            } else if (value == 1) {
                assertTrue(rs.getBoolean(1));
                assertTrue(rs.getBoolean("C"));
            } else {
                expectSqlConversionError(() -> rs.getBoolean(1), "boolean");
                expectSqlConversionError(() -> rs.getBoolean("C"), "boolean");
            }

            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((short) value, rs.getShort(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((short) value, rs.getShort("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((int) value, rs.getInt(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((int) value, rs.getInt("C"));

            assertEquals(value, rs.getLong(1));
            assertEquals(value, rs.getLong("C"));

            assertEquals(value, rs.getFloat(1));
            assertEquals(value, rs.getFloat("C"));

            assertEquals(value, rs.getDouble(1));
            assertEquals(value, rs.getDouble("C"));

            assertEquals(decimalVal, rs.getBigDecimal(1));
            assertEquals(decimalVal, rs.getBigDecimal("C"));

            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal(1, 2));
            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal("C", 2));

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(value, rs.getObject(1));
            assertEquals(value, rs.getObject("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject(1, Byte.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject("C", Byte.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Short.valueOf((short) value), rs.getObject(1, Short.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Short.valueOf((short) value), rs.getObject("C", Short.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Integer.valueOf((int) value), rs.getObject(1, Integer.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Integer.valueOf((int) value), rs.getObject("C", Integer.class));

            assertEquals(Long.valueOf(value), rs.getObject(1, Long.class));
            assertEquals(Long.valueOf(value), rs.getObject("C", Long.class));

            assertEquals(Float.valueOf(value), rs.getObject(1, Float.class));
            assertEquals(Float.valueOf(value), rs.getObject("C", Float.class));

            assertEquals(Double.valueOf(value), rs.getObject(1, Double.class));
            assertEquals(Double.valueOf(value), rs.getObject("C", Double.class));

            assertEquals(decimalVal, rs.getObject(1, BigDecimal.class));
            assertEquals(decimalVal, rs.getObject("C", BigDecimal.class));

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "true, -9223372036854775808, -9223372036854775808",
            "true, 9223372036854775807, 9223372036854775807",
            "true, -42, -42",
            "true, 42, 42",
            "false, -9223372036854775809, ",
            "false, 9223372036854775808, ",
            "false, xyz, ",
            "false,, "
    })
    public void getLongFromString(boolean valid, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                String error = "Cannot convert to long: " + value;

                expectSqlException(() -> rs.getLong(1), error);
                expectSqlException(() -> rs.getLong("C"), error);

                expectSqlException(() -> rs.getObject(1, Long.class), error);
                expectSqlException(() -> rs.getObject("C", Long.class), error);
            } else {
                long expected = Long.parseLong(result);

                assertEquals(expected, rs.getLong(1));
                assertEquals(expected, rs.getLong("C"));

                assertEquals(expected, rs.getObject(1, Long.class));
                assertEquals(expected, rs.getObject("C", Long.class));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void getLongFromBoolean(boolean value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.BOOLEAN, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (value) {
                assertEquals(1, rs.getLong(1));
                assertEquals(1, rs.getLong("C"));
            } else {
                assertEquals(0, rs.getLong(1));
                assertEquals(0, rs.getLong("C"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getLongNumbers")
    public void getLongFromNumber(boolean valid, Number value) throws SQLException {
        ColumnType columnType = columnTypefromObject(value);

        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (valid) {
                assertEquals(value.longValue(), rs.getLong(1));
                assertEquals(value.longValue(), rs.getLong("C"));

                assertEquals(value.longValue(), rs.getObject(1, Long.class));
                assertEquals(value.longValue(), rs.getObject("C", Long.class));
            } else {
                expectSqlConversionError(() -> rs.getLong(1), "long");
                expectSqlConversionError(() -> rs.getLong("C"), "long");

                expectSqlConversionError(() -> rs.getObject(1, Long.class), "long");
                expectSqlConversionError(() -> rs.getObject("C", Long.class), "long");
            }
        }
    }

    private static Stream<Arguments> getLongNumbers() {
        return Stream.of(
                Arguments.of(true, (byte) 42),
                Arguments.of(true, (short) 42),

                Arguments.of(true, 42),
                Arguments.of(true, Long.MAX_VALUE),
                Arguments.of(true, Long.MIN_VALUE),

                Arguments.of(true, 42.0f),
                Arguments.of(true, (float) (Long.MAX_VALUE)),
                Arguments.of(true, (float) (Long.MIN_VALUE)),
                Arguments.of(false, Float.NaN),
                Arguments.of(false, Float.NEGATIVE_INFINITY),
                Arguments.of(false, Float.POSITIVE_INFINITY),

                Arguments.of(true, 42.0d),
                Arguments.of(true, (double) (Long.MAX_VALUE)),
                Arguments.of(true, (double) (Long.MIN_VALUE)),
                Arguments.of(false, Double.NaN),
                Arguments.of(false, Double.NEGATIVE_INFINITY),
                Arguments.of(false, Double.POSITIVE_INFINITY),

                Arguments.of(true, BigDecimal.valueOf(42)),
                Arguments.of(true, BigDecimal.valueOf(42.1)),
                Arguments.of(true, BigDecimal.valueOf(42.9)),
                Arguments.of(true, BigDecimal.valueOf(Long.MAX_VALUE)),
                Arguments.of(true, BigDecimal.valueOf(Long.MIN_VALUE)),
                Arguments.of(false, BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE)),
                Arguments.of(false, BigDecimal.valueOf(Long.MIN_VALUE).add(BigDecimal.ONE.negate()))
        );
    }

    @ParameterizedTest
    @ValueSource(floats = {-42.3f, 42.9f})
    public void getFloat(float value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.FLOAT, 0, 0, false), value)) {
            assertTrue(rs.next());

            BigDecimal decimalVal = new BigDecimal(value);
            BigDecimal decimalScaledVal = new BigDecimal(value).setScale(2, RoundingMode.HALF_UP);
            String strVal = String.valueOf(value);

            expectSqlConversionError(() -> rs.getBoolean(1), "boolean");
            expectSqlConversionError(() -> rs.getBoolean("C"), "boolean");

            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((short) value, rs.getShort(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((short) value, rs.getShort("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((int) value, rs.getInt(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((int) value, rs.getInt("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((long) value, rs.getLong(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((long) value, rs.getLong("C"));

            assertEquals(value, rs.getFloat(1));
            assertEquals(value, rs.getFloat("C"));

            assertEquals(value, rs.getDouble(1));
            assertEquals(value, rs.getDouble("C"));

            assertEquals(decimalVal, rs.getBigDecimal(1));
            assertEquals(decimalVal, rs.getBigDecimal("C"));

            assertEquals(decimalScaledVal, rs.getBigDecimal(1, 2));
            assertEquals(decimalScaledVal, rs.getBigDecimal("C", 2));

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(value, rs.getObject(1));
            assertEquals(value, rs.getObject("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject(1, Byte.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject("C", Byte.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Short.valueOf((short) value), rs.getObject(1, Short.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Short.valueOf((short) value), rs.getObject("C", Short.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Integer.valueOf((int) value), rs.getObject(1, Integer.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Integer.valueOf((int) value), rs.getObject("C", Integer.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Long.valueOf((long) value), rs.getObject(1, Long.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Long.valueOf((long) value), rs.getObject("C", Long.class));

            assertEquals(Float.valueOf(value), rs.getObject(1, Float.class));
            assertEquals(Float.valueOf(value), rs.getObject("C", Float.class));

            assertEquals(Double.valueOf(value), rs.getObject(1, Double.class));
            assertEquals(Double.valueOf(value), rs.getObject("C", Double.class));

            assertEquals(decimalVal, rs.getObject(1, BigDecimal.class));
            assertEquals(decimalVal, rs.getObject("C", BigDecimal.class));

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "true, -9223372036854775808, -9223372036854775808",
            "true, 9223372036854775807, 9223372036854775807",
            "true, -42.43, -42.43",
            "true, 42.43, 42.43",
            "false, xyz, ",
            "false,, "
    })
    public void getFloatFromString(boolean valid, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                String error = "Cannot convert to float: " + value;

                expectSqlException(() -> rs.getFloat(1), error);
                expectSqlException(() -> rs.getFloat("C"), error);

                expectSqlException(() -> rs.getObject(1, Float.class), error);
                expectSqlException(() -> rs.getObject("C", Float.class), error);
            } else {
                float expected = Float.parseFloat(result);

                assertEquals(expected, rs.getFloat(1));
                assertEquals(expected, rs.getFloat("C"));

                assertEquals(expected, rs.getObject(1, Float.class));
                assertEquals(expected, rs.getObject("C", Float.class));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getFloatNumbers")
    public void getFloatFromNumber(boolean valid, Number value) throws SQLException {
        ColumnType columnType = columnTypefromObject(value);

        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (valid) {
                assertEquals(value.floatValue(), rs.getFloat(1));
                assertEquals(value.floatValue(), rs.getFloat("C"));

                assertEquals(value.floatValue(), rs.getObject(1, Float.class));
                assertEquals(value.floatValue(), rs.getObject("C", Float.class));
            } else {
                expectSqlConversionError(() -> rs.getFloat(1), "float");
                expectSqlConversionError(() -> rs.getFloat("C"), "float");

                expectSqlConversionError(() -> rs.getObject(1, Float.class), "float");
                expectSqlConversionError(() -> rs.getObject("C", Float.class), "float");
            }
        }
    }

    private static Stream<Arguments> getFloatNumbers() {
        return Stream.of(
                Arguments.of(true, (byte) 42),
                Arguments.of(true, (short) 42),

                Arguments.of(true, 42),
                Arguments.of(true, Long.MAX_VALUE),
                Arguments.of(true, Long.MIN_VALUE),

                Arguments.of(true, 42.0f),
                Arguments.of(true, (float) (Long.MAX_VALUE)),
                Arguments.of(true, (float) (Long.MIN_VALUE)),
                Arguments.of(true, Float.NaN),
                Arguments.of(true, Float.NEGATIVE_INFINITY),
                Arguments.of(true, Float.POSITIVE_INFINITY),

                Arguments.of(true, 42.0d),
                Arguments.of(true, (double) (Long.MAX_VALUE)),
                Arguments.of(true, (double) (Long.MIN_VALUE)),
                Arguments.of(true, Double.NaN),
                Arguments.of(false, Double.NEGATIVE_INFINITY),
                Arguments.of(false, Double.POSITIVE_INFINITY),

                Arguments.of(true, BigDecimal.valueOf(42)),
                Arguments.of(true, BigDecimal.valueOf(Float.MAX_VALUE)),
                Arguments.of(true, BigDecimal.valueOf(-Float.MAX_VALUE)),
                Arguments.of(false, BigDecimal.valueOf(Double.MAX_VALUE)),
                Arguments.of(false, BigDecimal.valueOf(-Double.MAX_VALUE))
        );
    }

    @ParameterizedTest
    @ValueSource(doubles = {-42.3d, 0, 42.9d})
    public void getDouble(double value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.DOUBLE, 0, 0, false), value)) {
            assertTrue(rs.next());

            BigDecimal decimalVal = new BigDecimal(value);
            BigDecimal decimalScaledVal = new BigDecimal(value).setScale(2, RoundingMode.HALF_UP);
            String strVal = String.valueOf(value);

            expectSqlConversionError(() -> rs.getBoolean(1), "boolean");
            expectSqlConversionError(() -> rs.getBoolean("C"), "boolean");

            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((byte) value, rs.getByte("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((short) value, rs.getShort(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((short) value, rs.getShort("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((int) value, rs.getInt(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((int) value, rs.getInt("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals((long) value, rs.getLong(1));
            //noinspection NumericCastThatLosesPrecision
            assertEquals((long) value, rs.getLong("C"));

            assertEquals((float) value, rs.getFloat(1));
            assertEquals((float) value, rs.getFloat("C"));

            assertEquals(value, rs.getDouble(1));
            assertEquals(value, rs.getDouble("C"));

            assertEquals(decimalVal, rs.getBigDecimal(1));
            assertEquals(decimalVal, rs.getBigDecimal("C"));

            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal(1, 2));
            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal("C", 2));

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(value, rs.getObject(1));
            assertEquals(value, rs.getObject("C"));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject(1, Byte.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Byte.valueOf((byte) value), rs.getObject("C", Byte.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Short.valueOf((short) value), rs.getObject(1, Short.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Short.valueOf((short) value), rs.getObject("C", Short.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Integer.valueOf((int) value), rs.getObject(1, Integer.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Integer.valueOf((int) value), rs.getObject("C", Integer.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Long.valueOf((long) value), rs.getObject(1, Long.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Long.valueOf((long) value), rs.getObject("C", Long.class));

            //noinspection NumericCastThatLosesPrecision
            assertEquals(Float.valueOf((float) value), rs.getObject(1, Float.class));
            //noinspection NumericCastThatLosesPrecision
            assertEquals(Float.valueOf((float) value), rs.getObject("C", Float.class));

            assertEquals(Double.valueOf(value), rs.getObject(1, Double.class));
            assertEquals(Double.valueOf(value), rs.getObject("C", Double.class));

            assertEquals(decimalVal, rs.getObject(1, BigDecimal.class));
            assertEquals(decimalVal, rs.getObject("C", BigDecimal.class));

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    @ParameterizedTest
    @CsvSource({
            "true, -9223372036854775808, -9223372036854775808",
            "true, 9223372036854775807, 9223372036854775807",
            "true, -42.43, -42.43",
            "true, 42.43, 42.43",
            "false, xyz, ",
            "false,, "
    })
    public void getDoubleFromString(boolean valid, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                String error = "Cannot convert to double: " + value;

                expectSqlException(() -> rs.getDouble(1), error);
                expectSqlException(() -> rs.getDouble("C"), error);

                expectSqlException(() -> rs.getObject(1, Double.class), error);
                expectSqlException(() -> rs.getObject("C", Double.class), error);
            } else {
                double expected = Double.parseDouble(result);

                assertEquals(expected, rs.getDouble(1));
                assertEquals(expected, rs.getDouble("C"));

                assertEquals(expected, rs.getObject(1, Double.class));
                assertEquals(expected, rs.getObject("C", Double.class));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getDoubleNumbers")
    public void getDoubleFromNumber(boolean valid, Number value) throws SQLException {
        ColumnType columnType = columnTypefromObject(value);

        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (valid) {
                assertEquals(value.doubleValue(), rs.getDouble(1));
                assertEquals(value.doubleValue(), rs.getDouble("C"));

                assertEquals(value.doubleValue(), rs.getObject(1, Double.class));
                assertEquals(value.doubleValue(), rs.getObject("C", Double.class));
            } else {
                expectSqlConversionError(() -> rs.getDouble(1), "double");
                expectSqlConversionError(() -> rs.getDouble("C"), "double");

                expectSqlConversionError(() -> rs.getObject(1, Double.class), "double");
                expectSqlConversionError(() -> rs.getObject("C", Double.class), "double");
            }
        }
    }

    private static Stream<Arguments> getDoubleNumbers() {
        return Stream.of(
                Arguments.of(true, (byte) 42),
                Arguments.of(true, (short) 42),

                Arguments.of(true, 42),
                Arguments.of(true, Long.MAX_VALUE),
                Arguments.of(true, Long.MIN_VALUE),

                Arguments.of(true, 42.0f),
                Arguments.of(true, (float) (Long.MAX_VALUE)),
                Arguments.of(true, (float) (Long.MIN_VALUE)),
                Arguments.of(true, Float.NaN),
                Arguments.of(true, Float.NEGATIVE_INFINITY),
                Arguments.of(true, Float.POSITIVE_INFINITY),

                Arguments.of(true, 42.0d),
                Arguments.of(true, (double) (Long.MAX_VALUE)),
                Arguments.of(true, (double) (Long.MIN_VALUE)),
                Arguments.of(true, Double.NaN),
                Arguments.of(true, Double.NEGATIVE_INFINITY),
                Arguments.of(true, Double.POSITIVE_INFINITY),

                Arguments.of(true, BigDecimal.valueOf(42.0d)),
                Arguments.of(true, BigDecimal.valueOf(Double.MAX_VALUE)),
                Arguments.of(true, BigDecimal.valueOf(-Double.MAX_VALUE)),
                Arguments.of(false, BigDecimal.TEN.pow(512, MathContext.DECIMAL128)),
                Arguments.of(false, BigDecimal.TEN.pow(512, MathContext.DECIMAL128).negate())
        );
    }

    @ParameterizedTest
    @MethodSource("getBigDecimalValues")
    public void getBigDecimal(BigDecimal value) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.DECIMAL, 0, 0, false), value)) {
            assertTrue(rs.next());

            BigDecimal decimalScaledVal = value.setScale(2, RoundingMode.HALF_UP);
            String strVal = String.valueOf(value);

            expectSqlConversionError(() -> rs.getBoolean(1), "boolean");
            expectSqlConversionError(() -> rs.getBoolean("C"), "boolean");

            assertEquals(value.byteValue(), rs.getByte(1));
            assertEquals(value.byteValue(), rs.getByte("C"));

            assertEquals(value.shortValue(), rs.getShort(1));
            assertEquals(value.shortValue(), rs.getShort("C"));

            assertEquals(value.intValue(), rs.getInt(1));
            assertEquals(value.intValue(), rs.getInt("C"));

            assertEquals(value.longValue(), rs.getLong(1));
            assertEquals(value.longValue(), rs.getLong("C"));

            assertEquals(value.floatValue(), rs.getFloat(1));
            assertEquals(value.floatValue(), rs.getFloat("C"));

            assertEquals(value.doubleValue(), rs.getDouble(1));
            assertEquals(value.doubleValue(), rs.getDouble("C"));

            assertEquals(value, rs.getBigDecimal(1));
            assertEquals(value, rs.getBigDecimal("C"));

            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal(1, 2));
            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal("C", 2));

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(value, rs.getObject(1));
            assertEquals(value, rs.getObject("C"));

            assertEquals(Byte.valueOf(value.byteValue()), rs.getObject(1, Byte.class));
            assertEquals(Byte.valueOf(value.byteValue()), rs.getObject("C", Byte.class));

            assertEquals(Short.valueOf(value.shortValue()), rs.getObject(1, Short.class));
            assertEquals(Short.valueOf(value.shortValue()), rs.getObject("C", Short.class));

            assertEquals(Integer.valueOf(value.intValue()), rs.getObject(1, Integer.class));
            assertEquals(Integer.valueOf(value.intValue()), rs.getObject("C", Integer.class));

            assertEquals(Long.valueOf(value.longValue()), rs.getObject(1, Long.class));
            assertEquals(Long.valueOf(value.longValue()), rs.getObject("C", Long.class));

            assertEquals(Float.valueOf(value.floatValue()), rs.getObject(1, Float.class));
            assertEquals(Float.valueOf(value.floatValue()), rs.getObject("C", Float.class));

            assertEquals(Double.valueOf(value.doubleValue()), rs.getObject(1, Double.class));
            assertEquals(Double.valueOf(value.doubleValue()), rs.getObject("C", Double.class));

            assertEquals(value, rs.getObject(1, BigDecimal.class));
            assertEquals(value, rs.getObject("C", BigDecimal.class));

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    private static Stream<BigDecimal> getBigDecimalValues() {
        return Stream.of(
                new BigDecimal("121.234").negate(),
                BigDecimal.ONE.negate(),
                BigDecimal.ZERO,
                BigDecimal.ONE,
                new BigDecimal("121.234")
        );
    }

    @ParameterizedTest
    @CsvSource({
            "true, -9223372036854775808, -9223372036854775808",
            "true, 9223372036854775807, 9223372036854775807",
            "true, -42, -42",
            "true, 42, 42",
            "true, -9223372036854775809, -9223372036854775809",
            "true, 9223372036854775808, 9223372036854775808",
            "true, 10223372036854775808.34954375423432, 10223372036854775808.34954375423432",
            "true, -10223372036854775808.34954375423432, -10223372036854775808.34954375423432",
            "false, xyz, ",
            "false,, "
    })
    public void getBigDecimalFromString(boolean valid, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                String error = "Cannot convert to BigDecimal: " + value;

                expectSqlException(() -> rs.getBigDecimal(1), error);
                expectSqlException(() -> rs.getBigDecimal("C"), error);
            } else {
                BigDecimal expected = new BigDecimal(result);

                assertEquals(expected, rs.getBigDecimal(1));
                assertEquals(expected, rs.getBigDecimal("C"));

                assertEquals(expected, rs.getObject(1, BigDecimal.class));
                assertEquals(expected, rs.getObject("C", BigDecimal.class));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getBigDecimalNumbers")
    public void getBigDecimalFromNumber(boolean valid, Number value) throws SQLException {
        ColumnType columnType = columnTypefromObject(value);

        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", columnType, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (valid) {
                BigDecimal expected;
                if (value instanceof BigDecimal) {
                    expected = (BigDecimal) value;
                } else if (value instanceof Float || value instanceof Double) {
                    expected = new BigDecimal(value.doubleValue());
                } else {
                    expected = new BigDecimal(value.longValue());
                }

                assertEquals(expected, rs.getBigDecimal(1));
                assertEquals(expected, rs.getBigDecimal("C"));

                assertEquals(expected, rs.getObject(1, BigDecimal.class));
                assertEquals(expected, rs.getObject("C", BigDecimal.class));
            } else {
                expectSqlConversionError(() -> rs.getBigDecimal(1), "double");
                expectSqlConversionError(() -> rs.getBigDecimal("C"), "double");

                expectSqlConversionError(() -> rs.getObject(1, BigDecimal.class), "double");
                expectSqlConversionError(() -> rs.getObject("C", BigDecimal.class), "double");
            }
        }
    }

    private static Stream<Arguments> getBigDecimalNumbers() {
        return Stream.of(
                Arguments.of(true, (byte) 42),
                Arguments.of(true, (short) 42),

                Arguments.of(true, 42),
                Arguments.of(true, Long.MAX_VALUE),
                Arguments.of(true, Long.MIN_VALUE),

                Arguments.of(true, 42.0f),
                Arguments.of(true, Float.MAX_VALUE),
                Arguments.of(true, -Float.MAX_VALUE),

                Arguments.of(true, 42.0d),
                Arguments.of(true, Double.MAX_VALUE),
                Arguments.of(true, -Double.MAX_VALUE),

                Arguments.of(true, BigDecimal.valueOf(42.0d)),
                Arguments.of(true, BigDecimal.valueOf(42.123d)),
                Arguments.of(true, BigDecimal.valueOf(45.9d)),
                Arguments.of(true, BigDecimal.valueOf(Double.MAX_VALUE)),
                Arguments.of(true, BigDecimal.valueOf(-Double.MAX_VALUE)),
                Arguments.of(true, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.TEN)),
                Arguments.of(true, BigDecimal.valueOf(-Double.MAX_VALUE).add(BigDecimal.TEN.negate()))
        );
    }

    @ParameterizedTest
    @MethodSource("getBigDecimalScaledValues")
    public void getBigDecimalScaled(BigDecimal value, int scale) throws SQLException {
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.DECIMAL, value.precision(), scale, false), value)) {
            assertTrue(rs.next());

            BigDecimal decimalScaledVal = value.setScale(scale, RoundingMode.HALF_UP);
            String strVal = String.valueOf(value);

            expectSqlConversionError(() -> rs.getBoolean(1), "boolean");
            expectSqlConversionError(() -> rs.getBoolean("C"), "boolean");

            assertEquals(value.byteValue(), rs.getByte(1));
            assertEquals(value.byteValue(), rs.getByte("C"));

            assertEquals(value.shortValue(), rs.getShort(1));
            assertEquals(value.shortValue(), rs.getShort("C"));

            assertEquals(value.intValue(), rs.getInt(1));
            assertEquals(value.intValue(), rs.getInt("C"));

            assertEquals(value.longValue(), rs.getLong(1));
            assertEquals(value.longValue(), rs.getLong("C"));

            assertEquals(value.floatValue(), rs.getFloat(1));
            assertEquals(value.floatValue(), rs.getFloat("C"));

            assertEquals(value.doubleValue(), rs.getDouble(1));
            assertEquals(value.doubleValue(), rs.getDouble("C"));

            assertEquals(value, rs.getBigDecimal(1));
            assertEquals(value, rs.getBigDecimal("C"));

            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal(1, scale));
            //noinspection deprecation
            assertEquals(decimalScaledVal, rs.getBigDecimal("C", scale));

            assertEquals(strVal, rs.getString(1));
            assertEquals(strVal, rs.getString("C"));

            assertEquals(strVal, rs.getNString(1));
            assertEquals(strVal, rs.getNString("C"));

            // getObject

            assertEquals(value, rs.getObject(1));
            assertEquals(value, rs.getObject("C"));

            assertEquals(Byte.valueOf(value.byteValue()), rs.getObject(1, Byte.class));
            assertEquals(Byte.valueOf(value.byteValue()), rs.getObject("C", Byte.class));

            assertEquals(Short.valueOf(value.shortValue()), rs.getObject(1, Short.class));
            assertEquals(Short.valueOf(value.shortValue()), rs.getObject("C", Short.class));

            assertEquals(Integer.valueOf(value.intValue()), rs.getObject(1, Integer.class));
            assertEquals(Integer.valueOf(value.intValue()), rs.getObject("C", Integer.class));

            assertEquals(Long.valueOf(value.longValue()), rs.getObject(1, Long.class));
            assertEquals(Long.valueOf(value.longValue()), rs.getObject("C", Long.class));

            assertEquals(Float.valueOf(value.floatValue()), rs.getObject(1, Float.class));
            assertEquals(Float.valueOf(value.floatValue()), rs.getObject("C", Float.class));

            assertEquals(Double.valueOf(value.doubleValue()), rs.getObject(1, Double.class));
            assertEquals(Double.valueOf(value.doubleValue()), rs.getObject("C", Double.class));

            assertEquals(value, rs.getObject(1, BigDecimal.class));
            assertEquals(value, rs.getObject("C", BigDecimal.class));

            assertEquals(strVal, rs.getObject(1, String.class));
            assertEquals(strVal, rs.getObject("C", String.class));

            expectSqlConversionError(() -> rs.getObject(1, UUID.class), "java.util.UUID");
            expectSqlConversionError(() -> rs.getObject("C", UUID.class), "java.util.UUID");
        }
    }

    private static Stream<Arguments> getBigDecimalScaledValues() {
        return Stream.of(
                Arguments.of(new BigDecimal("67.098912301287").negate(), 13),
                Arguments.of(new BigDecimal("121.234").negate(), 3),
                Arguments.of(BigDecimal.ZERO, 0),
                Arguments.of(new BigDecimal("121.234"), 3),
                Arguments.of(new BigDecimal("67.098912301287"), 13)
        );
    }

    @ParameterizedTest
    @CsvSource({
            "true, 0, -9223372036854775808, -9223372036854775808",
            "true, 0, 9223372036854775807, 9223372036854775807",
            "true, 0, -42, -42",
            "true, 0, 42, 42",
            "true, 0, -9223372036854775809, -9223372036854775809",
            "true, 0, 9223372036854775808, 9223372036854775808",
            "true, 14, 10223372036854775808.34954375423432, 10223372036854775808.34954375423432",
            "true, 14, -10223372036854775808.34954375423432, -10223372036854775808.34954375423432",
            "false, 2, xyz, ",
            "false, 2, , "
    })
    public void getBigDecimalScaledFromString(boolean valid, int scale, String value, String result) throws SQLException {
        if (value == null) {
            value = "";
        }
        try (ResultSet rs = createSingleRow(new ColumnDefinition("C", ColumnType.STRING, 0, 0, false), value)) {
            assertTrue(rs.next());

            if (!valid) {
                //noinspection deprecation
                expectSqlException(() -> rs.getBigDecimal(1, scale), "Cannot convert to BigDecimal: " + value);
                //noinspection deprecation
                expectSqlException(() -> rs.getBigDecimal("C", scale), "Cannot convert to BigDecimal: " + value);
            } else {
                //noinspection deprecation
                assertEquals(new BigDecimal(result), rs.getBigDecimal(1, scale));
                //noinspection deprecation
                assertEquals(new BigDecimal(result), rs.getBigDecimal("C", scale));
            }
        }
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

    private static void expectSqlConversionError(Executable call, String typeName) {
        SQLException err = assertThrows(SQLException.class, call);
        assertThat(err.getMessage(), containsString("Cannot convert to " + typeName));
        assertEquals(SqlStateCode.CONVERSION_FAILED, err.getSQLState());
    }

    private static void expectNotSupported(Executable call) {
        assertThrows(SQLFeatureNotSupportedException.class, call);
    }

    private static void expectSqlException(Executable m, String message) {
        SQLException err = assertThrows(SQLException.class, m);
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

    private static void expectPositioned(Executable call) {
        SQLException err = assertThrows(SQLException.class, call);
        assertThat(err.getMessage(), containsString("Result set is not positioned on a row."));
    }

    private static void expectClosed(Executable method) {
        expectSqlException(method, "Result set is closed.");
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
