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

import static org.apache.ignite.jdbc.util.JdbcTestUtils.assertThrowsSqlException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.jdbc.proto.JdbcQueryEventHandler;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Unit tests for PreparedStatement methods.
 */
public class PreparedStatementParamsTest extends BaseIgniteAbstractTest {

    /** Values for supported JDBC types. */
    private static final Map<JDBCType, Object> SUPPORTED_TYPES = new HashMap<>();

    static {
        SUPPORTED_TYPES.put(JDBCType.BOOLEAN, true);
        SUPPORTED_TYPES.put(JDBCType.TINYINT, (byte) 1);
        SUPPORTED_TYPES.put(JDBCType.SMALLINT, (short) 1);
        SUPPORTED_TYPES.put(JDBCType.INTEGER, 1);
        SUPPORTED_TYPES.put(JDBCType.BIGINT, 1L);
        SUPPORTED_TYPES.put(JDBCType.FLOAT, 1.0f);
        SUPPORTED_TYPES.put(JDBCType.REAL, 1.0f);
        SUPPORTED_TYPES.put(JDBCType.DOUBLE, 1.0d);
        SUPPORTED_TYPES.put(JDBCType.DECIMAL, new BigDecimal("123"));
        SUPPORTED_TYPES.put(JDBCType.CHAR, "123");
        SUPPORTED_TYPES.put(JDBCType.VARCHAR, "123");
        SUPPORTED_TYPES.put(JDBCType.BINARY, new byte[]{1, 2, 3});
        SUPPORTED_TYPES.put(JDBCType.VARBINARY, new byte[]{1, 2, 3});
        SUPPORTED_TYPES.put(JDBCType.DATE, new Date(1));
        SUPPORTED_TYPES.put(JDBCType.TIME, Time.valueOf(LocalTime.NOON));
        SUPPORTED_TYPES.put(JDBCType.TIMESTAMP, Timestamp.valueOf("2000-01-01 00:00:00.000"));
        SUPPORTED_TYPES.put(JDBCType.OTHER, new UUID(1, 1));
        SUPPORTED_TYPES.put(JDBCType.NULL, null);
    }

    private JdbcConnection conn;

    @BeforeEach
    public void initConnection() {
        conn = new JdbcConnection(Mockito.mock(JdbcQueryEventHandler.class), new ConnectionPropertiesImpl());
    }

    /** {@link PreparedStatement#clearParameters()} clears parameter list. */
    @Test
    public void clearParameters() throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT 1")) {
            pstmt.setObject(1, 1);

            JdbcPreparedStatement ignitePstmt = pstmt.unwrap(JdbcPreparedStatement.class);
            assertEquals(1, ignitePstmt.getArguments().size());

            pstmt.clearParameters();
            assertNull(ignitePstmt.getArguments(), "parameters has not been cleared");
        }
    }

    /**
     * Set {@code boolean} parameter via {@link PreparedStatement#setBoolean(int, boolean)}.
     */
    @Test
    public void testSetBoolean() throws SQLException {
        boolean value = value(Boolean.class, JDBCType.BOOLEAN);
        checkParameter(PreparedStatement::setBoolean, value);
    }

    /**
     * Set {@code byte} parameter via {@link PreparedStatement#setByte(int, byte)}.
     */
    @Test
    public void testSetByte() throws SQLException {
        byte value = value(Byte.class, JDBCType.TINYINT);
        checkParameter(PreparedStatement::setByte, value);
    }

    /**
     * Set {@code short} parameter via {@link PreparedStatement#setShort(int, short)}.
     */
    @Test
    public void testSetShort() throws SQLException {
        short value = value(Short.class, JDBCType.SMALLINT);
        checkParameter(PreparedStatement::setShort, value, value);
    }

    /**
     * Set {@code int} parameter via {@link PreparedStatement#setInt(int, int)}.
     */
    @Test
    public void testSetInt() throws SQLException {
        int value = value(Integer.class, JDBCType.INTEGER);
        checkParameter(PreparedStatement::setInt, value);
    }

    /**
     * Set {@code long} parameter via {@link PreparedStatement#setLong(int, long)}.
     */
    @Test
    public void testSetLong() throws SQLException {
        long value = value(Long.class, JDBCType.BIGINT);
        checkParameter(PreparedStatement::setLong, value);
    }

    /**
     * Set {@code float} parameter via {@link PreparedStatement#setFloat(int, float)}.
     */
    @Test
    public void testSetFloat() throws SQLException {
        float value = value(Float.class, JDBCType.FLOAT);
        checkParameter(PreparedStatement::setFloat, value);
    }

    /**
     * Set {@code double} parameter via {@link PreparedStatement#setDouble(int, double)}.
     */
    @Test
    public void testSetDouble() throws SQLException {
        double value = value(Double.class, JDBCType.DOUBLE);
        checkParameter(PreparedStatement::setDouble, value);
    }

    /**
     * Set {@code BigDecimal} parameter via {@link PreparedStatement#setBigDecimal(int, BigDecimal)}.
     */
    @Test
    public void testSetBigDecimal() throws SQLException {
        BigDecimal value = value(BigDecimal.class, JDBCType.DECIMAL);
        checkParameter(PreparedStatement::setBigDecimal, value);
    }

    /**
     * Set {@code string} parameter via {@link PreparedStatement#setString(int, String)}.
     */
    @Test
    public void testSetString() throws SQLException {
        String value = value(String.class, JDBCType.VARCHAR);
        checkParameter(PreparedStatement::setString, value);
    }

    /**
     * Set {@code byte array} parameter via {@link PreparedStatement#setBytes(int, byte[])}.
     */
    @Test
    public void testSetBytes() throws SQLException {
        byte[] bytes = value(byte[].class, JDBCType.VARBINARY);
        checkParameter(PreparedStatement::setBytes, bytes);
    }

    /**
     * Set {@link Date} parameter via {@link PreparedStatement#setDate(int, Date)}.
     */
    @Test
    public void testSetDate() throws SQLException {
        Date value = value(Date.class, JDBCType.DATE);
        checkParameter(PreparedStatement::setDate, value);
    }

    /**
     * Set {@link Time} parameter via {@link PreparedStatement#setTime(int, Time)}}.
     */
    @Test
    public void testSetTime() throws SQLException {
        Time time = value(Time.class, JDBCType.TIME);
        checkParameter(PreparedStatement::setTime, time);
    }

    /**
     * Set {@link Timestamp} parameter via {@link PreparedStatement#setTimestamp(int, Timestamp)}}.
     */
    @Test
    public void testSetTimestamp() throws SQLException {
        Timestamp timestamp = value(Timestamp.class, JDBCType.TIMESTAMP);
        checkParameter(PreparedStatement::setTimestamp, timestamp);
    }

    /** {@link PreparedStatement#setObject(int, Object)} for all supported types. */
    @ParameterizedTest(name = "setObject: {0} {2}")
    @MethodSource("jdbcTypeValueTypeNames")
    public void testSetObject(JDBCType jdbcType, Object value, String javaTypeName) throws SQLException {
        checkParameter((p, idx, v) -> p.setObject(idx, value), value);
    }

    /**
     * Call to {@link PreparedStatement#setObject(int, Object)}.
     */
    @Test
    public void testSetObjectWithTypeConversion() {
        int typeCode = JDBCType.INTEGER.getVendorTypeNumber();
        Object value = new Object();

        checkFeatureIsNotSupported((s) -> s.setObject(1, value, typeCode), "Conversion to target sql type is not supported.");
    }

    /**
     * {@link PreparedStatement#setObject(int, Object, int, int)} is not supported.
     */
    @Test
    public void testSetObjectWithScale() {
        int typeCode = JDBCType.INTEGER.getVendorTypeNumber();
        Object value = new Object();

        checkFeatureIsNotSupported((s) -> s.setObject(1, value, typeCode, 1), "Conversion to target sql type is not supported.");
    }

    /**
     * {@link PreparedStatement#setObject(int, Object, SQLType)} is not supported.
     */
    @Test
    public void testSetObjectWitSqlTypeIsNotSupported() {
        UUID uuid = new UUID(0, 0);
        SQLType sqlType = Mockito.mock(SQLType.class);

        SQLException err = checkSetParameterFails("setObject not implemented", (s) -> s.setObject(1, uuid, sqlType, 1));
        assertInstanceOf(SQLFeatureNotSupportedException.class, err);
    }

    /**
     * {@link PreparedStatement#setObject(int, Object)} throws an exception when called with unsupported type.
     */
    @Test
    public void testSetObjectRejectUnknownValue() {
        Object value = new Object();

        checkFeatureIsNotSupported((s) -> s.setObject(1, value), "Parameter is not supported");
    }

    /** {@code setNull} for supported types. */
    @ParameterizedTest(name = "setNull: {1}")
    @MethodSource("igniteTypeTypeNames")
    public void testSetNull(JDBCType igniteType, String typeName) throws SQLException {
        int typeCode = igniteType.getVendorTypeNumber();

        checkParameter((p, idx, v) -> p.setNull(idx, typeCode), null);

        // setNull with typename
        checkParameter((p, idx, v) -> p.setNull(idx, typeCode, typeName), null);
    }


    /** {@code setNull} does not support types not allowed by spec. */
    @ParameterizedTest(name = " setNull(typeId): {0}")
    @MethodSource("typesDoNotSupportSetNull")
    public void testSetNullForNotSupportedTypes(JDBCType jdbcType) {
        int typeCode = jdbcType.getVendorTypeNumber();

        SQLException err = checkSetParameterFails("Type is not supported", (s) -> s.setNull(1, typeCode));
        assertInstanceOf(SQLFeatureNotSupportedException.class, err);
        assertThat(err.getMessage(), containsString("Type is not supported"));

        // setNull with typename
        SQLException err2 = checkSetParameterFails("Type is not supported", (s) -> s.setNull(1, typeCode, jdbcType.getName()));
        assertInstanceOf(SQLFeatureNotSupportedException.class, err2);
        assertThat(err2.getMessage(), containsString("Type is not supported"));
    }

    private <T> void checkParameter(SetParameter<T> set, T value) throws SQLException {
        checkParameter(set, value, value);
    }

    /** Sets the first parameter and checks that that parameter is set. */
    private <T> void checkParameter(SetParameter<T> set, T value, Object expected) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT ?")) {
            set.setParameter(pstmt, 1, value);

            JdbcPreparedStatement igniteStmt = pstmt.unwrap(JdbcPreparedStatement.class);
            Object paramValue = igniteStmt.getArguments().get(0);

            if (expected == null) {
                assertNull(paramValue);
            } else if (paramValue.getClass().isArray()) {
                Object[] expectedArray = toObjects(expected);
                Object[] actualArray = toObjects(paramValue);

                // Compare array types
                assertSame(expected.getClass(), paramValue.getClass(), "array type");
                assertArrayEquals(expectedArray, actualArray, "array values");
            } else {
                assertEquals(expected, paramValue);
            }
        }
    }

    /** Expects that the given action that uses {@code PreparedStatement} throws an {@link SQLException}. */
    private SQLException checkSetParameterFails(String expectedErrorMessage, StatementConsumer action) {
        return assertThrowsSqlException(
                expectedErrorMessage,
                () -> {
                    try (PreparedStatement pstmt = conn.prepareStatement("SELECT ?")) {
                        action.consume(pstmt);
                    }
                });
    }

    /**
     * Expects that the given action that uses {@code PreparedStatement} throws an {@link SQLFeatureNotSupportedException}
     * and an error message contains {@code messagePart}.
     */
    private void checkFeatureIsNotSupported(StatementConsumer action, String messagePart) {
        SQLFeatureNotSupportedException err = Assertions.assertThrows(SQLFeatureNotSupportedException.class, () -> {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT ?")) {
                action.consume(pstmt);
            }
        });
        assertThat(err.getMessage(), containsString(messagePart));
    }

    private static Stream<Arguments> jdbcTypeValueTypeNames() {
        return SUPPORTED_TYPES.entrySet().stream()
                .sorted(Comparator.comparing(a -> a.getKey().getName()))
                .map(e -> {
                    JDBCType jdbcType = e.getKey();
                    Object value = e.getValue();
                    String javaTypeName = (value != null) ? value.getClass().getTypeName() : "NULL";

                    return Arguments.of(jdbcType, value, javaTypeName);
                });
    }

    private static Stream<Arguments> igniteTypeTypeNames() {
        return Arrays.stream(JDBCType.values())
                .filter(SUPPORTED_TYPES::containsKey)
                .map(t -> Arguments.of(t, t.getName()));
    }

    private static Stream<Arguments> typesDoNotSupportSetNull() {

        // Types that are not allowed in setNull spec
        TreeSet<SQLType> result = new TreeSet<>(Arrays.asList(
                JDBCType.ARRAY,
                JDBCType.BLOB,
                JDBCType.CLOB,
                JDBCType.DATALINK,
                JDBCType.JAVA_OBJECT,
                JDBCType.NCHAR,
                JDBCType.NCLOB,
                JDBCType.LONGNVARCHAR,
                JDBCType.REF,
                JDBCType.ROWID,
                JDBCType.SQLXML,
                JDBCType.STRUCT
        ));

        // Types that are not supported by JDBC driver implementation
        for (JDBCType jdbcType : JDBCType.values()) {
            if (!SUPPORTED_TYPES.containsKey(jdbcType)) {
                result.add(jdbcType);
            }
        }

        return result.stream()
                .sorted(Comparator.comparing(SQLType::getName))
                .map(t -> Arguments.of(t.getName(), t));
    }

    private static <T> T value(Class<T> javaType, JDBCType jdbcType) {
        Object value = SUPPORTED_TYPES.get(jdbcType);
        Objects.requireNonNull(value, "No value for " + javaType);

        return javaType.cast(value);
    }

    private static Object[] toObjects(Object input) {
        Class<?> inputClass = input.getClass();
        if (!inputClass.isArray()) {
            throw new IllegalArgumentException("Expected array but got " + inputClass.getTypeName());
        }

        Object[] out = new Object[Array.getLength(input)];
        for (var i = 0; i < out.length; i++) {
            out[i] = Array.get(input, i);
        }
        return out;
    }

    @FunctionalInterface
    interface SetParameter<T> {

        void setParameter(PreparedStatement s, int idx, T value) throws SQLException;
    }

    @FunctionalInterface
    interface StatementConsumer {

        void consume(PreparedStatement s) throws SQLException;
    }
}
