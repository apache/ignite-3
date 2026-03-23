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

package org.apache.ignite.catalog;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Column types. */
public class ColumnType<T> {
    /** A cache for column types by Java type. */
    private static final Map<Class<?>, ColumnType<?>> TYPES = new LinkedHashMap<>();

    /** {@code BOOLEAN} SQL column type. */
    public static final ColumnType<Boolean> BOOLEAN = new ColumnType<>(Boolean.class, "BOOLEAN");

    /** {@code TINYINT} SQL column type. */
    public static final ColumnType<Byte> TINYINT = new ColumnType<>(Byte.class, "TINYINT");

    /** {@code SMALLINT} SQL column type. */
    public static final ColumnType<Short> SMALLINT = new ColumnType<>(Short.class, "SMALLINT");

    /** {@code INTEGER} SQL column type. */
    public static final ColumnType<Integer> INTEGER = new ColumnType<>(Integer.class, "INT");

    /** {@code BIGINT} SQL column type. */
    public static final ColumnType<Long> BIGINT = new ColumnType<>(Long.class, "BIGINT");

    /** 8-bit signed integer. An alias for {@link #TINYINT}. */
    public static final ColumnType<Byte> INT8 = TINYINT;

    /** 16-bit signed integer. An alias for {@link #SMALLINT}. */
    public static final ColumnType<Short> INT16 = SMALLINT;

    /** 32-bit signed integer. An alias for {@link #INTEGER}. */
    public static final ColumnType<Integer> INT32 = INTEGER;

    /** 64-bit signed integer. An alias for {@link #BIGINT}. */
    public static final ColumnType<Long> INT64 = BIGINT;

    /** {@code REAL} SQL column type. */
    public static final ColumnType<Float> REAL = new ColumnType<>(Float.class, "REAL");

    /** 32-bit single-precision floating-point number. An alias for {@link #REAL}. */
    public static final ColumnType<Float> FLOAT = REAL;

    /** {@code DOUBLE} SQL column type. */
    public static final ColumnType<Double> DOUBLE = new ColumnType<>(Double.class, "DOUBLE");

    /** {@code VARCHAR} SQL column type. */
    public static final ColumnType<String> VARCHAR = new ColumnType<>(String.class, "VARCHAR");

    /** {@code VARCHAR} with specified length. */
    public static ColumnType<String> varchar(int length) {
        return VARCHAR.copy().length(length);
    }

    /** {@code VARBINARY} SQL column type. */
    public static final ColumnType<byte[]> VARBINARY = new ColumnType<>(byte[].class, "VARBINARY");

    /** {@code VARBINARY} with specified length. */
    public static ColumnType<byte[]> varbinary(int length) {
        return VARBINARY.copy().length(length);
    }

    /** {@code TIME} SQL column type. */
    public static final ColumnType<LocalTime> TIME = new ColumnType<>(LocalTime.class, "TIME");

    /** {@code TIME} with specified precision. */
    public static ColumnType<LocalTime> time(int precision) {
        return TIME.copy().precision(precision);
    }

    /** {@code TIMESTAMP} SQL column type. */
    public static final ColumnType<LocalDateTime> TIMESTAMP = new ColumnType<>(LocalDateTime.class, "TIMESTAMP");

    /** {@code TIMESTAMP} with specified precision. */
    public static ColumnType<LocalDateTime> timestamp(int precision) {
        return TIMESTAMP.copy().precision(precision);
    }

    /** {@code DATE} SQL column type. */
    public static final ColumnType<LocalDate> DATE = new ColumnType<>(LocalDate.class, "DATE");

    /** {@code DECIMAL} SQL column type. */
    public static final ColumnType<BigDecimal> DECIMAL = new ColumnType<>(BigDecimal.class, "DECIMAL");

    /** {@code DECIMAL} with specified precision and scale. */
    public static ColumnType<BigDecimal> decimal(int precision, int scale) {
        return DECIMAL.copy().precision(precision, scale);
    }

    /** {@code UUID} SQL column type. */
    public static final ColumnType<UUID> UUID = new ColumnType<>(UUID.class, "UUID");

    /** {@code TIMESTAMP WITH LOCAL TIME ZONE} SQL column type. */
    public static final ColumnType<Instant> TIMESTAMP_WITH_LOCAL_TIME_ZONE = new ColumnType<>(
            Instant.class, "TIMESTAMP WITH LOCAL TIME ZONE");

    private final Class<T> type;

    private final String typeName;

    private Boolean nullable;

    private T defaultValue;

    private String defaultExpression;

    private Integer precision;

    private Integer scale;

    private Integer length;

    private ColumnType(Class<T> type, String typeName) {
        this.type = type;
        this.typeName = typeName;

        //noinspection ThisEscapedInObjectConstruction
        TYPES.putIfAbsent(type, this);
    }

    private ColumnType(ColumnType<T> ref) {
        this.type = ref.type;
        this.typeName = ref.typeName;
        this.nullable = ref.nullable;
        this.defaultValue = ref.defaultValue;
        this.defaultExpression = ref.defaultExpression;
        this.precision = ref.precision;
        this.scale = ref.scale;
        this.length = ref.length;
    }

    /**
     * Creates a column type with the type derived from the Java type and specified length, precision, scale and nullability.
     *
     * @param type Java type from which to derive the column type.
     * @param length Length of the character data type.
     * @param precision Precision of the numeric data type.
     * @param scale Scale of the numeric data type.
     * @param nullable Nullability.
     * @return Created column type object.
     */
    public static ColumnType<?> of(Class<?> type, Integer length, Integer precision, Integer scale, Boolean nullable) {
        return of(type)
                .length_(length)
                .precision_(precision, scale)
                .nullable_(nullable);
    }

    /**
     * Creates a column type with the type derived from the Java type and default length, precision, scale and nullability.
     *
     * @param type Java type from which to derive the column type.
     * @return Created column type object.
     */
    public static ColumnType<?> of(Class<?> type) {
        ColumnType<?> columnType = TYPES.get(type);
        if (columnType == null) {
            throw new IllegalArgumentException("Class is not supported: " + type.getCanonicalName());
        }
        return columnType.copy();
    }

    /**
     * Returns Java type of the column type.
     *
     * @return Java class.
     */
    public Class<T> type() {
        return type;
    }

    /**
     * Returns SQL type name of the column type.
     *
     * @return Type name.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * Returns nullability of the column type.
     *
     * @return {@code false} if the column is non-nullable.
     */
    public Boolean nullable() {
        return nullable;
    }

    /**
     * Sets nullability of this type.
     *
     * @param n Nullable flag.
     * @return Copy of the column type object.
     */
    public ColumnType<T> nullable(Boolean n) {
        return copy().nullable_(n);
    }

    /**
     * Sets this type as not nullable.
     *
     * @return Copy of the column type object.
     */
    public ColumnType<T> notNull() {
        return nullable(false);
    }

    /**
     * Returns default value of the column type.
     *
     * @return Default value.
     */
    public T defaultValue() {
        return defaultValue;
    }

    /**
     * Set default value for this column type.
     *
     * @param value Default value.
     * @return Copy of the column type object.
     */
    public ColumnType<T> defaultValue(T value) {
        return copy().defaultValue_(value);
    }

    /**
     * Returns default value expression of the column type.
     *
     * @return Default value expression.
     */
    public String defaultExpression() {
        return defaultExpression;
    }

    /**
     * Set default value for this column type as an expression.
     *
     * @param expression Default value as an expression.
     * @return Copy of the column type object.
     */
    public ColumnType<T> defaultExpression(String expression) {
        return copy().defaultExpression_(expression);
    }

    /**
     * Returns precision of the column type.
     *
     * @return Precision.
     */
    public Integer precision() {
        return precision;
    }

    /**
     * Sets precision for this column type.
     *
     * @param precision Precision.
     * @return Copy of the column type object.
     */
    public ColumnType<T> precision(Integer precision) {
        return copy().precision_(precision);
    }

    /**
     * Sets precision and scale for this column type.
     *
     * @param precision Precision.
     * @param scale Scale.
     * @return Copy of the column type object.
     */
    public ColumnType<T> precision(Integer precision, Integer scale) {
        return copy().precision_(precision, scale);
    }

    /**
     * Returns scale of the column type.
     *
     * @return Scale.
     */
    public Integer scale() {
        return scale;
    }

    /**
     * Returns length of the column type.
     *
     * @return Length.
     */
    public Integer length() {
        return length;
    }

    /**
     * Sets length for this column type.
     *
     * @param length Length.
     * @return Copy of the column type object.
     */
    public ColumnType<T> length(Integer length) {
        return copy().length_(length);
    }

    private ColumnType<T> copy() {
        return new ColumnType<>(this);
    }

    private ColumnType<T> nullable_(Boolean n) {
        this.nullable = n;
        return this;
    }

    private ColumnType<T> defaultValue_(T value) {
        this.defaultValue = value;
        return this;
    }

    private ColumnType<T> defaultExpression_(String expression) {
        this.defaultExpression = expression;
        return this;
    }

    private ColumnType<T> precision_(Integer precision) {
        this.precision = precision;
        return this;
    }

    private ColumnType<T> precision_(Integer precision, Integer scale) {
        this.precision = precision;
        this.scale = scale;
        return this;
    }

    private ColumnType<T> length_(Integer length) {
        this.length = length;
        return this;
    }
}
