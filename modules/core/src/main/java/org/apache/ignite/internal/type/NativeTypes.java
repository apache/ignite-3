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

package org.apache.ignite.internal.type;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.type.StructNativeType.Field;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * A thin wrapper over {@link ColumnType} to instantiate parameterized constrained types.
 */
public class NativeTypes {
    /**
     * Maximum TIME and TIMESTAMP precision is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 38
     */
    public static final int MAX_TIME_PRECISION = 9;

    /** NULL type. */
    public static final NativeType NULL = new NativeType(ColumnType.NULL, 1);

    /**
     * BOOLEAN type.
     */
    public static final NativeType BOOLEAN = new NativeType(ColumnType.BOOLEAN, 1);

    /**
     * INT8 type.
     */
    public static final NativeType INT8 = new NativeType(ColumnType.INT8, 1);

    /**
     * INT16 type.
     */
    public static final NativeType INT16 = new NativeType(ColumnType.INT16, 2);

    /**
     * INT32 type.
     */
    public static final NativeType INT32 = new NativeType(ColumnType.INT32, 4);

    /**
     * INT64 type.
     */
    public static final NativeType INT64 = new NativeType(ColumnType.INT64, 8);

    /**
     * FLOAT type.
     */
    public static final NativeType FLOAT = new NativeType(ColumnType.FLOAT, 4);

    /**
     * DOUBLE type.
     */
    public static final NativeType DOUBLE = new NativeType(ColumnType.DOUBLE, 8);

    /**
     * UUID type.
     */
    public static final NativeType UUID = new NativeType(ColumnType.UUID, 16);

    /**
     * STRING type with length set to default value.
     */
    public static final NativeType STRING = new VarlenNativeType(ColumnType.STRING, 65536);

    /**
     * BYTES type with length set to default value.
     */
    public static final NativeType BYTES = new VarlenNativeType(ColumnType.BYTE_ARRAY, 65536);

    /** Timezone-free three-part value representing a year, month, and day. */
    public static final NativeType DATE = new NativeType(ColumnType.DATE, 3);

    /**
     * DURATION type.
     */
    public static final NativeType DURATION = new NativeType(ColumnType.DURATION, 8);

    /**
     * PERIOD type.
     */
    public static final NativeType PERIOD = new NativeType(ColumnType.PERIOD, 4);

    /** Don't allow to create an instance. */
    private NativeTypes() {
    }

    /**
     * Creates a STRING type with maximal length is {@code len}.
     *
     * @param len Maximum length of the string, {@link Integer#MAX_VALUE} if not defined.
     * @return Native type.
     */
    public static NativeType stringOf(int len) {
        return new VarlenNativeType(ColumnType.STRING, len);
    }

    /**
     * Creates a BYTES type with maximal length is {@code len}.
     *
     * @param len Maximum length of the byte array, {@link Integer#MAX_VALUE} if not defined.
     * @return Native type.
     */
    public static NativeType blobOf(int len) {
        return new VarlenNativeType(ColumnType.BYTE_ARRAY, len);
    }

    /**
     * Creates a DECIMAL type with maximal precision and scale.
     *
     * @param precision Precision.
     * @param scale Scale.
     * @return Native type.
     */
    public static NativeType decimalOf(int precision, int scale) {
        return new DecimalNativeType(precision, scale);
    }

    /**
     * Creates a TIME type with given precision.
     *
     * @param precision Fractional seconds meaningful digits. Allowed values are 0-9 for second to nanosecond precision.
     * @return Native type.
     */
    public static NativeType time(int precision) {
        return TemporalNativeType.time(precision);
    }

    /**
     * Creates DATETIME type as pair (date, time).
     *
     * @param precision Fractional seconds meaningful digits. Allowed values are 0-9 for second to nanosecond precision.
     * @return Native type.
     */
    public static NativeType datetime(int precision) {
        return TemporalNativeType.datetime(precision);
    }

    /**
     * Creates TIMESTAMP type.
     *
     * @param precision Fractional seconds meaningful digits. Allowed values are 0-9 for second to nanosecond precision.
     * @return Native type.
     */
    public static NativeType timestamp(int precision) {
        return TemporalNativeType.timestamp(precision);
    }

    /** Returns builder to create structured type. */
    public static StructTypeBuilder structBuilder() {
        return new StructTypeBuilder();
    }

    /**
     * Return the native type for specified object.
     *
     * @param val Object to map to native type.
     * @return {@code null} for {@code null} value. Returns {@link NativeType} according to the value's type or throws an exception
     *      if object type is unsupported.
     */
    @Contract("null -> null")
    public static @Nullable NativeType fromObject(@Nullable Object val) {
        if (val == null) {
            return null;
        }

        Class<?> cls = val.getClass();

        if (!supportedClass(cls)) {
            return null;
        }

        if (cls == Boolean.class) {
            return BOOLEAN;
        } else if (cls == Byte.class) {
            return INT8;
        } else if (cls == Short.class) {
            return INT16;
        } else if (cls == Integer.class) {
            return INT32;
        } else if (cls == Long.class) {
            return INT64;
        } else if (cls == Float.class) {
            return FLOAT;
        } else if (cls == Double.class) {
            return DOUBLE;
        } else if (cls == LocalDate.class) {
            return DATE;
        } else if (cls == LocalTime.class) {
            assert val instanceof LocalTime : val.getClass().getCanonicalName();

            return time(derivePrecisionFromNanos(((LocalTime) val).getNano()));
        } else if (cls == LocalDateTime.class) {
            assert val instanceof LocalDateTime : val.getClass().getCanonicalName();

            return datetime(derivePrecisionFromNanos(((LocalDateTime) val).getNano()));
        } else if (cls == Instant.class) {
            assert val instanceof Instant : val.getClass().getCanonicalName();

            return timestamp(derivePrecisionFromNanos(((Instant) val).getNano()));
        } else if (cls == byte[].class) {
            return blobOf(((byte[]) val).length);
        } else if (cls == String.class) {
            return stringOf(((CharSequence) val).length());
        } else if (cls == java.util.UUID.class) {
            return UUID;
        } else if (cls == BigDecimal.class) {
            return decimalOf(((BigDecimal) val).precision(), ((BigDecimal) val).scale());
        }

        throw new UnsupportedOperationException("Class is not supported: " + cls);
    }

    private static boolean supportedClass(Class<?> cls) {
        assert cls != null;

        return cls == Boolean.class
                || cls == Byte.class
                || cls == Short.class
                || cls == Integer.class
                || cls == Long.class
                || cls == Float.class
                || cls == Double.class
                || cls == LocalDate.class
                || cls == LocalTime.class
                || cls == LocalDateTime.class
                || cls == Instant.class
                || cls == byte[].class
                || cls == String.class
                || cls == java.util.UUID.class
                || cls == BigDecimal.class;
    }

    private static int derivePrecisionFromNanos(int nanos) {
        if (nanos == 0) {
            return 0;
        }

        int trailingZeroes = 0;
        while (nanos % 10 == 0) {
            trailingZeroes++;
            nanos /= 10;
        }

        return MAX_TIME_PRECISION - trailingZeroes;
    }

    /** A builder for constructing {@link StructNativeType} instances. */
    public static class StructTypeBuilder {
        private StructTypeBuilder() {
        }

        private final List<Field> fields = new ArrayList<>();

        /**
         * Adds a field to this struct type builder.
         *
         * <p>Fields are added in the order this method is called, and this order is preserved in the resulting {@link StructNativeType}.
         *
         * @param name The name of the field; Must not be null or blank.
         * @param type The native type of the field; Must not be null.
         * @param nullable Whether the field can contain null values.
         * @return This builder instance for method chaining.
         * @throws IllegalArgumentException If any argument violates provided constraints.
         */
        public StructTypeBuilder addField(String name, NativeType type, boolean nullable) {
            if (StringUtils.nullOrBlank(name)) {
                throw new IllegalArgumentException("Name must not be null or blank: "
                        + (name == null ? "<null>" : "\"" + name + "\"") + ".");
            }

            if (type == null) {
                throw new IllegalArgumentException("Type must not be null.");
            }

            fields.add(new Field(name, type, nullable));

            return this;
        }

        /**
         * Adds a field to this struct type builder.
         *
         * <p>Fields are added in the order this method is called, and this order is preserved in the resulting {@link StructNativeType}.
         *
         * @param field The field to add; Must not be null.
         * @return This builder instance for method chaining.
         * @throws IllegalArgumentException If any argument violates provided constraints.
         */
        public StructTypeBuilder addField(Field field) {
            if (field == null) {
                throw new IllegalArgumentException("Field must not be null.");
            }

            fields.add(field);

            return this;
        }

        /**
         * Builds and returns a new {@link StructNativeType} with the fields that have been added.
         *
         * <p>The returned struct contains an immutable copy of the fields in the order they were added. This method can be called multiple
         * times to create multiple struct instances, though subsequent modifications to the builder will not affect previously built
         * instances.
         *
         * @return A new {@code StructNativeType} instance containing the configured fields.
         */
        public StructNativeType build() {
            return new StructNativeType(List.copyOf(fields));
        }
    }
}
