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
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for storage built-in data types definition. The class contains predefined values for fixed-sized types and some of the
 * variable-sized types. Parameterized types, such as bitmask of size {@code n} bits or number of max n bytes are created using static
 * methods.
 *
 * <p>An instance of native type provides necessary indirection to read any field as an instance of {@code java.lang.Object} to avoid
 * switching inside the row methods.
 */

public enum NativeTypeSpec {
    /**
     * Native type representing a single-byte signed value.
     */
    INT8(true),

    /**
     * Native type representing a two-bytes signed value.
     */
    INT16(true),

    /**
     * Native type representing a four-bytes signed value.
     */
    INT32(true),

    /**
     * Native type representing an eight-bytes signed value.
     */
    INT64(true),

    /**
     * Native type representing a four-bytes floating-point value.
     */
    FLOAT(true),

    /**
     * Native type representing an eight-bytes floating-point value.
     */
    DOUBLE(true),

    /**
     * Native type representing a BigDecimal.
     */
    DECIMAL(false),

    /**
     * Native type representing a UUID.
     */
    UUID(true),

    /**
     * Native type representing a string.
     */
    STRING(false),

    /**
     * Native type representing an arbitrary byte array.
     */
    BYTES(false),

    /**
     * Native type representing a bitmask.
     */
    BITMASK(true),

    /**
     * Native type representing a BigInteger.
     */
    NUMBER(false),

    /**
     * Native type representing a timezone-free date.
     */
    DATE(true),

    /**
     * Native type representing a timezone-free time.
     */
    TIME(true),

    /**
     * Native type representing a timezone-free datetime.
     */
    DATETIME(true),

    /**
     * Point on the time-line. Number of ticks since {@code 1970-01-01T00:00:00Z}. Tick unit depends on precision.
     */
    TIMESTAMP(true),

    /**
     * Native type representing a boolean value.
     */
    BOOLEAN(true);

    /** Cached array with all enum values. */
    private static final NativeTypeSpec[] VALUES = values();

    /** Flag indicating whether this type specifies a fixed-length type. */
    private final boolean fixedSize;

    /**
     * Constructs a type with the given description and size.
     *
     * @param fixedSize Flag indicating whether this type specifies a fixed-length type.
     */
    NativeTypeSpec(boolean fixedSize) {
        this.fixedSize = fixedSize;
    }

    /**
     * Returns the spec instance by its ordinal. {@code null} if the ordinal is invalid.
     */
    public static @Nullable NativeTypeSpec fromOrdinal(byte ordinal) {
        return ordinal < 0 || ordinal >= VALUES.length ? null : VALUES[ordinal];
    }

    /**
     * Get fixed length flag: {@code true} for fixed-length types, {@code false} otherwise.
     */
    public boolean fixedLength() {
        return fixedSize;
    }

    /**
     * Maps class to native type.
     *
     * @param cls Class to map to native type.
     * @return Native type.
     */
    public static @Nullable NativeTypeSpec fromClass(Class<?> cls) {
        assert cls != null;

        // Primitives.
        if (cls == boolean.class) {
            return BOOLEAN;
        } else if (cls == byte.class) {
            return INT8;
        } else if (cls == short.class) {
            return INT16;
        } else if (cls == int.class) {
            return INT32;
        } else if (cls == long.class) {
            return INT64;
        } else if (cls == float.class) {
            return FLOAT;
        } else if (cls == double.class) {
            return DOUBLE;
        } else if (cls == Boolean.class) { // Boxed primitives.
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
        } else if (cls == LocalDate.class) { // Temporal types.
            return DATE;
        } else if (cls == LocalTime.class) {
            return TIME;
        } else if (cls == LocalDateTime.class) {
            return DATETIME;
        } else if (cls == Instant.class) {
            return TIMESTAMP;
        } else if (cls == byte[].class) { // Other types.
            return BYTES;
        } else if (cls == String.class) {
            return STRING;
        } else if (cls == java.util.UUID.class) {
            return UUID;
        } else if (cls == BitSet.class) {
            return BITMASK;
        } else if (cls == BigInteger.class) {
            return NUMBER;
        } else if (cls == BigDecimal.class) {
            return DECIMAL;
        }

        return null;
    }

    /**
     * Maps native type spec to a particular java class.
     *
     * @param spec Type spec to map.
     * @param nullable Whether class should accept null values.
     * @return Java class.
     */
    public static Class<?> toClass(NativeTypeSpec spec, boolean nullable) {
        assert spec != null;

        switch (spec) {
            case BOOLEAN:
                return nullable ? Boolean.class : boolean.class;
            case INT8:
                return nullable ? Byte.class : byte.class;
            case INT16:
                return nullable ? Short.class : short.class;
            case INT32:
                return nullable ? Integer.class : int.class;
            case INT64:
                return nullable ? Long.class : long.class;
            case FLOAT:
                return nullable ? Float.class : float.class;
            case DOUBLE:
                return nullable ? Double.class : double.class;
            case BITMASK:
                return BitSet.class;
            case BYTES:
                return byte[].class;
            case STRING:
                return String.class;
            case DATE:
                return LocalDate.class;
            case TIME:
                return LocalTime.class;
            case TIMESTAMP:
                return Instant.class;
            case DATETIME:
                return LocalDateTime.class;
            case UUID:
                return java.util.UUID.class;
            case NUMBER:
                return BigInteger.class;
            case DECIMAL:
                return BigDecimal.class;
            default:
                throw new IllegalStateException("Unknown typeSpec " + spec);
        }
    }

    /**
     * Gets client type corresponding to this server type.
     *
     * @return Client type code.
     */
    public ColumnType asColumnType() {
        ColumnType columnType = asColumnTypeOrNull();

        if (columnType == null) {
            throw new IgniteException(Common.INTERNAL_ERR, "Unsupported native type: " + this);
        }

        return columnType;
    }

    /**
     * Gets client type corresponding to this server type.
     *
     * @return Client type code.
     */
    @Nullable
    public ColumnType asColumnTypeOrNull() {
        switch (this) {
            case BOOLEAN:
                return ColumnType.BOOLEAN;

            case INT8:
                return ColumnType.INT8;

            case INT16:
                return ColumnType.INT16;

            case INT32:
                return ColumnType.INT32;

            case INT64:
                return ColumnType.INT64;

            case FLOAT:
                return ColumnType.FLOAT;

            case DOUBLE:
                return ColumnType.DOUBLE;

            case DECIMAL:
                return ColumnType.DECIMAL;

            case NUMBER:
                return ColumnType.NUMBER;

            case UUID:
                return ColumnType.UUID;

            case STRING:
                return ColumnType.STRING;

            case BYTES:
                return ColumnType.BYTE_ARRAY;

            case BITMASK:
                return ColumnType.BITMASK;

            case DATE:
                return ColumnType.DATE;

            case TIME:
                return ColumnType.TIME;

            case DATETIME:
                return ColumnType.DATETIME;

            case TIMESTAMP:
                return ColumnType.TIMESTAMP;

            default:
                return null;
        }
    }

    /**
     * Maps object to native type.
     *
     * @param val Object to map.
     * @return Native type.
     */
    public static @Nullable NativeTypeSpec fromObject(@Nullable Object val) {
        return val != null ? fromClass(val.getClass()) : null;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(NativeTypeSpec.class.getSimpleName(),
                "name", name(),
                "fixed", fixedLength());
    }
}
