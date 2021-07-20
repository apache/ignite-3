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

package org.apache.ignite.schema;

import java.util.Objects;

/**
 * Predefined column types.
 */
@SuppressWarnings("PublicInnerClass")
public class ColumnType {
    /** 8-bit signed int. */
    public static final ColumnType INT8 = new ColumnType(ColumnTypeSpec.INT8);

    /** 16-bit signed int. */
    public static final ColumnType INT16 = new ColumnType(ColumnTypeSpec.INT16);

    /** 32-bit signed int. */
    public static final ColumnType INT32 = new ColumnType(ColumnTypeSpec.INT32);

    /** 64-bit signed int. */
    public static final ColumnType INT64 = new ColumnType(ColumnTypeSpec.INT64);

    /** 8-bit unsigned int. */
    public static final ColumnType UINT8 = new ColumnType(ColumnTypeSpec.UINT8);

    /** 16-bit unsigned int. */
    public static final ColumnType UINT16 = new ColumnType(ColumnTypeSpec.UINT16);

    /** 32-bit unsigned int. */
    public static final ColumnType UINT32 = new ColumnType(ColumnTypeSpec.UINT32);

    /** 64-bit unsigned int. */
    public static final ColumnType UINT64 = new ColumnType(ColumnTypeSpec.UINT64);

    /** 32-bit float. */
    public static final ColumnType FLOAT = new ColumnType(ColumnTypeSpec.FLOAT);

    /** 64-bit double. */
    public static final ColumnType DOUBLE = new ColumnType(ColumnTypeSpec.DOUBLE);

    /** 128-bit UUID. */
    public static final ColumnType UUID = new ColumnType(ColumnTypeSpec.UUID);

    /** Timezone-free three-part value representing a year, month, and day. */
    public static final ColumnType DATE = new ColumnType(ColumnTypeSpec.DATE);

    /**
     * Bitmask type factory method.
     *
     * @param bits Bitmask size in bits.
     * @return Bitmap type.
     */
    public static VarLenColumnType bitmaskOf(int bits) {
        return new VarLenColumnType(ColumnTypeSpec.BITMASK, bits);
    }

    /**
     * String factory method.
     *
     * @return String type.
     */
    public static VarLenColumnType string() {
        return stringOf(0);
    }

    /**
     * String factory method for fix-sized string type.
     *
     * @param length String length in chars.
     * @return String type.
     */
    public static VarLenColumnType stringOf(int length) {
        return new VarLenColumnType(ColumnTypeSpec.STRING, length);
    }

    /**
     * Blob type factory method.
     *
     * @return Blob type.
     */
    public static VarLenColumnType blobOf() {
        return blobOf(0);
    }

    /**
     * Blob type factory method for fix-sized blob.
     *
     * @param length Blob length in bytes.
     * @return Blob type.
     */
    public static VarLenColumnType blobOf(int length) {
        return new VarLenColumnType(ColumnTypeSpec.BLOB, length);
    }

    /**
     * Decimal type factory method.
     *
     * @param precision Precision.
     * @param scale Scale.
     * @return Numeric type.
     */
    public static NumericColumnType number(int precision, int scale) {
        return new NumericColumnType(ColumnTypeSpec.DECIMAL, precision, scale);
    }

    /**
     * Timezone-free value representing a time of day in hours, minutes, seconds,
     * and subseconds depending on precision.
     *
     * @param precision Subsecond part length. Allowed values are 3/6/9 for millis/micros/nanos.
     * @return Native type.
     */
    public static TemporalColumnType time(int precision) {
        return new TemporalColumnType(ColumnTypeSpec.TIME, precision);
    }

    /**
     * Timezone-free datetime encoded as (date, time).
     *
     * @param precision Subsecond part length. Allowed values are 3/6/9 for millis/micros/nanos.
     * @return Native type.
     */
    public static TemporalColumnType datetime(int precision) {
        return new TemporalColumnType(ColumnTypeSpec.DATETIME, precision);
    }

    /**
     * Number of milliseconds/microseconds/nanoseconds since Jan 1, 1970 00:00:00.000 (with no timezone).
     *
     * @param precision Subsecond part length. Allowed values are 3/6/9 for millis/micros/nanos.
     * @return Native type.
     */
    public static TemporalColumnType timestamp(int precision) {
        return new TemporalColumnType(ColumnTypeSpec.TIMESTAMP, precision);
    }

    /**
     * Column type of variable length.
     */
    public static class VarLenColumnType extends ColumnType {
        /** Max length. */
        private final int length;

        /** Constructor. */
        private VarLenColumnType(ColumnTypeSpec typeSpec, int length) {
            super(typeSpec);

            this.length = length;
        }

        /**
         * @return Max column value length.
         */
        public int length() {
            return length;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            if (!super.equals(o))
                return false;
            VarLenColumnType type = (VarLenColumnType)o;
            return length == type.length;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), length);
        }
    }

    /**
     * Numeric column type.
     */
    public static class NumericColumnType extends ColumnType {
        /** Precision. */
        private final int precision;

        /** Scale. */
        private final int scale;

        /** Constructor. */
        private NumericColumnType(ColumnTypeSpec typeSpec, int precision, int scale) {
            super(typeSpec);

            this.precision = precision;
            this.scale = scale;
        }

        /**
         * @return Precision.
         */
        public int precision() {
            return precision;
        }

        /**
         * @return Scale.
         */
        public int scale() {
            return scale;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            if (!super.equals(o))
                return false;

            NumericColumnType type = (NumericColumnType)o;

            return precision == type.precision &&
                scale == type.scale;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), precision, scale);
        }
    }

    /**
     * Column type of variable length.
     */
    public static class TemporalColumnType extends ColumnType {
        /** Default temporal type precision: microseconds. */
        public static final int DEFAULT_PRECISION = 6;

        /** Length of second's fractional part. */
        private final int precision;

        /**
         * Creates temporal type.
         *
         * @param typeSpec Type spec.
         */
        private TemporalColumnType(ColumnTypeSpec typeSpec, int length) {
            super(typeSpec);

            assert length == 3 || length == 6 || length == 9 : "Unsupported temporal type precision.";

            this.precision = length;
        }

        /**
         * Length of second's fractional part
         *
         * @return Subsecond part length.
         */
        public int precision() {
            return precision;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            if (!super.equals(o))
                return false;

            TemporalColumnType type = (TemporalColumnType)o;

            return precision == type.precision;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), precision);
        }
    }

    /**
     * Column type spec.
     */
    public enum ColumnTypeSpec {
        /** 8-bit signed integer. */
        INT8,

        /** 16-bit signed integer. */
        INT16,

        /** 32-bit signed integer. */
        INT32,

        /** 64-bit signed integer. */
        INT64,

        /** 8-bit unsigned integer. */
        UINT8,

        /** 16-bit unsigned integer. */
        UINT16,

        /** 32-bit unsigned integer. */
        UINT32,

        /** 64-bit unsigned integer. */
        UINT64,

        /** 32-bit single-precision floating-point number. */
        FLOAT,

        /** 64-bit double-precision floating-point number. */
        DOUBLE,

        /** A decimal floating-point number. */
        DECIMAL,

        /** Timezone-free date. */
        DATE,

        /** Timezone-free time. */
        TIME,

        /** Timezone-free datetime. */
        DATETIME,

        /** Number of milliseconds since Jan 1, 1970 00:00:00.000 (with no timezone). */
        TIMESTAMP,

        /** 128-bitUUID. */
        UUID,

        /** Bit mask. */
        BITMASK,

        /** String. */
        STRING,

        /** Binary data. */
        BLOB,
    }

    /** Type spec. */
    private final ColumnTypeSpec typeSpec;

    /** Constructor. */
    private ColumnType(ColumnTypeSpec typeSpec) {
        this.typeSpec = typeSpec;
    }

    /**
     * @return Type spec.
     */
    public ColumnTypeSpec typeSpec() {
        return typeSpec;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ColumnType type = (ColumnType)o;

        return typeSpec == type.typeSpec;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(typeSpec);
    }
}
