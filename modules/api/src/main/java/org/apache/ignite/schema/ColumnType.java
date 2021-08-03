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
     * Number type factory method.
     *
     * @param precision Precision of value.
     * @return Number type.
     */
    public static NumberColumnType numberOf(int precision) {
        if (precision <= 0)
            throw new IllegalArgumentException("Precision [" + precision + "] must be positive integer value.");

        return new NumberColumnType(ColumnTypeSpec.NUMBER, precision);
    }

    /**
     * Number type factory method.
     *
     * @return Number type.
     */
    public static NumberColumnType numberOf() {
        return new NumberColumnType(ColumnTypeSpec.NUMBER, NumberColumnType.UNLIMITED_PRECISION);
    }

    /**
     * Decimal type factory method.
     *
     * @param precision Precision.
     * @param scale Scale.
     * @return Decimal type.
     */
    public static DecimalColumnType decimalOf(int precision, int scale) {
        if (precision <= 0)
            throw new IllegalArgumentException("Precision [" + precision + "] must be positive integer value.");

        if (scale < 0)
            throw new IllegalArgumentException("Scale [" + scale + "] must be non-negative integer value.");

        if (precision < scale)
            throw new IllegalArgumentException("Precision [" + precision + "] must be" +
                " not lower than scale [ " + scale + " ].");

        return new DecimalColumnType(ColumnTypeSpec.DECIMAL, precision, scale);
    }

    /**
     * Decimal type factory method with default precision and scale values.
     *
     * @return Decimal type.
     */
    public static DecimalColumnType decimalOf() {
        return new DecimalColumnType(
            ColumnTypeSpec.DECIMAL,
            DecimalColumnType.DEFAULT_PRECISION,
            DecimalColumnType.DEFAULT_SCALE
        );
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
     * Decimal column type.
     */
    public static class DecimalColumnType extends ColumnType {
        /** Default precision. */
        public static final int DEFAULT_PRECISION = 19;

        /** Default scale. */
        public static final int DEFAULT_SCALE = 3;

        /** Precision. */
        private final int precision;

        /** Scale. */
        private final int scale;

        /** Constructor. */
        private DecimalColumnType(ColumnTypeSpec typeSpec, int precision, int scale) {
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
            DecimalColumnType type = (DecimalColumnType)o;
            return precision == type.precision &&
                scale == type.scale;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), precision, scale);
        }
    }

    /**
     * Number column type.
     */
    public static class NumberColumnType extends ColumnType {
        /** Undefined precision. */
        public static final int UNLIMITED_PRECISION = -1;

        /** Max precision of value. If -1, column has no precision restrictions. */
        private final int precision;

        /** Constructor. */
        private NumberColumnType(ColumnTypeSpec typeSpec, int precision) {
            super(typeSpec);

            this.precision = precision;
        }

        /**
         * @return Max value precision.
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
            NumberColumnType type = (NumberColumnType)o;
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
        INT8,
        INT16,
        INT32,
        INT64,

        UINT8,
        UINT16,
        UINT32,
        UINT64,

        FLOAT,
        DOUBLE,

        DECIMAL,

        UUID,

        BITMASK,
        STRING,
        BLOB,

        NUMBER,
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
