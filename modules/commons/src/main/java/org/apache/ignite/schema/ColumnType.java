package org.apache.ignite.schema;

import java.util.Objects;

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

    /** Bitmask type factory method. */
    public static VarlenColumnType bitmaskOf(int bits) {
        return new VarlenColumnType(ColumnTypeSpec.BITMASK, bits);
    }

    /** String factory method. */
    public static VarlenColumnType string() {
        return stringOf(0);
    }

    /** String factory method. */
    public static VarlenColumnType stringOf(int length) {
        return new VarlenColumnType(ColumnTypeSpec.STRING, length);
    }

    /** Blob type factory method. */
    public static VarlenColumnType blobOf() {
        return blobOf(0);
    }

    /** Blob type factory method. */
    public static VarlenColumnType blobOf(int length) {
        return new VarlenColumnType(ColumnTypeSpec.BLOB, length);
    }

    /** Decimal type factory method. */
    public static NumericColumnType number(int precision, int scale) {
        return new NumericColumnType(ColumnTypeSpec.DECIMAL, precision, scale);
    }

    /**
     * Varlen column type.
     */
    public static class VarlenColumnType extends ColumnType {
        /** Max length. */
        private final int length;

        /** Constructor. */
        private VarlenColumnType(ColumnTypeSpec typeSpec, int length) {
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
            VarlenColumnType type = (VarlenColumnType)o;
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
        /** Presicion. */
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
