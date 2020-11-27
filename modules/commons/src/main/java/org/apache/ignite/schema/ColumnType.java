package org.apache.ignite.schema;

import java.util.Objects;

public class ColumnType {
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

        UUID,

        BITMASK,
        STRING,
        BLOB,
    }

    // Syntax sugar.
    public static final ColumnType INT8 = new ColumnType(ColumnTypeSpec.INT8);
    public static final ColumnType INT16 = new ColumnType(ColumnTypeSpec.INT16);
    public static final ColumnType INT32 = new ColumnType(ColumnTypeSpec.INT32);
    public static final ColumnType INT64 = new ColumnType(ColumnTypeSpec.INT64);

    public static final ColumnType UINT8 = new ColumnType(ColumnTypeSpec.UINT8);
    public static final ColumnType UINT16 = new ColumnType(ColumnTypeSpec.UINT16);
    public static final ColumnType UINT32 = new ColumnType(ColumnTypeSpec.UINT32);
    public static final ColumnType UINT64 = new ColumnType(ColumnTypeSpec.UINT64);

    public static final ColumnType FLOAT = new ColumnType(ColumnTypeSpec.FLOAT);
    public static final ColumnType DOUBLE = new ColumnType(ColumnTypeSpec.DOUBLE);

    public static final ColumnType UUID = new ColumnType(ColumnTypeSpec.UUID);

    public static VarlenColumnType bitmaskOf(int bits) {
        return new VarlenColumnType(ColumnTypeSpec.BITMASK, bits);
    }

    public static VarlenColumnType string() {
        return stringOf(0);
    }

    public static VarlenColumnType stringOf(int length) {
        return new VarlenColumnType(ColumnTypeSpec.STRING, length);
    }

    public static VarlenColumnType blobOf() {
        return blobOf(0);
    }

    public static VarlenColumnType blobOf(int length) {
        return new VarlenColumnType(ColumnTypeSpec.BLOB, length);
    }

    public static NumericColumnType number(int precision, int scale) {
        return new NumericColumnType(ColumnTypeSpec.STRING, precision, scale);
    }

    private final ColumnTypeSpec typeSpec;

    private ColumnType(ColumnTypeSpec typeSpec) {
        this.typeSpec = typeSpec;
    }

    public ColumnTypeSpec typeSpec() {
        return typeSpec;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ColumnType type = (ColumnType)o;

        return typeSpec == type.typeSpec;
    }

    @Override public int hashCode() {
        return Objects.hash(typeSpec);
    }

    public static class VarlenColumnType extends ColumnType {
        private final int length;

        private VarlenColumnType(ColumnTypeSpec typeSpec, int length) {
            super(typeSpec);

            this.length = length;
        }

        public int length() {
            return length;
        }

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

        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), length);
        }
    }

    public static class NumericColumnType extends ColumnType {
        private final int precision;
        private final int scale;

        private NumericColumnType(ColumnTypeSpec typeSpec, int precision, int scale) {
            super(typeSpec);

            this.precision = precision;
            this.scale = scale;
        }

        public int precision() {
            return precision;
        }

        public int scale() {
            return scale;
        }

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

        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), precision, scale);
        }
    }
}
