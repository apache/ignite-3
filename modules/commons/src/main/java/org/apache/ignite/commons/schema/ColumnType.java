package org.apache.ignite.commons.schema;

public class ColumnType {
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

    public static VarLenType bitmaskOf(int bits) {
        return new VarLenType(ColumnTypeSpec.BITMASK).lengthOf(bits);
    }

    public static VarLenType string() {
        return new VarLenType(ColumnTypeSpec.STRING);
    }

    public static VarLenType blob() {
        return new VarLenType(ColumnTypeSpec.STRING);
    }

    public static NumericType number(int precision, int scale) {
        return new NumericType(ColumnTypeSpec.STRING).withPrecision(precision).withScale(scale);
    }

    public static class VarLenType extends ColumnType {
        private int length;

        VarLenType(ColumnTypeSpec baseType) {
            super(baseType);
        }

        public VarLenType lengthOf(int length) {
            this.length = length;

            return this;
        }
    }

    private final ColumnTypeSpec typeSpec;

    private ColumnType(ColumnTypeSpec typeSpec) {
        this.typeSpec = typeSpec;
    }

    static class NumericType extends ColumnType {
        int precision;
        int scale;

        public NumericType(ColumnTypeSpec typeSpec) {
            super(typeSpec);
        }

        public NumericType withPrecision(int precision) {
            this.precision = precision;

            return this;
        }

        public NumericType withScale(int scale) {
            this.scale = scale;

            return this;
        }
    }

    enum ColumnTypeSpec {
        INT8, // Convert to numeric.
        INT16,
        INT32,
        INT64,

        UINT8,
        UINT16,
        UINT32,
        UINT64,

        FLOAT, // Convert to decimal???
        DOUBLE,

        //NUMERIC,
        //DECIMAL,

        UUID,

        BITMASK,
        STRING,
        BLOB,
    }
}
