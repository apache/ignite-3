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

package org.apache.ignite.internal.schema;

import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.ColumnType;

/**
 * A thin wrapper over {@link NativeTypeSpec} to instantiate parameterized constrained types.
 */
public class NativeType implements Comparable<NativeType> {
    /** */
    public static final NativeType BYTE = new NativeType(NativeTypeSpec.BYTE, 1);

    /** */
    public static final NativeType SHORT = new NativeType(NativeTypeSpec.SHORT, 2);

    /** */
    public static final NativeType INTEGER = new NativeType(NativeTypeSpec.INTEGER, 4);

    /** */
    public static final NativeType LONG = new NativeType(NativeTypeSpec.LONG, 8);

    /** */
    public static final NativeType FLOAT = new NativeType(NativeTypeSpec.FLOAT, 4);

    /** */
    public static final NativeType DOUBLE = new NativeType(NativeTypeSpec.DOUBLE, 8);

    /** */
    public static final NativeType UUID = new NativeType(NativeTypeSpec.UUID, 16);

    /** */
    public static final NativeType STRING = new NativeType(NativeTypeSpec.STRING);

    /** */
    public static final NativeType BYTES = new NativeType(NativeTypeSpec.BYTES);

    /** */
    private final NativeTypeSpec typeSpec;

    /** Type length. */
    private final int len;

    /**
     * Constructor for fixed-length types.
     *
     * @param typeSpec Type spec.
     * @param len Type length.
     */
    protected NativeType(NativeTypeSpec typeSpec, int len) {
        if (len <= 0)
            throw new IllegalArgumentException("Size must be positive [typeSpec=" + typeSpec + ", size=" + len + ']');

        this.typeSpec = typeSpec;
        this.len = len;
    }

    /**
     * Constructor for variable-length types.
     *
     * @param typeSpec Type spec.
     */
    protected NativeType(NativeTypeSpec typeSpec) {
        if (typeSpec.fixedLength())
            throw new IllegalArgumentException("Fixed-length types must be created by the " +
                "length-aware constructor: " + typeSpec);

        this.typeSpec = typeSpec;
        this.len = 0;
    }

    /**
     * @return Length of the type if it is a fixlen type. For varlen types the return value is undefined, so the user
     * should explicitly check {@code spec().fixedLength()} before using this method.
     *
     * @see NativeTypeSpec#fixedLength()
     */
    public int length() {
        return len;
    }

    /**
     * @return Type specification enum.
     */
    public NativeTypeSpec spec() {
        return typeSpec;
    }

    /**
     * Return the native type for specified object.
     *
     * @return {@code null} for {@code null} value. Otherwise returns NativeType according to the value's type.
     */
    public static NativeType fromObject(Object val) {
        NativeTypeSpec spec = NativeTypeSpec.fromObject(val);

        if (spec == null)
            return null;

        switch (spec) {
            case BYTE:
                return BYTE;

            case SHORT:
                return SHORT;

            case INTEGER:
                return INTEGER;

            case LONG:
                return LONG;

            case FLOAT:
                return FLOAT;

            case DOUBLE:
                return DOUBLE;

            case UUID:
                return UUID;

            case STRING:
                return new NativeType(NativeTypeSpec.STRING, ((String)val).length());

            case BYTES:
                return new NativeType(NativeTypeSpec.BYTE, ((byte[])val).length);

            case BITMASK:
                // TODO: what the object is present a bitmask?
                return Bitmask.of(0);

            default:
                assert false : "Unexpected type: " + spec;
                return null;
        }
    }

    /** */
    public static NativeType fromColumnType(ColumnType type) {
        switch (type.typeSpec()) {
            case INT8:
                return BYTE;

            case INT16:
                return SHORT;

            case INT32:
                return INTEGER;

            case INT64:
                return LONG;

            case UINT8:
            case UINT16:
            case UINT32:
            case UINT64:
                throw new UnsupportedOperationException("Unsigned types are not supported yet.");

            case FLOAT:
                return FLOAT;

            case DOUBLE:
                return DOUBLE;

            case DECIMAL:
                ColumnType.NumericColumnType numType = (ColumnType.NumericColumnType)type;
                return new NumericNativeType(numType.precision(), numType.scale());

            case UUID:
                return UUID;

            case BITMASK:
                return new Bitmask(((ColumnType.VarLenColumnType)type).length());

            case STRING:
                return new NativeType(NativeTypeSpec.STRING, ((ColumnType.VarLenColumnType)type).length());

            case BLOB:
                return new NativeType(NativeTypeSpec.BYTES, ((ColumnType.VarLenColumnType)type).length());

            default:
                assert false : "Unexpected type " + type;

                return null;
        }
    }

    /** */
    public boolean match(NativeType type) {
        if (this == type)
            return true;

        if (type == null)
            return true;

        return typeSpec == type.typeSpec && len >= type.len ;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        NativeType that = (NativeType)o;

        return len == that.len && typeSpec == that.typeSpec;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = typeSpec.hashCode();

        res = 31 * res + len;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(NativeType o) {
        // Fixed-sized types go first.
        if (len <= 0 && o.len > 0)
            return 1;

        if (len > 0 && o.len <= 0)
            return -1;

        // Either size is -1 for both, or positive for both. Compare sizes, then description.
        int cmp = Integer.compare(len, o.len);

        if (cmp != 0)
            return cmp;

        return typeSpec.name().compareTo(o.typeSpec.name());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NativeType.class.getSimpleName(),
            "name", typeSpec.name(),
            "len", len,
            "fixed", typeSpec.fixedLength());
    }

    /**
     * Numeric column type.
     */
    public static class NumericNativeType extends NativeType {
        /** Precision. */
        private final int precision;

        /** Scale. */
        private final int scale;

        /** Constructor. */
        private NumericNativeType(int precision, int scale) {
            super(NativeTypeSpec.DECIMAL);

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
            NumericNativeType type = (NumericNativeType)o;
            return precision == type.precision &&
                scale == type.scale;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), precision, scale);
        }
    }
}
