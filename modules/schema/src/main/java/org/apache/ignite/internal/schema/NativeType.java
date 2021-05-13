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

import java.util.BitSet;
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
    private final NativeTypeSpec typeSpec;

    /** Type size in bytes. */
    private final int sizeInBytes;

    /**
     * Constructor for fixed-length types.
     *
     * @param typeSpec Type spec.
     * @param sizeInBytes Type size in bytes.
     */
    protected NativeType(NativeTypeSpec typeSpec, int sizeInBytes) {
        if (!typeSpec.fixedLength())
            throw new IllegalArgumentException("Size must be provided only for fixed-length types: " + typeSpec);

        if (sizeInBytes <= 0)
            throw new IllegalArgumentException("Size must be positive [typeSpec=" + typeSpec + ", size=" + sizeInBytes + ']');

        this.typeSpec = typeSpec;
        this.sizeInBytes = sizeInBytes;
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
        this.sizeInBytes = 0;
    }

    /**
     * @return Size in bytes of the type if it is a fixlen type. For varlen types the return value is undefined, so the user
     * should explicitly check {@code spec().fixedLength()} before using this method.
     *
     * @see NativeTypeSpec#fixedLength()
     */
    public int sizeInBytes() {
        return sizeInBytes;
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
                return new VarlenNativeType(NativeTypeSpec.STRING, ((String)val).length());

            case BYTES:
                return new VarlenNativeType(NativeTypeSpec.BYTES, ((byte[])val).length);

            case BITMASK:
                return BitmaskNativeType.of(((BitSet)val).length());

            default:
                assert false : "Unexpected type: " + spec;
                return null;
        }
    }

    /** */
    public static NativeType of(ColumnType type) {
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
                return new BitmaskNativeType(((ColumnType.VarLenColumnType)type).length());

            case STRING:
                return new VarlenNativeType(NativeTypeSpec.STRING, ((ColumnType.VarLenColumnType)type).length());

            case BLOB:
                return new VarlenNativeType(NativeTypeSpec.BYTES, ((ColumnType.VarLenColumnType)type).length());

            default:
                throw new InvalidTypeException("Unexpected type " + type);
        }
    }

    /** */
    public boolean mismatch(NativeType type) {
        return this != type && type != null && typeSpec != type.typeSpec;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        NativeType that = (NativeType)o;

        return sizeInBytes == that.sizeInBytes && typeSpec == that.typeSpec;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = typeSpec.hashCode();

        res = 31 * res + sizeInBytes;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(NativeType o) {
        // Fixed-sized types go first.
        if (sizeInBytes <= 0 && o.sizeInBytes > 0)
            return 1;

        if (sizeInBytes > 0 && o.sizeInBytes <= 0)
            return -1;

        // Either size is -1 for both, or positive for both. Compare sizes, then description.
        int cmp = Integer.compare(sizeInBytes, o.sizeInBytes);

        if (cmp != 0)
            return cmp;

        return typeSpec.name().compareTo(o.typeSpec.name());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NativeType.class.getSimpleName(),
            "name", typeSpec.name(),
            "len", sizeInBytes,
            "fixed", typeSpec.fixedLength());
    }

}
