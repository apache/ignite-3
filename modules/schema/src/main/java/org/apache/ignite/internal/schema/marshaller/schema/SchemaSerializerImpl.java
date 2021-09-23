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

package org.apache.ignite.internal.schema.marshaller.schema;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;

/**
 * Serialize SchemaDescriptor object to byte array and vice versa.
 */
public class SchemaSerializerImpl extends AbstractSchemaSerializer {
    /** Instance. */
    public static final AbstractSchemaSerializer INSTANCE = new SchemaSerializerImpl();

    /** String array length. */
    private static final int STRING_HEADER = 4;

    /** Array length. */
    private static final int ARRAY_HEADER_LENGTH = 4;

    /** Byte. */
    private static final int BYTE = 1;

    /** Short. */
    private static final int SHORT = 2;

    /** Int. */
    private static final int INT = 4;

    /** Schema version. */
    private static final short SCHEMA_VER = 1;

    /**
     * Default constructor.
     */
    public SchemaSerializerImpl() {
        super(SCHEMA_VER);
    }

    /** {@inheritDoc} */
    @Override public byte[] bytes(SchemaDescriptor desc, ByteBuffer byteBuf) {
        byteBuf.putShort(SCHEMA_VER);
        byteBuf.putInt(desc.version());

        appendColumns(desc.keyColumns(), byteBuf);
        appendColumns(desc.valueColumns(), byteBuf);

        Column[] affinityCols = desc.affinityColumns();

        byteBuf.putInt(affinityCols.length);

        for (Column column : affinityCols)
            appendString(column.name(), byteBuf);

        return byteBuf.array();
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor value(ByteBuffer byteBuf) {
        int ver = byteBuf.getInt();

        Column[] keyCols = readColumns(byteBuf);
        Column[] valCols = readColumns(byteBuf);

        int affinityColsSize = byteBuf.getInt();

        String[] affinityCols = new String[affinityColsSize];

        for (int i = 0; i < affinityColsSize; i++)
            affinityCols[i] = readString(byteBuf);

        return new SchemaDescriptor(ver, keyCols, affinityCols, valCols);
    }

    /** {@inheritDoc} */
    @Override public int size(SchemaDescriptor desc) {
        return SHORT +                      //Assembler version
            INT +                          //Descriptor version
            getColumnsSize(desc.keyColumns()) +
            getColumnsSize(desc.valueColumns()) +
            ARRAY_HEADER_LENGTH +          //Affinity columns length
            getStringArraySize(desc.affinityColumns());
    }

    /**
     * Gets column names array size in bytes.
     *
     * @param cols Column array.
     * @return Size of an array with column names.
     */
    private int getStringArraySize(Column[] cols) {
        int size = ARRAY_HEADER_LENGTH;      //String array size header
        for (Column column : cols)
            size += getStringSize(column.name());

        return size;
    }

    /**
     * Gets columns array size in bytes.
     *
     * @param cols Column array.
     * @return Size of column array, including column name and column native type.
     */
    private int getColumnsSize(Columns cols) {
        int size = ARRAY_HEADER_LENGTH; //cols array length

        for (Column column : cols.columns())
            size += INT +                      //Schema index
                BYTE +                         //nullable flag
                getStringSize(column.name()) +
                getNativeTypeSize(column.type());

        return size;
    }

    /**
     * Gets native type size in bytes.
     *
     * @param type Native type.
     * @return Native type size depending on NativeTypeSpec params.
     */
    private int getNativeTypeSize(NativeType type) {
        int typeSize = 0;

        switch (type.spec()) {
            case STRING:
            case BYTES:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case NUMBER:
            case BITMASK:
                typeSize += INT; //For precision, len or bits

                break;
            case DECIMAL:
                typeSize += INT; //For precision
                typeSize += INT; //For scale

                break;
            default:
                break;
        }

        return getStringSize(type.spec().name()) + //native type name
            typeSize;
    }

    /**
     * Gets string size in bytes.
     *
     * @param str String.
     * @return Byte array size.
     */
    private int getStringSize(String str) {
        return STRING_HEADER + //string byte array header
            str.getBytes().length; // string byte array length
    }

    /**
     * Appends column array to byte buffer.
     *
     * @param buf Byte buffer.
     * @param cols Column array.
     */
    private void appendColumns(Columns cols, ByteBuffer buf) {
        Column[] colArr = cols.columns();

        buf.putInt(colArr.length);

        for (Column column : colArr)
            appendColumn(column, buf);
    }

    /**
     * Appends column to byte buffer.
     *
     * @param buf Byte buffer.
     * @param col Column.
     */
    private void appendColumn(Column col, ByteBuffer buf) {
        buf.putInt(col.schemaIndex());
        buf.put((byte)(col.nullable() ? 1 : 0));
        appendString(col.name(), buf);
        appendNativeType(buf, col.type());
    }

    /**
     * Appends native type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type Native type.
     */
    private void appendNativeType(ByteBuffer buf, NativeType type) {
        appendString(type.spec().name(), buf);

        switch (type.spec()) {
            case STRING:
            case BYTES:
                appendTypeLen(buf, type);
                break;
            case BITMASK:
                appendBits(buf, type);
                break;
            case DECIMAL:
                appendPrecision(buf, type);
                appendScale(buf, type);
                break;
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case NUMBER:
                appendPrecision(buf, type);
                break;
            default:
                break;
        }
    }

    /**
     * Appends precision of native type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type Native type with precision.
     */
    private void appendPrecision(ByteBuffer buf, NativeType type) {
        NativeTypeSpec spec = type.spec();
        int precision;

        if (spec == NativeTypeSpec.DECIMAL)
            precision = ((DecimalNativeType)type).precision();
        else if (spec == NativeTypeSpec.NUMBER)
            precision = ((NumberNativeType)type).precision();
        else if (type instanceof TemporalNativeType)
            precision = ((TemporalNativeType)type).precision();
        else
            throw new IllegalArgumentException("Native type does not contain precision " + type);

        buf.putInt(precision);
    }

    /**
     * Appends scale of native type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type Native type with scale.
     */
    private void appendScale(ByteBuffer buf, NativeType type) {
        assert type.spec() == NativeTypeSpec.DECIMAL;

        int scale = ((DecimalNativeType)type).scale();

        buf.putInt(scale);
    }

    /**
     * Appends len of native type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type VarLen native type.
     */
    private void appendTypeLen(ByteBuffer buf, NativeType type) {
        assert type.spec() == NativeTypeSpec.STRING || type.spec() == NativeTypeSpec.BYTES;

        int len = ((VarlenNativeType)type).length();

        buf.putInt(len);
    }

    /**
     * Appends bit len of bitmask native type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type Bitmask native type.
     */
    private void appendBits(ByteBuffer buf, NativeType type) {
        assert type.spec() == NativeTypeSpec.BITMASK;

        int bits = ((BitmaskNativeType)type).bits();

        buf.putInt(bits);
    }

    /**
     * Appends string byte representation to byte buffer.
     *
     * @param buf Byte buffer.
     * @param str String.
     */
    private void appendString(String str, ByteBuffer buf) {
        byte[] bytes = str.getBytes();

        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    /**
     * Reads column array from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Column array.
     */
    private Column[] readColumns(ByteBuffer buf) {
        int size = buf.getInt();

        Column[] colArr = new Column[size];

        for (int i = 0; i < size; i++)
            colArr[i] = readColumn(buf);

        return colArr;
    }

    /**
     * Reads column from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Column.
     */
    private Column readColumn(ByteBuffer buf) {
        int schemaIdx = buf.getInt();
        boolean nullable = buf.get() == 1;
        String name = readString(buf);

        NativeType nativeType = fromByteBuffer(buf);

        return new Column(name, nativeType, nullable).copy(schemaIdx);
    }

    /**
     * Reads native type from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Native type.
     */
    private NativeType fromByteBuffer(ByteBuffer buf) {
        String nativeTypeSpecName = readString(buf);

        NativeTypeSpec spec = NativeTypeSpec.valueOf(nativeTypeSpecName);

        switch (spec) {
            case STRING:
                int strLen = buf.getInt();

                return NativeTypes.stringOf(strLen);

            case BYTES:
                int len = buf.getInt();

                return NativeTypes.blobOf(len);

            case BITMASK:
                int bits = buf.getInt();

                return NativeTypes.bitmaskOf(bits);

            case DECIMAL: {
                int precision = buf.getInt();
                int scale = buf.getInt();

                return NativeTypes.decimalOf(precision, scale);
            }
            case TIME: {
                int precision = buf.getInt();

                return NativeTypes.time(precision);
            }
            case DATETIME: {
                int precision = buf.getInt();

                return NativeTypes.datetime(precision);
            }
            case TIMESTAMP: {
                int precision = buf.getInt();

                return NativeTypes.timestamp(precision);
            }
            case NUMBER: {
                int precision = buf.getInt();

                return NativeTypes.numberOf(precision);
            }
            case INT8:
                return NativeTypes.INT8;

            case INT16:
                return NativeTypes.INT16;

            case INT32:
                return NativeTypes.INT32;

            case INT64:
                return NativeTypes.INT64;

            case FLOAT:
                return NativeTypes.FLOAT;

            case DOUBLE:
                return NativeTypes.DOUBLE;

            case UUID:
                return NativeTypes.UUID;

            case DATE:
                return NativeTypes.DATE;
        }

        throw new InvalidTypeException("Unexpected type " + spec);
    }

    /**
     * Reads string from byte buffer.
     *
     * @param buf Byte buffer.
     * @return String.
     */
    private String readString(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] arr = new byte[len];

        buf.get(arr);

        return new String(arr);
    }
}
