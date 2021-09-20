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

import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.DecimalNativeType;
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

    /** Schema version. */
    private static final short SCHEMA_VER = 1;

    /**
     * Default constructor.
     */
    public SchemaSerializerImpl() {
        super(SCHEMA_VER);
    }

    /** {@inheritDoc} */
    @Override public ExtendedByteBuffer bytes(SchemaDescriptor desc, ExtendedByteBuffer byteBuf) {
        byteBuf.putShort(SCHEMA_VER);
        byteBuf.putInt(desc.version());

        appendColumns(desc.keyColumns(), byteBuf);
        appendColumns(desc.valueColumns(), byteBuf);

        Column[] affinityCols = desc.affinityColumns();

        byteBuf.putInt(affinityCols.length);

        for (Column column : affinityCols)
            byteBuf.putString(column.name());

        return byteBuf;
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor value(ExtendedByteBuffer byteBuf) {
        int ver = byteBuf.getInt();

        Column[] keyCols = readColumns(byteBuf);
        Column[] valCols = readColumns(byteBuf);

        int affinityColsSize = byteBuf.getInt();

        String[] affinityCols = new String[affinityColsSize];

        for (int i = 0; i < affinityColsSize; i++)
            affinityCols[i] = byteBuf.getString();

        return new SchemaDescriptor(ver, keyCols, affinityCols, valCols);
    }

    /**
     * Appends column array to byte buffer.
     *
     * @param buf Byte buffer.
     * @param cols Column array.
     */
    private void appendColumns(Columns cols, ExtendedByteBuffer buf) {
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
    private void appendColumn(Column col, ExtendedByteBuffer buf) {
        buf.putInt(col.schemaIndex());
        buf.put((byte)(col.nullable() ? 1 : 0));
        buf.putString(col.name());
        appendNativeType(buf, col.type());
    }

    /**
     * Appends native type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type Native type.
     */
    private void appendNativeType(ExtendedByteBuffer buf, NativeType type) {
        buf.putString(type.spec().name());

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
    private void appendPrecision(ExtendedByteBuffer buf, NativeType type) {
        NativeTypeSpec spec = type.spec();
        int precision;

        if (spec == NativeTypeSpec.DECIMAL)
            precision = ((DecimalNativeType)type).precision();
        else if (spec == NativeTypeSpec.NUMBER)
            precision = ((NumberNativeType)type).precision();
        else if (type instanceof TemporalNativeType)
            precision = ((TemporalNativeType)type).precision();
        else throw new IllegalArgumentException();

        buf.putInt(precision);
    }

    /**
     * Appends scale of native type to byte buffer.
     *
     * @param buf Byte buffer.
     * @param type Native type with scale.
     */
    private void appendScale(ExtendedByteBuffer buf, NativeType type) {
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
    private void appendTypeLen(ExtendedByteBuffer buf, NativeType type) {
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
    private void appendBits(ExtendedByteBuffer buf, NativeType type) {
        assert type.spec() == NativeTypeSpec.BITMASK;

        int bits = ((BitmaskNativeType)type).bits();

        buf.putInt(bits);
    }

    /**
     * Reads column array from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Column array.
     */
    private Column[] readColumns(ExtendedByteBuffer buf) {
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
    private Column readColumn(ExtendedByteBuffer buf) {
        int schemaIdx = buf.getInt();
        boolean nullable = buf.get() == 1;
        String name = buf.getString();

        NativeType nativeType = NativeTypes.fromByteBuffer(buf);

        return new Column(name, nativeType, nullable).copy(schemaIdx);
    }
}
