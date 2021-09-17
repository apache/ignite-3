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
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 *
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
        return Size.BYTE +                      //Assembler version
            Size.INT +                          //Descriptor version
            getColumnsSize(desc.keyColumns()) +
            getColumnsSize(desc.valueColumns()) +
            Size.ARRAY_HEADER_LENGTH +          //Affinity columns length
            getStringArraySize(desc.affinityColumns());
    }

    /**
     * @param cols
     * @return
     */
    private int getStringArraySize(Column[] cols) {
        int size = Size.ARRAY_HEADER_LENGTH;      //String array size header
        for (Column column : cols)
            size += getStringSize(column.name());

        return size;
    }

    /**
     * @param cols
     * @return
     */
    private int getColumnsSize(Columns cols) {
        int size = Size.ARRAY_HEADER_LENGTH; //cols array length

        for (Column column : cols.columns())
            size += Size.INT +                      //Schema index
                Size.BOOL +                         //nullable flag
                getStringSize(column.name()) +
                Size.INT +                          //type size in bytes
                getStringSize(column.type().spec().name()) +
                Size.INT +                          //type precision (for DECIMAL AND NUMBER types)
                Size.INT;                           //type scale (for DECIMAL type)

        return size;
    }

    /**
     * @param str
     * @return
     */
    private int getStringSize(String str) {
        return Size.STRING_HEADER + //string byte array header
            str.getBytes().length; // string byte array length
    }

    /**
     * @param cols
     * @param buf
     */
    private void appendColumns(Columns cols, ByteBuffer buf) {
        Column[] colArr = cols.columns();

        buf.putInt(colArr.length);

        for (Column column : colArr)
            appendColumn(column, buf);
    }

    /**
     * @param col
     * @param buf
     */
    private void appendColumn(Column col, ByteBuffer buf) {
        buf.putInt(col.schemaIndex());
        buf.put((byte)(col.nullable() ? 1 : 0));
        appendString(col.name(), buf);
        NativeType type = col.type();
        buf.putInt(type.sizeInBytes());

        appendPrecisionAndScale(buf, type);

        appendString(type.spec().name(), buf);
    }

    /**
     * @param buf
     * @param type
     */
    private void appendPrecisionAndScale(ByteBuffer buf, NativeType type) {
        int precision = 0;
        int scale = 0;
        if (type.spec() == NativeTypeSpec.DECIMAL) {
            precision = ((DecimalNativeType)type).precision();
            scale = ((DecimalNativeType)type).scale();
        } else if ((type.spec() == NativeTypeSpec.NUMBER))
            precision = ((NumberNativeType)type).precision();

        buf.putInt(precision);
        buf.putInt(scale);
    }

    private void appendString(String name, ByteBuffer buf) {
        byte[] bytes = name.getBytes();

        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    /**
     * @param buf
     * @return
     */
    private Column[] readColumns(ByteBuffer buf) {
        int size = buf.getInt();

        Column[] colArr = new Column[size];

        for (int i = 0; i < size; i++)
            colArr[i] = readColumn(buf);

        return colArr;
    }

    /**
     * @param buf
     * @return
     */
    private Column readColumn(ByteBuffer buf) {
        int schemaIdx = buf.getInt();
        boolean nullable = buf.get() == 1;
        String name = readString(buf);
        int sizeInBytes = buf.getInt();
        int precision = buf.getInt();
        int scale = buf.getInt();
        String nativeTypeSpecName = readString(buf);

        NativeType nativeType = NativeTypes.fromName(nativeTypeSpecName, sizeInBytes, precision, scale);

        return new Column(name, nativeType, nullable).copy(schemaIdx);
    }

    /**
     * @param buf
     * @return
     */
    private String readString(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] arr = new byte[len];

        for (int i = 0; i < len; i++)
            arr[i] = buf.get();

        return new String(arr);
    }
}
