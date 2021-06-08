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

package org.apache.ignite.internal.schema.row;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.jetbrains.annotations.NotNull;

/**
 * Schema-aware row.
 * <p>
 * The class contains non-generic methods to read boxed and unboxed primitives based on the schema column types.
 * Any type conversions and coercions should be implemented outside the row by the key-value or query runtime.
 * When a non-boxed primitive is read from a null column value, it is converted to the primitive type default value.
 */
public class Row implements BinaryRow {
    /** Schema descriptor. */
    private final SchemaDescriptor schema;

    /** Binary row. */
    private final BinaryRow row;

    /** Key reader. */
    private final ChunkReader keyReader;

    /** Value reader. */
    private final ChunkReader valReader;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param row Binary row representation.
     */
    public Row(SchemaDescriptor schema, BinaryRow row) {
        assert row.schemaVersion() == schema.version();

        this.row = row;
        this.schema = schema;

        final short flags = readShort(FLAGS_FIELD_OFFSET);

        keyReader = createReader(KEY_CHUNK_OFFSET,
            (flags & RowFlags.KEY_TYNY_FORMAT) != 0,
            (flags & RowFlags.OMIT_KEY_NULL_MAP_FLAG) == 0 ? schema.keyColumns().nullMapSize() : 0,
            (flags & RowFlags.OMIT_KEY_VARTBL_FLAG) == 0);

        valReader = ((flags & RowFlags.NO_VALUE_FLAG) == 0) ?
            createReader(
                KEY_CHUNK_OFFSET + keyReader.chunkLength(),
                (flags & RowFlags.VAL_TYNY_FORMAT) != 0,
                (flags & RowFlags.OMIT_VAL_NULL_MAP_FLAG) == 0 ? schema.valueColumns().nullMapSize() : 0,
                (flags & RowFlags.OMIT_VAL_VARTBL_FLAG) == 0) :
            null;
    }


    /**
     * @return Row schema.
     */
    public SchemaDescriptor rowSchema() {
        return schema;
    }

    /**
     * @return {@code True} if row has non-null value, {@code false} otherwise.
     */
    @Override public boolean hasValue() {
        return row.hasValue();
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public byte byteValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.BYTE);

        return off < 0 ? 0 : readByte(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Byte byteValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.BYTE);

        return off < 0 ? null : readByte(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public short shortValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.SHORT);

        return off < 0 ? 0 : readShort(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Short shortValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.SHORT);

        return off < 0 ? null : readShort(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public int intValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.INTEGER);

        return off < 0 ? 0 : readInteger(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Integer intValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.INTEGER);

        return off < 0 ? null : readInteger(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public long longValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.LONG);

        return off < 0 ? 0 : readLong(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Long longValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.LONG);

        return off < 0 ? null : readLong(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public float floatValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.FLOAT);

        return off < 0 ? 0.f : readFloat(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Float floatValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.FLOAT);

        return off < 0 ? null : readFloat(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public double doubleValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.DOUBLE);

        return off < 0 ? 0.d : readDouble(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Double doubleValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.DOUBLE);

        return off < 0 ? null : readDouble(offset(off));
    }

    /**
     * Reads value from specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public BigDecimal decimalValue(int col) throws InvalidTypeException {
        // TODO: IGNITE-13668 decimal support
        return null;
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public String stringValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.STRING);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = length(offLen);

        return readString(off, len);
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public byte[] bytesValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.BYTES);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = length(offLen);

        return readBytes(off, len);
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public UUID uuidValue(int col) throws InvalidTypeException {
        long found = findColumn(col, NativeTypeSpec.UUID);

        if (found < 0)
            return null;

        int off = offset(found);

        long lsb = readLong(off);
        long msb = readLong(off + 8);

        return new UUID(msb, lsb);
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public BitSet bitmaskValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.BITMASK);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = columnLength(col);

        return BitSet.valueOf(readBytes(off, len));
    }

    /**
     * @return Row flags.
     */
    private boolean hasFlag(int flag) {
        return ((readShort(FLAGS_FIELD_OFFSET) & flag)) != 0;
    }

    /**
     * Gets the column offset and length encoded into a single 8-byte value (4 least significant bytes encoding the
     * offset from the beginning of the row and 4 most significant bytes encoding the field length for varlength
     * columns). The offset and length should be extracted using {@link #offset(long)} and {@link #length(long)}
     * methods.
     * Will also validate that the actual column type matches the requested column type, throwing
     * {@link InvalidTypeException} if the types do not match.
     *
     * @param colIdx Column index.
     * @param type Expected column type.
     * @return Encoded offset + length of the column.
     * @see #offset(long)
     * @see #length(long)
     * @see InvalidTypeException If actual column type does not match the requested column type.
     */
    protected long findColumn(int colIdx, NativeTypeSpec type) throws InvalidTypeException {
        // Get base offset (key start or value start) for the given column.
        boolean isKeyCol = schema.isKeyColumn(colIdx);

        // Adjust the column index according to the number of key columns.
        if (!isKeyCol)
            colIdx -= schema.keyColumns().length();

        ChunkReader reader = isKeyCol ? keyReader : valReader;
        Columns cols = isKeyCol ? schema.keyColumns() : schema.valueColumns();

        if (cols.column(colIdx).type().spec() != type)
            throw new InvalidTypeException("Invalid column type requested [requested=" + type +
                ", column=" + cols.column(colIdx) + ']');

        assert reader != null;

        if (reader.isNull(colIdx))
            return -1;

        assert reader.hasVartable() || type.fixedLength();

        return type.fixedLength() ?
            reader.fixlenColumnOffset(cols, colIdx) :
            reader.varlenColumnOffsetAndLength(cols, colIdx);
    }

    /**
     * @param colIdx Column index.
     * @return Column length.
     */
    private int columnLength(int colIdx) {
        Column col = schema.column(colIdx);

        return col.type().sizeInBytes();
    }

    /**
     * Utility method to extract the column offset from the {@link #findColumn(int, NativeTypeSpec)} result. The
     * offset is calculated from the beginning of the row.
     *
     * @param offLen {@code findColumn} invocation result.
     * @return Column offset from the beginning of the row.
     */
    private static int offset(long offLen) {
        return (int)offLen;
    }

    /**
     * Utility method to extract the column length from the {@link #findColumn(int, NativeTypeSpec)} result for
     * varlen columns.
     *
     * @param offLen {@code findColumn} invocation result.
     * @return Length of the column or {@code 0} if the column is fixed-length.
     */
    private static int length(long offLen) {
        return (int)(offLen >>> 32);
    }

    /** {@inheritDoc} */
    @Override public int schemaVersion() {
        return row.schemaVersion();
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return row.hash();
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer keySlice() {
        return row.keySlice();
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer valueSlice() {
        return row.valueSlice();
    }

    /** {@inheritDoc} */
    @Override public void writeTo(OutputStream stream) throws IOException {
        row.writeTo(stream);
    }

    /** {@inheritDoc} */
    @Override public byte readByte(int off) {
        return row.readByte(off);
    }

    /** {@inheritDoc} */
    @Override public short readShort(int off) {
        return row.readShort(off);
    }

    /** {@inheritDoc} */
    @Override public int readInteger(int off) {
        return row.readInteger(off);
    }

    /** {@inheritDoc} */
    @Override public long readLong(int off) {
        return row.readLong(off);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(int off) {
        return row.readFloat(off);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(int off) {
        return row.readDouble(off);
    }

    /** {@inheritDoc} */
    @Override public String readString(int off, int len) {
        return row.readString(off, len);
    }

    /** {@inheritDoc} */
    @Override public byte[] readBytes(int off, int len) {
        return row.readBytes(off, len);
    }

}
