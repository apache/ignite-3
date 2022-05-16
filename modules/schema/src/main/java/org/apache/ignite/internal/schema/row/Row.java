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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.internal.util.HashCalculator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Schema-aware row.
 *
 * <p>The class contains non-generic methods to read boxed and unboxed primitives based on the schema column types. Any type conversions
 * and coercions should be implemented outside the row by the key-value or query runtime.
 *
 * <p>When a non-boxed primitive is read from a null column value, it is converted to the primitive type default value.
 *
 * <p>Natively supported temporal types are decoded automatically after read.
 *
 * @see TemporalTypesHelper
 */
public class Row implements BinaryRowEx, SchemaAware, InternalTuple {
    /**
     * Null map offset.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    private static final int NULLMAP_CHUNK_OFFSET = CHUNK_HEADER_SIZE;

    /** Schema descriptor. */
    protected final SchemaDescriptor schema;

    /** Binary row. */
    private final BinaryRow row;

    /** Cached key slice byte buffer. */
    private final ByteBuffer keySlice;

    /** Cached value slice byte buffer. */
    private final ByteBuffer valueSlice;

    /** Cached colocation hash value. */
    private int colocationHash;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param row    Binary row representation.
     */
    public Row(SchemaDescriptor schema, BinaryRow row) {
        this.row = row;
        this.schema = schema;
        keySlice = row.keySlice();
        valueSlice = row.valueSlice();
    }

    /**
     * Get row schema.
     */
    @Override
    @NotNull
    public SchemaDescriptor schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasValue() {
        return row.hasValue();
    }

    /** {@inheritDoc} */
    @Override
    public int count() {
        return schema.length();
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    public Object value(int col) {
        return schema.column(col).type().spec().objectValue(this, col);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.INT8, isKeyCol);

        return off < 0 ? 0 : chunk(isKeyCol).get(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public Byte byteValueBoxed(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.INT8, isKeyCol);

        return off < 0 ? null : chunk(isKeyCol).get(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.INT16, isKeyCol);

        return off < 0 ? 0 : chunk(isKeyCol).getShort(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public Short shortValueBoxed(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.INT16, isKeyCol);

        return off < 0 ? null : chunk(isKeyCol).getShort(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.INT32, isKeyCol);

        return off < 0 ? 0 : chunk(isKeyCol).getInt(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public Integer intValueBoxed(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.INT32, isKeyCol);

        return off < 0 ? null : chunk(isKeyCol).getInt(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.INT64, isKeyCol);

        return off < 0 ? 0 : chunk(isKeyCol).getLong(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public Long longValueBoxed(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.INT64, isKeyCol);

        return off < 0 ? null : chunk(isKeyCol).getLong(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.FLOAT, isKeyCol);

        return off < 0 ? 0.f : chunk(isKeyCol).getFloat(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public Float floatValueBoxed(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.FLOAT, isKeyCol);

        return off < 0 ? null : chunk(isKeyCol).getFloat(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.DOUBLE, isKeyCol);

        return off < 0 ? 0.d : chunk(isKeyCol).getDouble(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public Double doubleValueBoxed(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long off = findColumn(col, NativeTypeSpec.DOUBLE, isKeyCol);

        return off < 0 ? null : chunk(isKeyCol).getDouble(offset(off));
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal decimalValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.DECIMAL, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);
        int len = length(offLen);

        DecimalNativeType type = (DecimalNativeType) schema.column(col).type();

        byte[] bytes = readBytes(chunk(isKeyCol), off, len);

        return new BigDecimal(new BigInteger(bytes), type.scale());
    }

    /** {@inheritDoc} */
    @Override
    public BigInteger numberValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.NUMBER, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);
        int len = length(offLen);

        return new BigInteger(readBytes(chunk(isKeyCol), off, len));
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.STRING, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);
        int len = length(offLen);

        ByteBuffer chunk = chunk(isKeyCol);

        if (chunk.hasArray()) {
            return new String(chunk.array(), chunk.arrayOffset() + off, len, StandardCharsets.UTF_8);
        } else {
            return new String(readBytes(chunk, off, len), StandardCharsets.UTF_8);
        }
    }

    /** {@inheritDoc} */
    @Override
    public byte[] bytesValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.BYTES, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);
        int len = length(offLen);

        return readBytes(chunk(isKeyCol), off, len);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long found = findColumn(col, NativeTypeSpec.UUID, isKeyCol);

        if (found < 0) {
            return null;
        }

        int off = offset(found);

        ByteBuffer chunk = chunk(isKeyCol);

        long lsb = chunk.getLong(off);
        long msb = chunk.getLong(off + 8);

        return new UUID(msb, lsb);
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.BITMASK, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);
        int len = columnLength(col);

        return BitSet.valueOf(readBytes(chunk(isKeyCol), off, len));
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.DATE, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);

        return readDate(chunk(isKeyCol), off);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.TIME, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);

        TemporalNativeType type = (TemporalNativeType) schema.column(col).type();

        return readTime(chunk(isKeyCol), off, type);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime dateTimeValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.DATETIME, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);

        TemporalNativeType type = (TemporalNativeType) schema.column(col).type();

        ByteBuffer chunk = chunk(isKeyCol);

        return LocalDateTime.of(readDate(chunk, off), readTime(chunk, off + 3, type));
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int col) throws InvalidTypeException {
        boolean isKeyCol = schema.isKeyColumn(col);

        long offLen = findColumn(col, NativeTypeSpec.TIMESTAMP, isKeyCol);

        if (offLen < 0) {
            return null;
        }

        int off = offset(offLen);

        TemporalNativeType type = (TemporalNativeType) schema.column(col).type();

        ByteBuffer chunk = chunk(isKeyCol);

        long seconds = chunk.getLong(off);
        int nanos = 0;

        if (type.precision() != 0) {
            nanos = chunk.getInt(off + 8);
        }

        return Instant.ofEpochSecond(seconds, nanos);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNullValue(int col) {
        return hasNullValue(col, null);
    }

    /**
     * Checks whether the given column contains a null value.
     *
     * @param col Column index.
     * @param expectedType Column type (needed for type checking).
     * @return {@code true} if this column contains a null value, {@code false} otherwise.
     */
    public boolean hasNullValue(int col, @Nullable NativeTypeSpec expectedType) {
        boolean isKeyCol = schema.isKeyColumn(col);

        return findColumn(col, expectedType, isKeyCol) < 0;
    }

    /**
     * Reads and decode time column value.
     *
     * @param chunk Key or value chunk.
     * @param off Offset
     * @param type Temporal type precision.
     * @return LocalTime value.
     */
    private LocalTime readTime(ByteBuffer chunk, int off, TemporalNativeType type) {
        long time = Integer.toUnsignedLong(chunk.getInt(off));

        if (type.precision() > 3) {
            time <<= 16;
            time |= Short.toUnsignedLong(chunk.getShort(off + 4));
            time = (time >>> TemporalTypesHelper.NANOSECOND_PART_LEN) << 32 | (time & TemporalTypesHelper.NANOSECOND_PART_MASK);
        } else { // Decompress
            time = (time >>> TemporalTypesHelper.MILLISECOND_PART_LEN) << 32 | (time & TemporalTypesHelper.MILLISECOND_PART_MASK);
        }

        return TemporalTypesHelper.decodeTime(type, time);
    }

    /**
     * Reads and decode date column value.
     *
     * @param chunk Key or value chunk.
     * @param off Offset
     * @return LocalDate value.
     */
    private LocalDate readDate(ByteBuffer chunk, int off) {
        int date = Short.toUnsignedInt(chunk.getShort(off)) << 8;
        date |= Byte.toUnsignedInt(chunk.get(off + 2));

        return TemporalTypesHelper.decodeDate(date);
    }

    /**
     * Gets the column offset and length encoded into a single 8-byte value (4 least significant bytes encoding the offset from the
     * beginning of the row and 4 most significant bytes encoding the field length for varlength columns). The offset and length should be
     * extracted using {@link #offset(long)} and {@link #length(long)} methods. Will also validate that the actual column type matches the
     * requested column type, throwing {@link InvalidTypeException} if the types do not match.
     *
     * @param colIdx Column index.
     * @param type Expected column type.
     * @param isKeyCol {@code} if column is part of the key.
     * @return {@code -1} if value is {@code null} for a column, otherwise encoded offset + length of the column.
     * @see #offset(long)
     * @see #length(long)
     * @see SchemaDescriptor#isKeyColumn(int)
     * @see InvalidTypeException If actual column type does not match the requested column type.
     */
    protected long findColumn(int colIdx, NativeTypeSpec type, boolean isKeyCol) throws InvalidTypeException {
        Columns cols;

        ByteBuffer chunk = chunk(isKeyCol);
        int flags = chunk.get(FLAGS_FIELD_OFFSET);

        if (isKeyCol) {
            cols = schema.keyColumns();
        } else {
            // Adjust the column index according to the number of key columns.
            if (!hasValue()) {
                throw new IllegalStateException("Row has no value.");
            }

            colIdx -= schema.keyColumns().length();

            cols = schema.valueColumns();
        }

        NativeTypeSpec actualType = cols.column(colIdx).type().spec();

        if (type != null && actualType != type) {
            throw new InvalidTypeException("Invalid column type requested [requested=" + type + ", column=" + cols.column(colIdx) + ']');
        }

        int nullMapLen = cols.nullMapSize();

        VarTableFormat format = VarTableFormat.fromFlags(flags);

        if (nullMapLen > 0 && isNull(chunk, colIdx)) {
            return -1;
        }

        int dataOffset = varTableOffset(nullMapLen);

        dataOffset += format.vartableLength(format.readVartableSize(chunk, dataOffset));

        return actualType.fixedLength()
                ? fixedSizeColumnOffset(chunk, dataOffset, cols, colIdx, nullMapLen > 0) :
                varlenColumnOffsetAndLength(chunk, dataOffset, cols, colIdx, nullMapLen, format);
    }

    /**
     * Calculates the offset of the fixed-size column with the given index in the row. It essentially folds the null-map with the column
     * lengths to calculate the size of non-null columns preceding the requested column.
     *
     * @param chunk Key or value chunk.
     * @param dataOffset Chunk data offset.
     * @param cols Columns chunk.
     * @param idx Column index in the chunk.
     * @param hasNullmap {@code true} if chunk has null-map, {@code false} otherwise.
     * @return Encoded offset (from the row start) of the requested fixlen column.
     */
    int fixedSizeColumnOffset(ByteBuffer chunk, int dataOffset, Columns cols, int idx, boolean hasNullmap) {
        int colOff = 0;

        // Calculate fixlen column offset.
        int colByteIdx = idx >> 3; // Equivalent expression for: idx / 8

        // Set bits starting from posInByte, inclusive, up to either the end of the byte or the last column index, inclusive
        int startBit = idx & 7; // Equivalent expression for: idx % 8
        int endBit = (colByteIdx == (cols.length() + 7) >> 3 - 1) /* last byte */
                ? ((cols.numberOfFixsizeColumns() - 1) & 7) : 7; // Equivalent expression for: (expr) % 8
        int mask = (0xFF >> (7 - endBit)) & (0xFF << startBit);

        if (hasNullmap) {
            // Fold offset based on the whole map bytes in the schema
            for (int i = 0; i < colByteIdx; i++) {
                colOff += cols.foldFixedLength(i, Byte.toUnsignedInt(chunk.get(NULLMAP_CHUNK_OFFSET + i)));
            }

            colOff += cols.foldFixedLength(colByteIdx, Byte.toUnsignedInt(chunk.get(NULLMAP_CHUNK_OFFSET + colByteIdx)) | mask);
        } else {
            for (int i = 0; i < colByteIdx; i++) {
                colOff += cols.foldFixedLength(i, 0);
            }

            colOff += cols.foldFixedLength(colByteIdx, mask);
        }

        return dataOffset + colOff;
    }

    /**
     * Calculates the offset and length of varlen column. First, it calculates the number of non-null columns preceding the requested column
     * by folding the null-map bits. This number is used to adjust the column index and find the corresponding entry in the varlen table.
     * The length of the column is calculated either by subtracting two adjacent varlen table offsets, or by subtracting the last varlen
     * table offset from the chunk length.
     *
     * <p>Note: Offset for the very fisrt varlen is skipped in vartable and calculated from fixlen columns sizes.
     *
     * @param chunk Key or value chunk.
     * @param dataOff Chunk data offset.
     * @param cols Columns chunk.
     * @param idx Column index in the chunk.
     * @param nullMapLen Null-map length or {@code 0} if null-map is omitted.
     * @param format Vartable format helper or {@code null} if vartable is omitted.
     * @return Encoded offset (from the row start) and length of the column with the given index.
     */
    long varlenColumnOffsetAndLength(
            ByteBuffer chunk,
            int dataOff,
            Columns cols,
            int idx,
            int nullMapLen,
            VarTableFormat format
    ) {
        assert cols.hasVarlengthColumns() && cols.firstVarlengthColumn() <= idx : "Invalid varlen column index: colId=" + idx;

        if (nullMapLen > 0) { // Calculates fixlen columns chunk size regarding the 'null' flags.
            int nullStartByte = cols.firstVarlengthColumn() >> 3; // Equivalent expression for: idx / 8
            int startBitInByte = cols.firstVarlengthColumn() & 7; // Equivalent expression for: (expr) % 8

            int nullEndByte = idx >> 3; // Equivalent expression for: idx / 8
            int endBitInByte = idx & 7; // Equivalent expression for: idx % 8

            int numNullsBefore = 0;

            for (int i = nullStartByte; i <= nullEndByte; i++) {
                byte nullmapByte = chunk.get(NULLMAP_CHUNK_OFFSET + i);

                if (i == nullStartByte) { // We need to clear startBitInByte least significant bits
                    nullmapByte &= (0xFF << startBitInByte);
                }

                if (i == nullEndByte) { // We need to clear 8-endBitInByte most significant bits
                    nullmapByte &= (0xFF >> (8 - endBitInByte));
                }

                numNullsBefore += Columns.numberOfNullColumns(nullmapByte);
            }

            idx -= numNullsBefore;
        }

        idx -= cols.numberOfFixsizeColumns();

        // Calculate length and offset for very first (non-null) varlen column
        // as vartable don't store the offset for the first varlen.
        if (idx == 0) {
            int off = cols.numberOfFixsizeColumns() == 0 ? dataOff
                    : fixedSizeColumnOffset(chunk, dataOff, cols, cols.numberOfFixsizeColumns(), nullMapLen > 0);

            long len = format != VarTableFormat.SKIPPED
                    ?
                    // Length is either diff between current offset and next varlen offset or end-of-chunk.
                    dataOff + format.readVarlenOffset(chunk, varTableOffset(nullMapLen), 0) - off :
                    chunk.limit() - off;

            return (len << 32) | off;
        }

        final int varTblOff = varTableOffset(nullMapLen);
        final int vartblSize = format.readVartableSize(chunk, varTblOff);

        assert idx > 0 && vartblSize >= idx : "Vartable index is out of bound: colId=" + idx;

        // Offset of idx-th column is from base offset.
        int resOff = dataOff + format.readVarlenOffset(chunk, varTblOff, idx - 1);

        long len = (vartblSize == idx)
                ?
                // totalLength - columnStartOffset
                chunk.limit() - resOff :
                // nextColumnStartOffset - columnStartOffset
                dataOff + format.readVarlenOffset(chunk, varTblOff, idx) - resOff;

        return (len << 32) | resOff;
    }

    /**
     * Returns either key or value slice.
     */
    private ByteBuffer chunk(boolean isKeyCol) {
        return isKeyCol ? keySlice : valueSlice;
    }

    /**
     * Get vartable offset.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param nullMapLen Null-map length.
     * @return Vartable offset.
     */
    private int varTableOffset(int nullMapLen) {
        return CHUNK_HEADER_SIZE + nullMapLen;
    }

    /**
     * Checks the row's null-map for the given column index in the chunk.
     *
     * @param chunk Key or value chunk.
     * @param idx Offset of the column in the chunk.
     * @return {@code true} if the column value is {@code null}, {@code false} otherwise.
     */
    protected boolean isNull(ByteBuffer chunk, int idx) {
        int nullByte = idx >> 3; // Equivalent expression for: idx / 8
        int posInByte = idx & 7; // Equivalent expression for: idx % 8

        int map = chunk.get(CHUNK_HEADER_SIZE + nullByte) & 0xFF;

        return (map & (1 << posInByte)) != 0;
    }

    /**
     * Get column length by its index.
     *
     * @param colIdx Column index.
     * @return Column length.
     */
    private int columnLength(int colIdx) {
        Column col = schema.column(colIdx);

        return col.type().sizeInBytes();
    }

    /**
     * Utility method to extract the column offset from the {@link #findColumn} result. The offset is calculated from
     * the beginning of the row.
     *
     * @param offLen {@code findColumn} invocation result.
     * @return Column offset from the beginning of the row.
     */
    private static int offset(long offLen) {
        return (int) offLen;
    }

    /**
     * Utility method to extract the column length from the {@link #findColumn} result for varlen columns.
     *
     * @param offLen {@code findColumn} invocation result.
     * @return Length of the column or {@code 0} if the column is fixed-length.
     */
    private static int length(long offLen) {
        return (int) (offLen >>> 32);
    }

    /** {@inheritDoc} */
    @Override
    public int schemaVersion() {
        return row.schemaVersion();
    }

    /** {@inheritDoc} */
    @Override
    public int hash() {
        return row.hash();
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer keySlice() {
        return row.keySlice();
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer valueSlice() {
        return row.valueSlice();
    }

    /**
     * Writes binary row to given stream.
     *
     * @param stream Stream to write to.
     * @throws IOException If write operation fails.
     */
    @Override
    public void writeTo(OutputStream stream) throws IOException {
        row.writeTo(stream);
    }

    /**
     * Read bytes by offset.
     *
     * @param chunk Key or value chunk.
     * @param off Offset.
     * @param len Length.
     * @return Byte array.
     */
    private byte[] readBytes(ByteBuffer chunk, int off, int len) {
        try {
            byte[] res = new byte[len];

            chunk.position(off);

            chunk.get(res, 0, res.length);

            return res;
        } finally {
            chunk.position(0);
        }
    }

    /** {@inheritDoc} */
    @Override
    public byte[] bytes() {
        return row.bytes();
    }

    /** {@inheritDoc} */
    @Override
    public int colocationHash() {
        int h0 = colocationHash;

        if (h0 == 0) {
            HashCalculator hashCalc = new HashCalculator();

            for (Column c : schema().colocationColumns()) {
                ColocationUtils.append(hashCalc, value(c.schemaIndex()), c.type().spec());
            }

            colocationHash = h0 = hashCalc.hash();
        }

        return h0;
    }
}
