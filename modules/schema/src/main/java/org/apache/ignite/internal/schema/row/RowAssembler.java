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

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.AssemblyException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRow.RowFlags;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;

import static org.apache.ignite.internal.schema.BinaryRow.KEY_CHUNK_OFFSET;
import static org.apache.ignite.internal.schema.BinaryRow.KEY_HASH_FIELD_OFFSET;
import static org.apache.ignite.internal.schema.BinaryRow.RowFlags.OMIT_KEY_VARTBL_FLAG;
import static org.apache.ignite.internal.schema.BinaryRow.RowFlags.OMIT_VAL_VARTBL_FLAG;

/**
 * Utility class to build rows using column appending pattern. The external user of this class must consult
 * with the schema and provide the columns in strict internal column sort order during the row construction.
 * Additionally, the user of this class should pre-calculate the resulting row size when possible to avoid
 * unnecessary data copies and allow some size-optimizations can be applied.
 *
 * @see #utf8EncodedLength(CharSequence)
 */
public class RowAssembler {
    /** Schema. */
    private final SchemaDescriptor schema;

    /** The number of non-null varlen columns in values chunk. */
    private final int valVarlenCols;

    /** Target byte buffer to write to. */
    private final ExpandableByteBuf buf;

    /** Current columns chunk. */
    private Columns curCols;

    /** Current field index (the field is unset). */
    private int curCol;

    /** Hash. */
    private int hash;

    /** Flags. */
    private short flags;

    /** Charset encoder for strings. Initialized lazily. */
    private CharsetEncoder strEncoder;

    /** Current chunk writer. */
    private ChunkWriter chunkWriter;

    /** Value write mode. */
    private ChunkFormat valWriteMode;

    /**
     * Calculates encoded string length.
     *
     * @param seq Char sequence.
     * @return Encoded string length.
     * @implNote This implementation is not tolerant to malformed char sequences.
     */
    public static int utf8EncodedLength(CharSequence seq) {
        int cnt = 0;

        for (int i = 0, len = seq.length(); i < len; i++) {
            char ch = seq.charAt(i);

            if (ch <= 0x7F)
                cnt++;
            else if (ch <= 0x7FF)
                cnt += 2;
            else if (Character.isHighSurrogate(ch)) {
                cnt += 4;
                ++i;
            }
            else
                cnt += 3;
        }

        return cnt;
    }

    /**
     * Creates RowAssembler for chunks of unknown size.
     * <p>
     * RowAssembler will use adaptive buffer size and omit some optimizations for small key/value chunks.
     *
     * @param schema Row schema.
     * @param nonNullVarlenKeyCols Number of non-null varlen columns in key chunk.
     * @param nonNullVarlenValCols Number of non-null varlen columns in value chunk.
     */
    public RowAssembler(
        SchemaDescriptor schema,
        int nonNullVarlenKeyCols,
        int nonNullVarlenValCols
    ) {
        this(schema,
            0,
            schema.keyColumns().nullMapSize() > 0,
            nonNullVarlenKeyCols,
            0,
            schema.valueColumns().nullMapSize() > 0,
            nonNullVarlenValCols);
    }

    /**
     * Creates RowAssembler for chunks with estimated sizes.
     * <p>
     * RowAssembler will apply optimizations based on chunks sizes estimations.
     *
     * @param schema Row schema.
     * @param keyDataSize Key payload size. Estimated upper-bound or zero if unknown.
     * @param nonNullVarlenKeyCols Number of non-null varlen columns in key chunk.
     * @param valDataSize Value data size. Estimated upper-bound or zero if unknown.
     * @param nonNullVarlenValCols Number of non-null varlen columns in value chunk.
     */
    public RowAssembler(
        SchemaDescriptor schema,
        int keyDataSize,
        int nonNullVarlenKeyCols,
        int valDataSize,
        int nonNullVarlenValCols
    ) {
        this(
            schema,
            keyDataSize,
            schema.keyColumns().nullMapSize() > 0,
            nonNullVarlenKeyCols,
            valDataSize,
            schema.valueColumns().nullMapSize() > 0,
            nonNullVarlenValCols);
    }

    /**
     * Creates RowAssembler for chunks with estimated sizes.
     * <p>
     * RowAssembler will apply optimizations based on chunks sizes estimations.
     *
     * @param schema Row schema.
     * @param keyDataSize Key payload size. Estimated upper-bound or zero if unknown.
     * @param keyHasNulls Null flag. {@code True} if key has nulls values, {@code false} otherwise.
     * @param keyVarlenCols Number of non-null varlen columns in key chunk.
     * @param valDataSize Value data size. Estimated upper-bound or zero if unknown.
     * @param valHasNulls Null flag. {@code True} if value has nulls values, {@code false} otherwise.
     * @param valVarlenCols Number of non-null varlen columns in value chunk.
     */
    public RowAssembler(
        SchemaDescriptor schema,
        int keyDataSize,
        boolean keyHasNulls,
        int keyVarlenCols,
        int valDataSize,
        boolean valHasNulls,
        int valVarlenCols
    ) {
        this.schema = schema;
        this.valVarlenCols = valVarlenCols;

        curCols = schema.keyColumns();
        curCol = 0;
        flags = 0;
        strEncoder = null;

        final int keyNullMapSize = keyHasNulls ? schema.keyColumns().nullMapSize() : 0;
        final int valNullMapSize = valHasNulls ? schema.valueColumns().nullMapSize() : 0;

        final ChunkFormat keyWriteMode = ChunkFormat.writeMode(keyDataSize, keyNullMapSize, keyVarlenCols);
        valWriteMode = ChunkFormat.writeMode(valDataSize, keyNullMapSize, valVarlenCols);

        // Key flags.
        if (schema.keyColumns().nullMapSize() == 0)
            flags |= RowFlags.OMIT_KEY_NULL_MAP_FLAG;
        if (keyVarlenCols == 0)
            flags |= OMIT_KEY_VARTBL_FLAG;

        flags |= keyWriteMode.modeFlags() & 0x0F << 8;

        int size = BinaryRow.HEADER_SIZE +
            keyWriteMode.chunkSize(keyDataSize, keyNullMapSize, keyDataSize) +
            valWriteMode.chunkSize(valDataSize, valNullMapSize, valDataSize);

        buf = new ExpandableByteBuf(size);
        buf.putShort(0, (short)schema.version());

        chunkWriter = keyWriteMode.writer(buf, KEY_CHUNK_OFFSET, keyNullMapSize, keyVarlenCols);
    }

    /**
     * Appends {@code null} value for the current column to the chunk.
     */
    public void appendNull() {
        Column col = curCols.column(curCol);

        if (!col.nullable())
            throw new IllegalArgumentException("Failed to set column (null was passed, but column is not nullable): " +
                col);

        if (isKeyColumn())
            hash = 31 * hash;

        chunkWriter.setNull(curCol);

        shiftColumn();
    }

    /**
     * Appends byte value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendByte(byte val) {
        checkType(NativeTypes.BYTE);

        if (isKeyColumn())
            hash = 31 * hash + Byte.hashCode(val);

        chunkWriter.appendByte(val);

        shiftColumn();
    }

    /**
     * Appends short value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendShort(short val) {
        checkType(NativeTypes.SHORT);

        if (isKeyColumn())
            hash = 31 * hash + Short.hashCode(val);

        chunkWriter.appendShort(val);

        shiftColumn();
    }

    /**
     * Appends int value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendInt(int val) {
        checkType(NativeTypes.INTEGER);

        if (isKeyColumn())
            hash = 31 * hash + Integer.hashCode(val);

        chunkWriter.appendInt(val);

        shiftColumn();
    }

    /**
     * Appends long value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendLong(long val) {
        checkType(NativeTypes.LONG);

        if (isKeyColumn())
            hash = 31 * hash + Long.hashCode(val);

        chunkWriter.appendLong(val);

        shiftColumn();
    }

    /**
     * Appends float value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendFloat(float val) {
        checkType(NativeTypes.FLOAT);

        if (isKeyColumn())
            hash = 31 * hash + Float.hashCode(val);

        chunkWriter.appendFloat(val);

        shiftColumn();
    }

    /**
     * Appends double value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendDouble(double val) {
        checkType(NativeTypes.DOUBLE);

        if (isKeyColumn())
            hash = 31 * hash + Double.hashCode(val);

        chunkWriter.appendDouble(val);

        shiftColumn();
    }

    /**
     * Appends UUID value for the current column to the chunk.
     *
     * @param uuid Column value.
     */
    public void appendUuid(UUID uuid) {
        checkType(NativeTypes.UUID);

        if (isKeyColumn())
            hash = 31 * hash + uuid.hashCode();

        chunkWriter.appendUuid(uuid);

        shiftColumn();
    }

    /**
     * Appends String value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendString(String val) {
        checkType(NativeTypes.STRING);

        assert (flags & (schema.keyColumns() == curCols ? OMIT_KEY_VARTBL_FLAG : OMIT_VAL_VARTBL_FLAG)) == 0 :
            "Illegal writing of varlen when 'omit vartable' flag is set for a chunk.";

        if (isKeyColumn())
            hash = 31 * hash + val.hashCode();

        chunkWriter.appendString(val, encoder());

        shiftColumn();
    }

    /**
     * Appends byte[] value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendBytes(byte[] val) {
        checkType(NativeTypes.BYTES);

        assert (flags & (schema.keyColumns() == curCols ? OMIT_KEY_VARTBL_FLAG : OMIT_VAL_VARTBL_FLAG)) == 0 :
            "Illegal writing of varlen when 'omit vartable' flag is set for a chunk.";

        if (isKeyColumn())
            hash = 31 * hash + Arrays.hashCode(val);

        chunkWriter.appendBytes(val);

        shiftColumn();
    }

    /**
     * Appends BitSet value for the current column to the chunk.
     *
     * @param bitSet Column value.
     */
    public void appendBitmask(BitSet bitSet) {
        Column col = curCols.column(curCol);

        checkType(NativeTypeSpec.BITMASK);

        BitmaskNativeType maskType = (BitmaskNativeType)col.type();

        if (bitSet.length() > maskType.bits())
            throw new IllegalArgumentException("Failed to set bitmask for column '" + col.name() + "' " +
                "(mask size exceeds allocated size) [mask=" + bitSet + ", maxSize=" + maskType.bits() + "]");

        if (isKeyColumn())
            hash = 31 * hash + bitSet.hashCode();

        chunkWriter.appendBitmask(bitSet, maskType.sizeInBytes());

        shiftColumn();
    }

    /**
     * @return Serialized row.
     */
    public byte[] build() {
        if (schema.keyColumns() == curCols)
            throw new AssemblyException("Key column missed: colIdx=" + curCol);
        else {
            if (curCol == 0)
                flags |= RowFlags.NO_VALUE_FLAG;
            else if (schema.valueColumns().length() != curCol)
                throw new AssemblyException("Value column missed: colIdx=" + curCol);
        }

        buf.putShort(BinaryRow.FLAGS_FIELD_OFFSET, flags);
        buf.putInt(KEY_HASH_FIELD_OFFSET, hash);

        return buf.toArray();
    }

    private boolean isKeyColumn () {
        return curCols == schema.keyColumns();
    }

    /**
     * @return UTF-8 string encoder.
     */
    private CharsetEncoder encoder() {
        if (strEncoder == null)
            strEncoder = StandardCharsets.UTF_8.newEncoder();

        return strEncoder;
    }

    /**
     * Checks that the type being appended matches the column type.
     *
     * @param type Type spec that is attempted to be appended.
     */
    private void checkType(NativeTypeSpec type) {
        Column col = curCols.column(curCol);

        if (col.type().spec() != type)
            throw new IllegalArgumentException("Failed to set column (int was passed, but column is of different " +
                "type): " + col);
    }

    /**
     * Checks that the type being appended matches the column type.
     *
     * @param type Type that is attempted to be appended.
     */
    private void checkType(NativeType type) {
        checkType(type.spec());
    }

    /**
     * Shifts current column indexes as necessary, also
     * switch to value chunk writer when moving from key to value columns.
     */
    private void shiftColumn() {
        curCol++;

        if (curCol == curCols.length()) {
            // Write sizes.
            chunkWriter.flush();

            if (schema.valueColumns() == curCols)
                return; // No more columns.

            // Switch key->value columns.
            curCols = schema.valueColumns();
            curCol = 0;

            // Write value flags.
            if (valVarlenCols == 0)
                flags |= OMIT_VAL_VARTBL_FLAG;
            else {
                if (schema.valueColumns().nullMapSize() == 0)
                    flags |= RowFlags.OMIT_VAL_NULL_MAP_FLAG;

                flags |= valWriteMode.modeFlags() & 0x0F << 14;
            }

            // Create value chunk writer.
            chunkWriter = valWriteMode.writer(buf,
                BinaryRow.HEADER_SIZE + chunkWriter.chunkLength() /* Key chunk size */,
                schema.valueColumns().nullMapSize(),
                valVarlenCols);
        }
    }
}
