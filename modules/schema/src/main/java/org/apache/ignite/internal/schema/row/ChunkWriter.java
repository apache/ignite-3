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

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.AssemblyException;
import org.apache.ignite.internal.schema.NativeTypes;

/**
 * Abstract row chunk writer.
 */
class ChunkWriter {
    /** Chunk buffer. */
    protected final ExpandableByteBuf buf;

    /** Base offset of the chunk */
    protected final int baseOff;

    /** Offset of the varlen table for the chunk. */
    protected final int varTblOff;

    /** Offset of data for the chunk. */
    protected final int dataOff;

    /** Vartable format helper. */
    private final VarTableFormat format;

    /** Index of the current varlen table entry. Incremented each time non-null varlen column is appended. */
    protected int curVartblEntry;

    /** Current offset for the next column to be appended. */
    protected int curOff;

    /** Chunk flags. */
    private byte flags;

    /**
     * @param buf Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapLen Null-map length in bytes.
     * @param vartblLen Vartable length in bytes.
     * @param format Vartable format helper.
     */
    protected ChunkWriter(ExpandableByteBuf buf, int baseOff, int nullMapLen, int vartblLen, VarTableFormat format) {
        this.buf = buf;
        this.baseOff = baseOff;
        this.format = format;

        flags = format.formatFlags();
        varTblOff = nullmapOff() + nullMapLen;
        dataOff = varTblOff + vartblLen;
        curOff = dataOff;
        curVartblEntry = 0;

        if (nullMapLen == 0)
            flags |= VarTableFormat.OMIT_NULL_MAP_FLAG;

        if (vartblLen == 0)
            flags |= VarTableFormat.OMIT_VARTBL_FLAG;
    }

    /**
     * Appends byte value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendByte(byte val) {
        buf.put(curOff, val);

        curOff += NativeTypes.BYTE.sizeInBytes();
    }

    /**
     * Appends short value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendShort(short val) {
        buf.putShort(curOff, val);

        curOff += NativeTypes.SHORT.sizeInBytes();
    }

    /**
     * Appends int value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendInt(int val) {
        buf.putInt(curOff, val);

        curOff += NativeTypes.INTEGER.sizeInBytes();
    }

    /**
     * Appends long value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendLong(long val) {
        buf.putLong(curOff, val);

        curOff += NativeTypes.LONG.sizeInBytes();
    }

    /**
     * Appends float value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendFloat(float val) {
        buf.putFloat(curOff, val);

        curOff += NativeTypes.FLOAT.sizeInBytes();
    }

    /**
     * Appends double value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendDouble(double val) {
        buf.putDouble(curOff, val);

        curOff += NativeTypes.DOUBLE.sizeInBytes();
    }

    /**
     * Appends UUID value for the current column to the chunk.
     *
     * @param uuid Column value.
     */
    public void appendUuid(UUID uuid) {
        buf.putLong(curOff, uuid.getLeastSignificantBits());
        buf.putLong(curOff + 8, uuid.getMostSignificantBits());

        curOff += NativeTypes.UUID.sizeInBytes();
    }

    /**
     * Appends String value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendString(String val, CharsetEncoder encoder) {
        try {
            int written = buf.putString(curOff, val, encoder);

            writeVarlenOffset(curVartblEntry, curOff - dataOff);

            curVartblEntry++;
            curOff += written;
        }
        catch (CharacterCodingException e) {
            throw new AssemblyException("Failed to encode string", e);
        }
    }

    /**
     * Appends byte[] value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendBytes(byte[] val) {
        buf.putBytes(curOff, val);

        writeVarlenOffset(curVartblEntry, curOff - dataOff);

        curVartblEntry++;
        curOff += val.length;
    }

    /**
     * Appends BitSet value for the current column to the chunk.
     *
     * @param bitSet Column value.
     */
    public void appendBitmask(BitSet bitSet, int size) {
        byte[] arr = bitSet.toByteArray();

        buf.putBytes(curOff, arr);

        for (int i = 0; i < size - arr.length; i++)
            buf.put(curOff + arr.length + i, (byte)0);

        curOff += size;
    }

    /**
     * @return Null-map offset.
     */
    private int nullmapOff() {
        return baseOff + VarTableFormat.CHUNK_LEN_FLD_SIZE;
    }

    /**
     * @return Chunk flags.
     */
    public short chunkFlags() {
        return flags;
    }

    /**
     * @return Chunk size in bytes.
     */
    public int chunkLength() {
        return curOff - baseOff;
    }

    /**
     * Post-write action.
     */
    void flush() {
        buf.putInt(baseOff, chunkLength());

        if (curVartblEntry > 1) {
            assert varTblOff + format.vartableLength(curVartblEntry - 1) == dataOff : "Vartable overlow: size=" + curVartblEntry;

            format.writeVartableSize(buf, varTblOff, curVartblEntry - 1);
        }
    }

    /**
     * Writes the given offset to the varlen table entry with the given index.
     *
     * @param entryIdx Vartable entry index.
     * @param off Offset to write.
     */
    protected void writeVarlenOffset(int entryIdx, int off) {
        if (entryIdx == 0)
            return; // Omit offset for very first varlen.

        assert (flags & VarTableFormat.OMIT_VARTBL_FLAG) == 0 :
            "Illegal writing of varlen when 'omit vartable' flag is set for a chunk.";

        format.writeVarlenOffset(buf, varTblOff, entryIdx - 1, off);
    }

    /**
     * Sets null flag in the null-map for the given column.
     *
     * @param colIdx Column index.
     */
    protected void setNull(int colIdx) {
        assert (flags & VarTableFormat.OMIT_NULL_MAP_FLAG) == 0 : "Null-map is omitted.";

        int byteInMap = colIdx / 8;
        int bitInByte = colIdx % 8;

        buf.ensureCapacity(nullmapOff() + byteInMap + 1);

        buf.put(nullmapOff() + byteInMap, (byte)(buf.get(nullmapOff() + byteInMap) | (1 << bitInByte)));
    }
}
