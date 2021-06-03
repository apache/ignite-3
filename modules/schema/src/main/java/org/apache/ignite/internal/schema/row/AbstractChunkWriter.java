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
abstract class AbstractChunkWriter {
    /** Chunk buffer. */
    protected final ExpandableByteBuf buf;

    /** Base offset of the chunk */
    protected final int baseOff;

    /** Offset of the null map for the chunk. */
    protected final int nullMapOff;

    /** Offset of the varlen table for the chunk. */
    protected final int varTblOff;

    /** Offset of data for the chunk. */
    protected final int dataOff;

    /** Index of the current varlen table entry. Incremented each time non-null varlen column is appended. */
    protected int curVartblItem;

    /** Current offset for the next column to be appended. */
    protected int curOff;

    /**
     * @param buf Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapOff Null-map offset.
     * @param varTblOff Vartable offset.
     */
    protected AbstractChunkWriter(ExpandableByteBuf buf, int baseOff, int nullMapOff, int varTblOff, int dataOff) {
        this.buf = buf;
        this.baseOff = baseOff;
        this.nullMapOff = nullMapOff;
        this.varTblOff = varTblOff;
        this.dataOff = dataOff;
        curOff = dataOff;
        curVartblItem = 0;
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

            writeOffset(curVartblItem, curOff - dataOff);

            curVartblItem++;
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

        writeOffset(curVartblItem, curOff - dataOff);

        curVartblItem++;
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
     * @return Chunk size in bytes.
     */
    public int chunkLength() {
        return curOff - baseOff;
    }

    /**
     * Post-write action.
     */
    abstract void flush();

    /**
     * Writes the given offset to the varlen table entry with the given index.
     *
     * @param tblEntryIdx Varlen table entry index.
     * @param off Offset to write.
     */
    protected abstract void writeOffset(int tblEntryIdx, int off);

    /**
     * Sets null flag in the null map for the given column.
     *
     * @param colIdx Column index.
     */
    protected void setNull(int colIdx) {
        int byteInMap = colIdx / 8;
        int bitInByte = colIdx % 8;

        buf.ensureCapacity(nullMapOff + byteInMap + 1);

        buf.put(nullMapOff + byteInMap, (byte)(buf.get(nullMapOff + byteInMap) | (1 << bitInByte)));
    }
}
