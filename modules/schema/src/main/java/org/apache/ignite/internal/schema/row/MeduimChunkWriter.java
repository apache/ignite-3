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

/**
 * Row chunk writer for small key/value chunks.
 *
 * Uses {@code byte} values for coding sizes/offsets,
 * supports chunks with payload less upt to 255 bytes.
 */
class MeduimChunkWriter extends ChunkWriter {
    /**
     * Calculates vartable length (in bytes).
     *
     * @param items Vartable items.
     * @return Vartable size in bytes.
     */
    static int vartableLength(int items) {
        return items == 0 ? 0 : Short.BYTES /* Table size */ + items * Short.BYTES;
    }

    /**
     * Calculates chunk size.
     *
     * @param payloadLen Payload size in bytes.
     * @param nullMapLen Null-map size in bytes.
     * @param vartblSize Amount of vartable items.
     * @return Bytes required to write a chunk or {@code -1} if a chunk is too long.
     */
    static int chunkSize(int payloadLen, int nullMapLen, int vartblSize) {
        return Short.BYTES /* Chunk len. */ + nullMapLen + vartableLength(vartblSize) + payloadLen;
    }

    /**
     * Check if chunk fits to max size.
     *
     * @param payloadLen Payload size in bytes.
     * @param nullMapLen Null-map size in bytes.
     * @param vartblSize Amount of vartable items.
     * @return {@code true} if a chunk is tiny, {@code false} otherwise.
     */
    static boolean isMediumChunk(int payloadLen, int nullMapLen, int vartblSize) {
        return chunkSize(payloadLen, nullMapLen, vartblSize) < 64 * 1024;
    }

    /**
     * Creates chunk writer to write chunk in tiny format.
     *
     * @param buf Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapLen Null-map size in bytes.
     * @param vartblSize Amount of vartable items.
     */
    MeduimChunkWriter(ExpandableByteBuf buf, int baseOff, int nullMapOff, int varTblOff, int dataOff) {
        super(buf,baseOff, nullMapOff, varTblOff, dataOff);


        curVartblItem = 0;
    }

    /** {@inheritDoc} */
    @Override void flush() {
        final int size = chunkLength();

        assert size < (2 << 16) && size > 0 : "Size field value overflow: " + size;

        buf.putShort(baseOff, (short)size);

        if (curVartblItem > 0)
            buf.putShort(varTblOff, (short)curVartblItem);
    }

    /** {@inheritDoc} */
    @Override protected void writeOffset(int tblEntryIdx, int off) {
        final int itemOff = varTblOff + Short.BYTES + tblEntryIdx * Short.BYTES;

        assert off < (2 << 8) && off >= 0 : "Varlen offset overflow: offset=" + off;
        assert itemOff < dataOff : "Vartable overflow: size=" + itemOff;

        buf.putShort(itemOff, (short)off);
    }
}
