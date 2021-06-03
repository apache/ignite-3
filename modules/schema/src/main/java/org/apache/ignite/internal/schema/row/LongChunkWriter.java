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
 * Row chunk writer for long key/value chunks.
 *
 * Uses {@code int} values for coding sizes/offsets,
 * supports chunks with payload up to 2 GiB.
 */
class LongChunkWriter extends AbstractChunkWriter {
    /**
     * Calculates vartable length (in bytes).
     *
     * @param items Vartable items.
     * @return Vartable size in bytes.
     */
    static int vartableLength(int items) {
        return items == 0 ? 0 : Integer.BYTES /* Table size */ + items * Integer.BYTES;
    }

    /**
     * Creates chunk writer for long chunk format.
     *
     * @param buf Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapLen Null-map size in bytes.
     * @param vartblSize Amount of vartable items.
     */
    LongChunkWriter(ExpandableByteBuf buf, int baseOff, int nullMapLen, int vartblSize) {
        super(
            buf,
            baseOff,
            baseOff + Integer.BYTES /* Chunk size */,
            baseOff + Integer.BYTES /* Chunk size */ + nullMapLen,
            baseOff + Integer.BYTES /* Chunk size */ + nullMapLen + vartableLength(vartblSize));

        curVartblItem = 0;
    }

    /** {@inheritDoc} */
    @Override void flush() {
        final int size = chunkLength();

        assert size > 0 : "Size field value overflow: " + size;
        assert varTblOff + vartableLength(curVartblItem) == dataOff : "Vartable underflowed.";

        buf.putInt(baseOff, size);

        if (curVartblItem > 0)
            buf.putInt(varTblOff, curVartblItem);
    }

    /** {@inheritDoc} */
    @Override protected void writeOffset(int tblEntryIdx, int off) {
        final int itemOff = varTblOff + Integer.BYTES + tblEntryIdx * Integer.BYTES;

        assert off >= 0 : "Varlen offset overflow: offset=" + off;
        assert itemOff < dataOff : "Vartable overflow: size=" + itemOff;

        buf.putInt(itemOff, off);
    }
}
