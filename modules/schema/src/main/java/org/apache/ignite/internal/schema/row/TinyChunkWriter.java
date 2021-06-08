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
 * <p>
 * Uses {@code byte} values for coding sizes/offsets,
 * supports chunks with payload less upt to 255 bytes.
 */
class TinyChunkWriter extends ChunkWriter {
    /**
     * Creates chunk writer to write chunk in tiny format.
     *
     * @param buf Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapOff Null-map offset.
     * @param varTblOff Vartable offset.
     */
    TinyChunkWriter(ExpandableByteBuf buf, int baseOff, int nullMapOff, int varTblOff, int dataOff) {
        super(buf,baseOff, nullMapOff, varTblOff, dataOff);

        curVartblItem = 0;
    }

    /** {@inheritDoc} */
    @Override void flush() {
        final int size = chunkLength();

        assert size < (2 << 8) && size > 0 : "Size field value overflow: " + size;

        buf.put(baseOff, (byte)size);

        if (curVartblItem > 0)
            buf.put(varTblOff, (byte)curVartblItem);
    }

    /** {@inheritDoc} */
    @Override protected void writeOffset(int tblEntryIdx, int off) {
        final int itemOff = varTblOff + Byte.BYTES + tblEntryIdx * Byte.BYTES;

        assert off < (2 << 8) && off >= 0 : "Varlen offset overflow: offset=" + off;
        assert itemOff < dataOff : "Vartable overflow: size=" + itemOff;

        buf.put(itemOff, (byte)off);
    }
}
