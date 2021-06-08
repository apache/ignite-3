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

import org.apache.ignite.internal.schema.BinaryRow;

/**
 * Chunk writers factory.
 */
abstract class ChunkFormat {
    /** Writer factory for tiny-sized chunks. */
    private static final ChunkFormat TINY = new TinyChunkFormat();

    /** Writer factory for med-sized chunks. */
    private static final ChunkFormat MEDIUM = new MedChunkFormat();

    /** Writer factory for large-sized chunks. */
    private static final ChunkFormat LARGE = new LargeChunkFormat();

    /**
     * Check if chunk fits to max size.
     *
     * @param payloadLen Payload size in bytes.
     * @param nullMapLen Null-map size in bytes.
     * @param vartblSize Amount of vartable items.
     * @return {@code true} if a chunk is tiny, {@code false} otherwise.
     */
    static ChunkFormat writeMode(int payloadLen, int nullMapLen, int vartblSize) {
        if (TINY.chunkSize(payloadLen, nullMapLen, vartblSize) < 256)
            return TINY;

        if (MEDIUM.chunkSize(payloadLen, nullMapLen, vartblSize) < 64 * 1024)
            return MEDIUM;

        return LARGE;
    }

    /**
     * @param payloadLen Row payload length in bytes.
     * @param nullMapLen Null-map length in bytes.
     * @param vartblItems Number of vartable items.
     * @return Chunk size.
     */
    abstract int chunkSize(int payloadLen, int nullMapLen, int vartblItems);

    /**
     * Returns mode flags. First 4-bits are used.
     *
     * @return Chunk specific flags.
     */
    byte modeFlags() {
        return 0;
    }

    /**
     * Calculates vartable length (in bytes).
     *
     * @param items Vartable items.
     * @return Vartable size in bytes.
     */
    protected abstract int vartableLength(int items);

    /**
     * Chunk writer factory method.
     *
     * @param buf Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapLen Null-map length.
     * @param vartblSize Vartable length.
     * @return Chunk writer.
     */
    abstract ChunkWriter writer(ExpandableByteBuf buf, int baseOff, int nullMapLen, int vartblSize);

    abstract ChunkReader reader(BinaryRow row, int baseOff, int nullMapLen, boolean hasVarTable);

    /**
     * Writer factory for tiny-sized chunks.
     */
    private static class TinyChunkFormat extends ChunkFormat {

        /** {@inheritDoc} */
        @Override protected int vartableLength(int items) {
            return items == 0 ? 0 : Byte.BYTES /* Table size */ + items * Byte.BYTES;
        }

        /** {@inheritDoc} */
        @Override int chunkSize(int payloadLen, int nullMapLen, int vartblItems) {
            return Byte.BYTES /* Chunk len. */ + nullMapLen + vartableLength(vartblItems) + payloadLen;
        }

        /** {@inheritDoc} */
        @Override byte modeFlags() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override ChunkWriter writer(ExpandableByteBuf buf, int baseOff, int nullMapLen, int vartblSize) {
            return new TinyChunkWriter(buf,
                baseOff,
                baseOff + Byte.BYTES /* Chunk size */,
                baseOff + Byte.BYTES + nullMapLen,
                baseOff + Byte.BYTES + nullMapLen + vartableLength(vartblSize));
        }

        /** {@inheritDoc} */
        @Override ChunkReader reader(BinaryRow row,int baseOff, int nullMapLen, boolean hasVarTable) {
            return new TinyChunkReader(row, baseOff,  nullMapLen, hasVarTable);
        }
    }

    /**
     * Writer factory for med-size chunks.
     */
    private static class MedChunkFormat extends ChunkFormat {
        /** {@inheritDoc} */
        @Override protected int vartableLength(int items) {
            return items == 0 ? 0 : Short.BYTES /* Table size */ + items * Short.BYTES;
        }

        /** {@inheritDoc} */
        @Override int chunkSize(int payloadLen, int nullMapLen, int vartblItems) {
            return Short.BYTES /* Chunk len. */ + nullMapLen + vartableLength(vartblItems) + payloadLen;
        }

        /** {@inheritDoc} */
        @Override byte modeFlags() {
            return 2;
        }

        /** {@inheritDoc} */
        @Override ChunkWriter writer(ExpandableByteBuf buf, int baseOff, int nullMapLen, int vartblSize) {
            return new MeduimChunkWriter(buf,
                baseOff,
                baseOff + Short.BYTES /* Chunk size */,
                baseOff + Short.BYTES + nullMapLen,
                baseOff + Short.BYTES + nullMapLen + vartableLength(vartblSize));
        }

        /** {@inheritDoc} */
        @Override ChunkReader reader(BinaryRow row,int baseOff, int nullMapLen, boolean hasVarTable) {
            return new MediumChunkReader(row, baseOff,  nullMapLen, hasVarTable);
        }
    }

    /**
     * Writer factory for large-sized chunks.
     */
    private static class LargeChunkFormat extends ChunkFormat {
        /** {@inheritDoc} */
        @Override protected int vartableLength(int items) {
            return items == 0 ? 0 : Integer.BYTES /* Table size */ + items * Integer.BYTES;
        }

        /** {@inheritDoc} */
        @Override int chunkSize(int payloadLen, int nullMapLen, int vartblItems) {
            return Integer.BYTES /* Chunk len. */ + nullMapLen + vartableLength(vartblItems) + payloadLen;
        }

        /** {@inheritDoc} */
        @Override ChunkWriter writer(ExpandableByteBuf buf, int baseOff, int nullMapLen, int vartblSize) {
            return new TinyChunkWriter(buf,
                baseOff,
                baseOff + Integer.BYTES /* Chunk size */,
                baseOff + Integer.BYTES + nullMapLen,
                baseOff + Integer.BYTES + nullMapLen + vartableLength(vartblSize));
        }
        /** {@inheritDoc} */
        @Override ChunkReader reader(BinaryRow row,int baseOff, int nullMapLen, boolean hasVarTable) {
            return new LargeChunkReader(row, baseOff,  nullMapLen, hasVarTable);
        }
    }

    /**
     * Tiny chunk format reader.
     */
    private static class TinyChunkReader extends ChunkReader {
        /**
         * @param row Row.
         * @param baseOff Base offset.
         * @param nullMapLen Null-map length in bytes.
         * @param hasVarTable Vartable presence flag.
         */
        TinyChunkReader(BinaryRow row, int baseOff, int nullMapLen, boolean hasVarTable) {
            super(row, baseOff);

            nullMapOff = baseOff + Byte.BYTES;
            varTableOff = nullMapOff + nullMapLen;
            dataOff = varTableOff + (hasVarTable ? Byte.BYTES + (row.readByte(varTableOff) & 0xFF) * Byte.BYTES : 0);
        }

        /** {@inheritDoc} */
        @Override int chunkLength() {
            return row.readByte(baseOff) & 0xFF;
        }

        /** {@inheritDoc} */
        @Override int vartableItems() {
            return hasVartable() ? (row.readByte(varTableOff) & 0xFF) : 0;
        }

        /** {@inheritDoc} */
        @Override protected int varlenItemOffset(int itemIdx) {
            return dataOff + (row.readByte(varTableOff + Byte.BYTES + itemIdx * Byte.BYTES) & 0xFF);
        }
    }

    /**
     * Medium chunk format reader.
     */
    private static class MediumChunkReader extends ChunkReader {
        /**
         * @param row Row.
         * @param baseOff Base offset.
         * @param nullMapLen Null-map length in bytes.
         * @param hasVarTable Vartable presence flag.
         */
        MediumChunkReader(BinaryRow row, int baseOff, int nullMapLen, boolean hasVarTable) {
            super(row, baseOff);

            nullMapOff = baseOff + Short.BYTES;
            varTableOff = nullMapOff + nullMapLen;
            dataOff = varTableOff + (hasVarTable ? Short.BYTES + (row.readShort(varTableOff) & 0xFFFF) * Short.BYTES : 0);
        }

        /** {@inheritDoc} */
        @Override int chunkLength() {
            return row.readShort(baseOff) & 0xFF;
        }

        /** {@inheritDoc} */
        @Override int vartableItems() {
            return hasVartable() ? (row.readShort(varTableOff) & 0xFFFF) : 0;
        }

        /** {@inheritDoc} */
        @Override protected int varlenItemOffset(int itemIdx) {
            return dataOff + (row.readShort(varTableOff + Short.BYTES + itemIdx * Short.BYTES) & 0xFFFF);
        }
    }

    /**
     * Large chunk format reader.
     */
    private static class LargeChunkReader extends ChunkReader {
        /**
         * @param row Row.
         * @param baseOff Base offset.
         * @param nullMapLen Null-map length in bytes.
         * @param hasVarTable Vartable presence flag.
         */
        LargeChunkReader(BinaryRow row, int baseOff, int nullMapLen, boolean hasVarTable) {
            super(row, baseOff);

            nullMapOff = baseOff + Integer.BYTES;
            varTableOff = baseOff + Integer.BYTES + nullMapLen;
            dataOff = varTableOff + (hasVarTable ? Integer.BYTES + row.readInteger(varTableOff) * Integer.BYTES : 0);
        }

        /** {@inheritDoc} */
        @Override public int chunkLength() {
            return row.readInteger(baseOff);
        }

        /** {@inheritDoc} */
        @Override int vartableItems() {
            return hasVartable() ? row.readInteger(varTableOff) : 0;
        }

        /** {@inheritDoc} */
        @Override protected int varlenItemOffset(int itemIdx) {
            return dataOff + row.readInteger(varTableOff + Integer.BYTES + itemIdx * Integer.BYTES);
        }
    }
}
