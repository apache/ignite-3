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
    /** First 2 bits in chunk flags. */
    public static final int FORMAT_CODE_MASK = 0x03;

    /** Flag indicates key chunk omits varlen table. */
    public static final int OMIT_NULL_MAP_FLAG = 1 << 2;

    /** Flag indicates value chunk omits null map. */
    public static final int OMIT_VARTBL_FLAG = 1 << 3;

    /** Writer factory for tiny-sized chunks. */
    private static final ChunkFormat TINY = new ChunkFormat(Byte.BYTES, Byte.BYTES, (byte)1) {
        @Override void writeOffset(ExpandableByteBuf buf, int itemOff, int off) {
            assert off < (1 << 8) && off >= 0 : "Varlen offset overflow: offset=" + off;

            buf.put(itemOff, (byte)off);
        }

        @Override int readOffset(BinaryRow row, int itemOff) {
            return row.readByte(itemOff) & 0xFF;
        }

        @Override void writeVartblSize(ExpandableByteBuf buf, int vartblOff, int size) {
            assert size < (1 << 8) && size >= 0 : "Vartable size overflow: size=" + size;

            buf.put(vartblOff, (byte)vartblOff);
        }

        @Override int readVartblSize(BinaryRow row, int vartblOff) {
            return row.readByte(vartblOff) & 0xFF;
        }
    };

    /** Writer factory for med-sized chunks. */
    private static final ChunkFormat MEDIUM = new ChunkFormat(Short.BYTES, Short.BYTES, (byte)2) {
        @Override void writeOffset(ExpandableByteBuf buf, int itemOff, int off) {
            assert off < (1 << 16) && off >= 0 : "Varlen offset overflow: offset=" + off;

            buf.putShort(itemOff, (short)off);
        }

        @Override int readOffset(BinaryRow row, int itemOff) {
            return row.readShort(itemOff) & 0xFFFF;
        }

        @Override void writeVartblSize(ExpandableByteBuf buf, int vartblOff, int size) {
            assert size < (1 << 16) && size >= 0 : "Vartable size overflow: size=" + size;

            buf.putShort(vartblOff, (short)vartblOff);
        }

        @Override int readVartblSize(BinaryRow row, int vartblOff) {
            return row.readShort(vartblOff) & 0xFFFF;
        }
    };

    /** Writer factory for large-sized chunks. */
    private static final ChunkFormat LARGE = new ChunkFormat(Short.BYTES, Integer.BYTES, (byte)0) {
        @Override void writeOffset(ExpandableByteBuf buf, int itemOff, int off) {
            buf.putInt(itemOff, off);
        }

        @Override int readOffset(BinaryRow row, int itemOff) {
            return row.readInteger(itemOff);
        }

        @Override void writeVartblSize(ExpandableByteBuf buf, int vartblOff, int size) {
            assert size < (1 << 16) && size >= 0 : "Vartable size overflow: size=" + size;

            buf.putShort(vartblOff, (short)vartblOff);
        }

        @Override int readVartblSize(BinaryRow row, int vartblOff) {
            return row.readShort(vartblOff) & 0xFFFF;
        }
    };

    /** Chunk length field size. */
    public static final int CHUNK_LEN_FLD_SIZE = Integer.BYTES;

    private final int vartableItemSize;

    private final int vartableSizeFieldLen;

    private final byte modeFlags;

    /**
     * Return chunk formatter.
     *
     * @param payloadLen Payload size in bytes.
     * @return Chunk formatter.
     */
    static ChunkFormat formatter(int payloadLen) {
        if (payloadLen < 256)
            return TINY;

        if (payloadLen < 64 * 1024)
            return MEDIUM;

        return LARGE;
    }

    /**
     * @param row Binary row.
     * @param offset Offset.
     * @param nullMapSize Default null-map size.
     * @param chunkFlags Chunk flags.
     * @return Reader.
     */
    static ChunkReader createReader(BinaryRow row, int offset, int nullMapSize, byte chunkFlags) {
        return fromFlags(chunkFlags).reader(row, offset,
            (chunkFlags & OMIT_NULL_MAP_FLAG) == 0 ? nullMapSize : 0,
            (chunkFlags & OMIT_VARTBL_FLAG) == 0);
    }

    /**
     * Chunk formatter from given flags.
     *
     * @param chunkFlags Chunk specific flags.
     * @return Chunk formatter.
     */
    private static ChunkFormat fromFlags(byte chunkFlags) {
        final int mode = chunkFlags & FORMAT_CODE_MASK;

        switch (mode) {
            case 1:
                return TINY;
            case 2:
                return MEDIUM;
            default:
                return LARGE;
        }
    }

    /**
     * @param vartableSizeFieldLen Vartalble size field length.
     * @param vartableItemSize Vartable item size.
     */
    public ChunkFormat(int vartableSizeFieldLen, int vartableItemSize, byte modeFlags) {
        this.vartableItemSize = vartableItemSize;
        this.vartableSizeFieldLen = vartableSizeFieldLen;
        this.modeFlags = modeFlags;
    }

    int vartableSizeFieldLen() {
        return vartableSizeFieldLen;
    }

    int vartableItemSize() {
        return vartableItemSize;
    }

    public byte modeFlags() {
        return modeFlags;
    }

    /**
     * @param payloadLen Row payload length in bytes.
     * @param nullMapLen Null-map length in bytes.
     * @param vartblItems Number of vartable items.
     * @return Chunk size.
     */
    int chunkSize(int payloadLen, int nullMapLen, int vartblItems) {
        return CHUNK_LEN_FLD_SIZE /* Chunk len. */ + nullMapLen + vartableLength(vartblItems) + payloadLen;
    }

    /**
     * Calculates vartable length (in bytes).
     *
     * @param items Vartable items.
     * @return Vartable size in bytes.
     */
    protected int vartableLength(int items) {
        return items == 0 ? 0 : vartableSizeFieldLen /* Table size */ + items * vartableItemSize;
    }

    /**
     * Calculates vartable item offset.
     *
     * @param idx Vartable item idx.
     * @return Vartable item offset.
     */
    int vartblItemOff(int idx) {
        return vartableSizeFieldLen /* Table size */ + idx * vartableItemSize;
    }

    abstract void writeOffset(ExpandableByteBuf buf, int vartblItemOff, int off);

    abstract int readOffset(BinaryRow row, int vartblOff);

    abstract void writeVartblSize(ExpandableByteBuf buf, int vartblOff, int size);

    abstract int readVartblSize(BinaryRow row, int vartblOff);

    /**
     * Chunk writer factory method.
     *
     * @param buf Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapLen Null-map length.
     * @param vartblItems Vartable items.
     * @return Chunk writer.
     */
    ChunkWriter writer(ExpandableByteBuf buf, int baseOff, int nullMapLen, int vartblItems) {
        return new ChunkWriter(buf, baseOff, nullMapLen, vartblItems, this);
    }

    /**
     * Chunk reader factory method.
     *
     * @param row Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapLen Null-map length.
     * @param hasVarTable Has vartable flag.
     * @return Chunk reader.
     */
    ChunkReader reader(BinaryRow row, int baseOff, int nullMapLen, boolean hasVarTable) {
        return new ChunkReader(row, baseOff, nullMapLen, hasVarTable, this);
    }
}