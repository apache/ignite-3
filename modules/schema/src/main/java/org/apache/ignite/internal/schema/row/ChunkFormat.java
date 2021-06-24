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
 * Chunk format.
 */
abstract class ChunkFormat {
    /** First two flag bits reserved for format code. */
    public static final int FORMAT_CODE_MASK = 0x03;

    /** Flag indicates key chunk omits vartable. */
    public static final int OMIT_NULL_MAP_FLAG = 1 << 2;

    /** Flag indicates value chunk omits null map. */
    public static final int OMIT_VARTBL_FLAG = 1 << 3;

    /** Writer factory for tiny-sized chunks. */
    private static final ChunkFormat TINY = new ChunkFormat(Byte.BYTES, Byte.BYTES, (byte)1) {
        /** {@inheritDoc} */
        @Override void writeVarlenOffset(ExpandableByteBuf buf, int vartblOff, int entryIdx, int off) {
            assert off < (1 << 8) && off >= 0 : "Varlen offset overflow: offset=" + off;

            buf.put(vartblOff + vartableEntryOffset(entryIdx), (byte)off);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return row.readByte(vartblOff + vartableEntryOffset(entryIdx)) & 0xFF;
        }

        /** {@inheritDoc} */
        @Override void writeVartableSize(ExpandableByteBuf buf, int vartblOff, int size) {
            assert size < (1 << 8) && size >= 0 : "Vartable size overflow: size=" + size;

            buf.put(vartblOff, (byte)size);
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return row.readByte(vartblOff) & 0xFF;
        }
    };

    /** Writer factory for med-sized chunks. */
    private static final ChunkFormat MEDIUM = new ChunkFormat(Short.BYTES, Short.BYTES, (byte)2) {
        /** {@inheritDoc} */
        @Override void writeVarlenOffset(ExpandableByteBuf buf, int vartblOff, int entryIdx, int off) {
            assert off < (1 << 16) && off >= 0 : "Varlen offset overflow: offset=" + off;

            buf.putShort(vartblOff + vartableEntryOffset(entryIdx), (short)off);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return row.readShort(vartblOff + vartableEntryOffset(entryIdx)) & 0xFFFF;
        }

        /** {@inheritDoc} */
        @Override void writeVartableSize(ExpandableByteBuf buf, int vartblOff, int size) {
            assert size < (1 << 16) && size >= 0 : "Vartable size overflow: size=" + size;

            buf.putShort(vartblOff, (short)size);
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return row.readShort(vartblOff) & 0xFFFF;
        }
    };

    /** Writer factory for large-sized chunks. */
    private static final ChunkFormat LARGE = new ChunkFormat(Short.BYTES, Integer.BYTES, (byte)0) {
        /** {@inheritDoc} */
        @Override void writeVarlenOffset(ExpandableByteBuf buf, int vartblOff, int entryIdx, int off) {
            buf.putInt(vartblOff + vartableEntryOffset(entryIdx), off);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return row.readInteger(vartblOff + vartableEntryOffset(entryIdx));
        }

        /** {@inheritDoc} */
        @Override void writeVartableSize(ExpandableByteBuf buf, int vartblOff, int size) {
            assert size < (1 << 16) && size >= 0 : "Vartable size overflow: size=" + size;

            buf.putShort(vartblOff, (short)size);
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return row.readShort(vartblOff) & 0xFFFF;
        }
    };

    /**
     * Return chunk formatter.
     *
     * @param payloadLen Payload size in bytes.
     * @return Chunk formatter.
     */
    static ChunkFormat formatter(int payloadLen) {
        if (payloadLen > 0) {
            if (payloadLen < 256)
                return TINY;

            if (payloadLen < 64 * 1024)
                return MEDIUM;
        }

        return LARGE;
    }

    /**
     * Creates chunk reader.
     *
     * @param row Binary row.
     * @param offset Chunk offset.
     * @param nullMapSize Default chunk null-map size.
     * @param chunkFlags Chunk flags. First 4-bits are meaningful.
     * @return Chunk reader.
     */
    static ChunkReader createReader(BinaryRow row, int offset, int nullMapSize, byte chunkFlags) {
        return fromFlags(chunkFlags).reader(row, offset, nullMapSize, chunkFlags);
    }

    /**
     * Chunk format factory method.
     *
     * @param chunkFlags Chunk specific flags. Only first 4-bits are meaningful.
     * @return Chunk formatter regarding the provided flags.
     */
    private static ChunkFormat fromFlags(byte chunkFlags) {
        switch (chunkFlags & FORMAT_CODE_MASK) {
            case 1:
                return TINY;
            case 2:
                return MEDIUM;
            default:
                return LARGE;
        }
    }

    /** Size of chunk length field. */
    public static final int CHUNK_LEN_FLD_SIZE = Integer.BYTES;

    /** Size of vartable entry. */
    private final int vartblEntrySize;

    /** Size of cartable size field. */
    private final int vartblSizeFieldSize;

    /** Format flags. */
    private final byte flags;

    /**
     * @param vartblSizeFieldSize Size of vartalble size field (in bytes).
     * @param vartblEntrySize Size of vartable entry (in bytes).
     * @param flags Format specific flags.
     */
    ChunkFormat(int vartblSizeFieldSize, int vartblEntrySize, byte flags) {
        this.vartblEntrySize = vartblEntrySize;
        this.vartblSizeFieldSize = vartblSizeFieldSize;
        this.flags = flags;
    }

    /**
     * @return Format specific flags for a chunk.
     */
    public byte formatFlags() {
        return flags;
    }

    /**
     * Calculates chunk size for the format.
     *
     * @param payloadLen Row payload length in bytes.
     * @param nullMapLen Null-map length in bytes.
     * @param vartblEntries Number of vartable entries.
     * @return Total chunk size.
     */
    int chunkSize(int payloadLen, int nullMapLen, int vartblEntries) {
        return CHUNK_LEN_FLD_SIZE /* Chunk len. */ + nullMapLen + vartableLength(vartblEntries - 1) + payloadLen;
    }

    /**
     * Calculates vartable size in bytes.
     *
     * @param entries Vartable entries.
     * @return Vartable size in bytes.
     */
    int vartableLength(int entries) {
        return entries <= 0 ? 0 : vartblSizeFieldSize /* Table size */ + entries * vartblEntrySize;
    }

    /**
     * Calculates vartable entry offset.
     *
     * @param idx Vartable entry idx.
     * @return Vartable entry offset.
     */
    int vartableEntryOffset(int idx) {
        return vartblSizeFieldSize /* Table size */ + idx * vartblEntrySize;
    }

    /**
     * Writes varlen offset to vartable.
     *
     * @param buf Row buffer.
     * @param vartblOff Vartable offset.
     * @param entryIdx Vartable entry index.
     * @param off Varlen offset to be written.
     */
    abstract void writeVarlenOffset(ExpandableByteBuf buf, int vartblOff, int entryIdx, int off);

    /**
     * Readss varlen offset from vartable.
     *
     * @param row Row.
     * @param vartblOff Vartable offset.
     * @param entryIdx Vartable entry index.
     * @return Varlen offset.
     */
    abstract int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx);

    /**
     * Writes vartable size.
     *
     * @param buf Row buffer.
     * @param vartblOff Vartable offset.
     * @param size Number of entries in the vartable.
     */
    abstract void writeVartableSize(ExpandableByteBuf buf, int vartblOff, int size);

    /**
     * Reads vartable size.
     *
     * @param row Row.
     * @param vartblOff Vartable offset.
     * @return Number of entries in the vartable.
     */
    abstract int readVartableSize(BinaryRow row, int vartblOff);

    /**
     * Chunk writer factory method.
     *
     * @param buf Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapLen Null-map length.
     * @param nonNullVarlens Number of entries in vartable.
     * @return Chunk writer.
     */
    ChunkWriter writer(ExpandableByteBuf buf, int baseOff, int nullMapLen, int nonNullVarlens) {
        return new ChunkWriter(
            buf,
            baseOff,
            nullMapLen,
            vartableLength(nonNullVarlens - 1),
            this);
    }

    /**
     * Chunk reader factory method.
     *
     * @param row Row buffer.
     * @param baseOff Chunk base offset.
     * @param nullMapSize Default chunk null-map size.
     * @param chunkFlags Chunk flags.
     * @return Chunk reader.
     */
    ChunkReader reader(BinaryRow row, int baseOff, int nullMapSize, byte chunkFlags) {
        return new ChunkReader(
            row,
            baseOff,
            (chunkFlags & OMIT_NULL_MAP_FLAG) == 0 ? nullMapSize : 0,
            (chunkFlags & OMIT_VARTBL_FLAG) == 0,
            this);
    }
}
