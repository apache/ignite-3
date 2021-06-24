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
abstract class VarTableFormat {
    /** First two flag bits reserved for format code. */
    public static final int FORMAT_CODE_MASK = 0x03;

    /** Flag indicates key chunk omits vartable. */
    public static final int OMIT_NULL_MAP_FLAG = 1 << 2;

    /** Flag indicates value chunk omits null map. */
    public static final int OMIT_VARTBL_FLAG = 1 << 3;

    /** Writer factory for tiny-sized chunks. */
    static final VarTableFormat TINY = new TinyFormat();

    /** Writer factory for med-sized chunks. */
    static final VarTableFormat MEDIUM = new MediumFormat();

    /** Writer factory for large-sized chunks. */
    static final VarTableFormat LARGE = new LargeFormat();

    /**
     * Return chunk formatter.
     *
     * @param payloadLen Payload size in bytes.
     * @return Chunk formatter.
     */
    static VarTableFormat format(int payloadLen) {
        if (payloadLen > 0) {
            if (payloadLen < 256)
                return TINY;

            if (payloadLen < 64 * 1024)
                return MEDIUM;
        }

        return LARGE;
    }

    /**
     * Chunk format factory method.
     *
     * @param chunkFlags Chunk specific flags. Only first 4-bits are meaningful.
     * @return Chunk formatter regarding the provided flags.
     */
    public static VarTableFormat fromFlags(int chunkFlags) {
        switch (chunkFlags & FORMAT_CODE_MASK) {
            case 1:
                return TINY;
            case 2:
                return MEDIUM;
            default:
                return LARGE;
        }
    }

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
    VarTableFormat(int vartblSizeFieldSize, int vartblEntrySize, byte flags) {
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
     * Readss varlen offset from vartable.
     *
     * @param row Row.
     * @param vartblOff Vartable offset.
     * @param entryIdx Vartable entry index.
     * @return Varlen offset.
     */
    abstract int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx);

    /**
     * Reads vartable size.
     *
     * @param row Row.
     * @param vartblOff Vartable offset.
     * @return Number of entries in the vartable.
     */
    abstract int readVartableSize(BinaryRow row, int vartblOff);

    /**
     * Convert vartable inplace to the current format.
     *
     * @param buf Row buffer.
     * @param vartblOff Vartable offset.
     * @param entries Number of entries in the vartable.
     * @return Number of bytes vartable was shrinked by.
     */
    public abstract int compactVarTable(ExpandableByteBuf buf, int vartblOff, int entries);

    /**
     * Chunk format for small rows.
     */
    private static class TinyFormat extends VarTableFormat {
        /**
         * Creates chunk format.
         */
        TinyFormat() {
            super(Byte.BYTES, Byte.BYTES, (byte)1);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return Byte.toUnsignedInt(row.readByte(vartblOff + vartableEntryOffset(entryIdx)));
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return Byte.toUnsignedInt(row.readByte(vartblOff));
        }

        /** {@inheritDoc} */
        @Override public int compactVarTable(ExpandableByteBuf buf, int vartblOff, int entres) {
            assert entres > 0;

            buf.put(vartblOff, (byte)entres);

            int dstOff = vartblOff + 1;
            int srcOff = vartblOff + 2;

            for (int i = 0; i < entres; i++, srcOff += Integer.BYTES, dstOff++)
                buf.put(dstOff, buf.get(srcOff));

            buf.shift(srcOff, dstOff);

            return srcOff - dstOff;
        }
    }

    /**
     * Chunk format for rows od medium size.
     */
    private static class MediumFormat extends VarTableFormat {
        /**
         * Creates chunk format.
         */
        MediumFormat() {
            super(Short.BYTES, Short.BYTES, (byte)2);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return Short.toUnsignedInt(row.readShort(vartblOff + vartableEntryOffset(entryIdx)));
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return Short.toUnsignedInt(row.readShort(vartblOff));
        }

        /** {@inheritDoc} */
        @Override public int compactVarTable(ExpandableByteBuf buf, int vartblOff, int entries) {
            buf.putShort(vartblOff, (short)entries);

            int dstOff = vartblOff + 2;
            int srcOff = vartblOff + 2;

            for (int i = 0; i < entries; i++, srcOff += Integer.BYTES, dstOff += Short.BYTES)
                buf.putShort(dstOff, buf.getShort(srcOff));

            buf.shift(srcOff, dstOff);

            return srcOff - dstOff;
        }
    }

    /**
     * Chunk format for large rows.
     */
    private static class LargeFormat extends VarTableFormat {
        /**
         * Creates chunk format.
         */
        LargeFormat() {
            super(Short.BYTES, Integer.BYTES, (byte)0);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return row.readInteger(vartblOff + vartableEntryOffset(entryIdx));
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return Short.toUnsignedInt(row.readShort(vartblOff));
        }

        /** {@inheritDoc} */
        @Override public int compactVarTable(ExpandableByteBuf buf, int vartblOff, int entries) {
            buf.putShort(vartblOff, (short)entries);

            return 0; // Nothing to do.
        }
    }
}
