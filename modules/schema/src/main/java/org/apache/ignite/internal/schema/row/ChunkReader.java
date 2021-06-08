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
import org.apache.ignite.internal.schema.Columns;

/**
 * Abstract chunk reader.
 */
abstract class ChunkReader {
    /** Row. */
    protected final BinaryRow row;

    /** Base offset. */
    protected final int baseOff;

    /** Null-map offset. */
    protected int nullMapOff;

    /** Vartable offset. */
    protected int varTableOff;

    /** Payload offset. */
    protected int dataOff;

    /**
     * @param baseOff Chunk base offset.
     */
    ChunkReader(BinaryRow row, int baseOff) {
        this.row = row;
        this.baseOff = baseOff;
    }

    /**
     * @return Chunk length in bytes
     */
    abstract int chunkLength();

    /**
     * @return Number of items in vartable.
     */
    abstract int vartableItems();

    /**
     * Checks the row's null map for the given column index in the chunk.
     *
     * @param idx Offset of the column in the chunk.
     * @return {@code true} if the column value is {@code null}.
     */
    /** {@inheritDoc} */
    protected boolean isNull(int idx) {
        if (!hasNullmap())
            return false;

        int nullByte = idx / 8;
        int posInByte = idx % 8;

        int map = row.readByte(nullMapOff + nullByte);

        return (map & (1 << posInByte)) != 0;
    }

    /**
     * @return {@code True} if chunk has vartable.
     */
    protected boolean hasVartable() {
        return dataOff > varTableOff;
    }

    /**
     * @return {@code True} if chunk has nullmap.
     */
    protected boolean hasNullmap() {
        return varTableOff > nullMapOff;
    }

    /**
     * @param itemIdx Varlen table item index.
     * @return Varlen item offset.
     */
    protected abstract int varlenItemOffset(int itemIdx);

    /**
     * Calculates the offset of the fixlen column with the given index in the row. It essentially folds the null map
     * with the column lengths to calculate the size of non-null columns preceding the requested column.
     *
     * @param cols Columns chunk.
     * @param idx Column index in the chunk.
     * @return Encoded offset (from the row start) of the requested fixlen column.
     */
    int fixlenColumnOffset(Columns cols, int idx) {
        int colOff = 0;

        // Calculate fixlen column offset.
        {
            int colByteIdx = idx / 8;

            // Set bits starting from posInByte, inclusive, up to either the end of the byte or the last column index, inclusive
            int startBit = idx % 8;
            int endBit = colByteIdx == (cols.length() + 7) / 8 - 1 ? ((cols.numberOfFixsizeColumns() - 1) % 8) : 7;
            int mask = (0xFF >> (7 - endBit)) & (0xFF << startBit);

            if (hasNullmap()) {
                // Fold offset based on the whole map bytes in the schema
                for (int i = 0; i < colByteIdx; i++)
                    colOff += cols.foldFixedLength(i, row.readByte(nullMapOff + i));

                colOff += cols.foldFixedLength(colByteIdx, row.readByte(nullMapOff + colByteIdx) | mask);
            }
            else {
                for (int i = 0; i < colByteIdx; i++)
                    colOff += cols.foldFixedLength(i, 0);

                colOff += cols.foldFixedLength(colByteIdx, mask);
            }
        }

        return dataOff + colOff;
    }

    /**
     * Calculates the offset and length of varlen column. First, it calculates the number of non-null columns
     * preceding the requested column by folding the null map bits. This number is used to adjust the column index
     * and find the corresponding entry in the varlen table. The length of the column is calculated either by
     * subtracting two adjacent varlen table offsets, or by subtracting the last varlen table offset from the chunk
     * length.
     *
     * @param cols Columns chunk.
     * @param idx Column index in the chunk.
     * @return Encoded offset (from the row start) and length of the column with the given index.
     */
    long varlenColumnOffsetAndLength(Columns cols, int idx) {
        assert hasVartable() : "Chunk has no vartable: colId=" + idx;

        if (hasNullmap()) {
            int nullStartByte = cols.firstVarlengthColumn() / 8;
            int startBitInByte = cols.firstVarlengthColumn() % 8;

            int nullEndByte = idx / 8;
            int endBitInByte = idx % 8;

            int numNullsBefore = 0;

            for (int i = nullStartByte; i <= nullEndByte; i++) {
                byte nullmapByte = row.readByte(nullMapOff + i);

                if (i == nullStartByte)
                    // We need to clear startBitInByte least significant bits
                    nullmapByte &= (0xFF << startBitInByte);

                if (i == nullEndByte)
                    // We need to clear 8-endBitInByte most significant bits
                    nullmapByte &= (0xFF >> (8 - endBitInByte));

                numNullsBefore += Columns.numberOfNullColumns(nullmapByte);
            }

            idx -= numNullsBefore;
        }

        idx -= cols.numberOfFixsizeColumns();

        // Offset of idx-th column is from base offset.
        int resOff = varlenItemOffset(idx);

        long len = (idx == vartableItems() - 1) ?
            // totalLength - columnStartOffset
            (baseOff + chunkLength()) - resOff :
            // nextColumnStartOffset - columnStartOffset
            varlenItemOffset(idx + 1) - resOff;

        return (len << 32) | resOff;
    }
}
