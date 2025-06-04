package org.apache.ignite.internal.storage.pagememory.pending;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.RowId;

public interface PendingRowsIo {
    int ROW_MOST_SIGNIFICANT_BITS_OFFSET = 0;

    int ROW_LEAST_SIGNIFICANT_BITS_OFFSET = 8;

    int ROW_ID_MSB_OFFSET = ROW_LEAST_SIGNIFICANT_BITS_OFFSET + 8;

    int ROW_ID_LSB_OFFSET = ROW_ID_MSB_OFFSET + 8;

    int SIZE_IN_BYTES = ROW_ID_LSB_OFFSET + 8;

    /**
     * Returns an offset of the element inside the page.
     *
     * @see BplusIo#offset(int)
     */
    int offset(int idx);

    /**
     * Stores a row, copied from another page.
     *
     * @see BplusIo#store(long, int, BplusIo, long, int)
     */
    default void store(long dstPageAddr, int dstIdx, BplusIo<PendingRowsKey> srcIo, long srcPageAddr, int srcIdx) {
        int dstOffset = offset(dstIdx);
        int srcOffset = srcIo.offset(srcIdx);

        PageUtils.copyMemory(srcPageAddr, srcOffset, dstPageAddr, dstOffset, SIZE_IN_BYTES);
    }

    /**
     * Stores a row in the page.
     *
     * @see BplusIo#storeByOffset(long, int, Object)
     */
    default void storeByOffset(long pageAddr, int off, PendingRowsKey row) {
        putLong(pageAddr, off + ROW_MOST_SIGNIFICANT_BITS_OFFSET, row.transactionId().getMostSignificantBits());
        putLong(pageAddr, off + ROW_LEAST_SIGNIFICANT_BITS_OFFSET, row.transactionId().getLeastSignificantBits());
        putLong(pageAddr, off + ROW_ID_MSB_OFFSET, row.rowId().mostSignificantBits());
        putLong(pageAddr, off + ROW_ID_LSB_OFFSET, row.rowId().leastSignificantBits());
    }

    /**
     * Compare the row from the page with passed row, thus defining the order of element in the {@link PendingRowsTree}.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param row Row.
     * @return Comparison result.
     */
    default int compare(long pageAddr, int idx, PendingRowsKey row) {
        int offset = offset(idx);

        int cmp = Long.compare(getLong(pageAddr, offset + ROW_MOST_SIGNIFICANT_BITS_OFFSET), row.transactionId().getMostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        cmp = Long.compare(getLong(pageAddr, offset + ROW_LEAST_SIGNIFICANT_BITS_OFFSET), row.transactionId().getLeastSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        cmp = Long.compare(getLong(pageAddr, offset + ROW_ID_MSB_OFFSET), row.rowId().mostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(getLong(pageAddr, offset + ROW_ID_LSB_OFFSET), row.rowId().leastSignificantBits());
    }

    /**
     * Reads a row from the page.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param partitionId Partition id to enrich read partitionless links.
     * @return Version chain.
     */
    default PendingRowsKey getRow(long pageAddr, int idx, int partitionId) {
        int offset = offset(idx);
        long msb = getLong(pageAddr, offset + ROW_MOST_SIGNIFICANT_BITS_OFFSET);
        long lsb = getLong(pageAddr, offset + ROW_LEAST_SIGNIFICANT_BITS_OFFSET);
        long rowIdMsb = getLong(pageAddr, offset + ROW_ID_MSB_OFFSET);
        long rowIdLsb = getLong(pageAddr, offset + ROW_ID_LSB_OFFSET);

        return new PendingRowsKey(new UUID(msb, lsb), new RowId(partitionId, rowIdMsb, rowIdLsb));
    }
}
