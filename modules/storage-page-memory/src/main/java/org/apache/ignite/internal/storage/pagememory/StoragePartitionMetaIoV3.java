package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import org.apache.ignite.internal.pagememory.util.PageIdUtils;

/** Storage Io for partition metadata pages (version 3). */
public class StoragePartitionMetaIoV3 extends StoragePartitionMetaIo {
    private static final int WI_HEAD_OFF = ESTIMATED_SIZE_OFF + Long.BYTES;

    /** Constructor. */
    StoragePartitionMetaIoV3() {
        super(3);
    }

    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setWiHead(pageAddr, PageIdUtils.NULL_LINK);
    }

    /**
     * Sets the head link of the write intent list in the partition metadata.
     *
     * @param pageAddr The address of the page to update.
     * @param headLink The link value to set as the head of the write intent list.
     */
    public void setWiHead(long pageAddr, long headLink) {
        assertPageType(pageAddr);

        putLong(pageAddr, WI_HEAD_OFF, headLink);
    }

    @Override
    public long getWiHead(long pageAddr) {
        return getLong(pageAddr, WI_HEAD_OFF);
    }
}
