/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.pagememory.mv.io;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.storage.pagememory.mv.MvPageTypes.T_BLOB_FRAGMENT_IO;

import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;

/**
 * Pages IO for blob fragments. The blob itself is stored as a chain of fragments, one fragment per page.
 *
 * <p>A page layout is as follows:
 * <ul>
 *     <li>ID of the next page in the chain is stored (0 if current page is the last one in the chain) [8 bytes]</li>
 *     <li>Total blob length (only present if the page is the first page in a chain, otherwise this field is skipped) [4 bytes]</li>
 *     <li>Bytes representing the current fragment</li>
 * </ul>
 */
public class BlobFragmentIo extends PageIo {
    private static final int NEXT_PAGE_ID_OFF = COMMON_HEADER_END;

    private static final int FRAGMENT_BYTES_OR_TOTAL_LENGTH_OFF = NEXT_PAGE_ID_OFF + Long.BYTES;

    /** I/O versions. */
    public static final IoVersions<BlobFragmentIo> VERSIONS = new IoVersions<>(new BlobFragmentIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private BlobFragmentIo(int ver) {
        super(T_BLOB_FRAGMENT_IO, ver, FLAG_AUX);
    }

    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setNextPageId(pageAddr, 0);
    }

    /**
     * Returns number of bytes of a blob that can be stored in a page.
     *
     * @param pageSize Page size in bytes.
     * @param firstPage Whether this is the first page of a chain representing a blob.
     */
    public int getCapacityForFragmentBytes(int pageSize, boolean firstPage) {
        return pageSize - fragmentBytesOffset(firstPage);
    }

    /**
     * Reads next page ID.
     */
    public long getNextPageId(long pageAddr) {
        return getLong(pageAddr, NEXT_PAGE_ID_OFF);
    }

    /**
     * Writes next page ID.
     */
    public void setNextPageId(long pageAddr, long nextPageId) {
        putLong(pageAddr, NEXT_PAGE_ID_OFF, nextPageId);
    }

    /**
     * Reads total blob length.
     */
    public int getTotalLength(long pageAddr) {
        return getInt(pageAddr, FRAGMENT_BYTES_OR_TOTAL_LENGTH_OFF);
    }

    /**
     * Writes total blob length.
     */
    public void setTotalLength(long pageAddr, int totalLength) {
        putInt(pageAddr, FRAGMENT_BYTES_OR_TOTAL_LENGTH_OFF, totalLength);
    }

    /**
     * Reads fragment bytes to the given array.
     */
    public void getFragmentBytes(long pageAddr, boolean firstPage, byte[] destArray, int destOffset, int fragmentLength) {
        PageUtils.getBytes(pageAddr, fragmentBytesOffset(firstPage), destArray, destOffset, fragmentLength);
    }

    /**
     * Writes fragment bytes from the given array.
     */
    public void setFragmentBytes(long pageAddr, boolean firstPage, byte[] bytes, int bytesOffset, int fragmentLength) {
        PageUtils.putBytes(pageAddr, fragmentBytesOffset(firstPage), bytes, bytesOffset, fragmentLength);
    }

    private static int fragmentBytesOffset(boolean firstPage) {
        return FRAGMENT_BYTES_OR_TOTAL_LENGTH_OFF + (firstPage ? Integer.BYTES : 0);
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("BlobFragmentIo [").nl()
                .app("nextPageId=").app(getNextPageId(addr)).nl()
                .app(']');
    }
}
