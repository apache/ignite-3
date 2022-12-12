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

import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.lang.IgniteStringBuilder;

/**
 * Pages IO for first blob page.
 */
public class BlobFirstIo extends BlobIo {
    /** Page IO type. */
    public static final short T_BLOB_FIRST_IO = 13;

    private static final int NEXT_PAGE_ID_OFF = PageIo.COMMON_HEADER_END;
    private static final int TOTAL_LENGTH_OFF = NEXT_PAGE_ID_OFF + Long.BYTES;
    private static final int FRAGMENT_LENGTH_OFF = TOTAL_LENGTH_OFF + Long.BYTES;
    private static final int FRAGMENT_BYTES_OFF = FRAGMENT_LENGTH_OFF + Integer.BYTES;

    /** I/O versions. */
    public static final IoVersions<BlobFirstIo> VERSIONS = new IoVersions<>(new BlobFirstIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private BlobFirstIo(int ver) {
        super(T_BLOB_FIRST_IO, ver);
    }

    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setNextPageId(pageAddr, 0);
        setTotalLength(pageAddr, 0);
        setFragmentLength(pageAddr, 0);
    }

    @Override
    public int fullHeaderSize() {
        return FRAGMENT_BYTES_OFF;
    }

    @Override
    public long getNextPageId(long pageAddr) {
        return getLong(pageAddr, NEXT_PAGE_ID_OFF);
    }

    @Override
    public void setNextPageId(long pageAddr, long nextPageId) {
        putLong(pageAddr, NEXT_PAGE_ID_OFF, nextPageId);
    }

    @Override
    public int getTotalLength(long pageAddr) {
        return getInt(pageAddr, TOTAL_LENGTH_OFF);
    }

    @Override
    public void setTotalLength(long pageAddr, int totalLength) {
        putInt(pageAddr, TOTAL_LENGTH_OFF, totalLength);
    }

    @Override
    public int getFragmentLength(long pageAddr) {
        return getInt(pageAddr, FRAGMENT_LENGTH_OFF);
    }

    @Override
    public void setFragmentLength(long pageAddr, int fragmentLength) {
        putInt(pageAddr, FRAGMENT_LENGTH_OFF, fragmentLength);
    }

    @Override
    public void getFragmentBytes(long pageAddr, byte[] destArray, int destOffset, int fragmentLength) {
        PageUtils.getBytes(pageAddr, FRAGMENT_BYTES_OFF, destArray, destOffset, fragmentLength);
    }

    @Override
    public void setFragmentBytes(long pageAddr, byte[] bytes, int bytesOffset, int fragmentLength) {
        PageUtils.putBytes(pageAddr, FRAGMENT_BYTES_OFF, bytes, bytesOffset, fragmentLength);
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("BlobFirstIo [").nl()
                .app("nextPageId=").app(getNextPageId(addr)).nl()
                .app("fragmentLength=").app(getFragmentLength(addr)).nl()
                .app(']');
    }
}
