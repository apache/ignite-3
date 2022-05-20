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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getBytes;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.storage.pagememory.TableTree.RowData.FULL;
import static org.apache.ignite.internal.storage.pagememory.TableTree.RowData.KEY_ONLY;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.pagememory.io.RowIo;
import org.apache.ignite.internal.storage.pagememory.io.TableDataIo;
import org.apache.ignite.internal.storage.pagememory.io.TableInnerIo;
import org.apache.ignite.internal.storage.pagememory.io.TableLeafIo;
import org.apache.ignite.internal.storage.pagememory.io.TableMetaIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for storage-page-memory module.
 */
public class TableTree extends BplusTree<TableSearchRow, TableDataRow> {
    private final int partId;

    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Global remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param partId Partition id.
     * @param initNew {@code True} if new tree should be created.
     */
    public TableTree(
            int grpId,
            String grpName,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            int partId,
            boolean initNew
    ) throws IgniteInternalCheckedException {
        super(
                "TableTree_" + grpId,
                grpId,
                grpName,
                pageMem,
                lockLsnr,
                FLAG_AUX,
                globalRmvId,
                metaPageId,
                reuseList
        );

        this.partId = partId;

        setIos(TableInnerIo.VERSIONS, TableLeafIo.VERSIONS, TableMetaIo.VERSIONS);

        initTree(initNew);
    }

    /** {@inheritDoc} */
    @Override
    protected long allocatePageNoReuse() throws IgniteInternalCheckedException {
        return pageMem.allocatePage(grpId, partId, defaultPageFlag);
    }

    /** {@inheritDoc} */
    @Override
    protected int compare(BplusIo<TableSearchRow> io, long pageAddr, int idx, TableSearchRow row) throws IgniteInternalCheckedException {
        RowIo rowIo = (RowIo) io;

        int cmp = Integer.compare(rowIo.hash(pageAddr, idx), row.hash());

        return cmp != 0 ? cmp : compareRows(rowIo.link(pageAddr, idx), row);
    }

    /** {@inheritDoc} */
    @Override
    public TableDataRow getRow(BplusIo<TableSearchRow> io, long pageAddr, int idx, Object x) throws IgniteInternalCheckedException {
        RowIo rowIo = (RowIo) io;

        int hash = rowIo.hash(pageAddr, idx);
        long link = rowIo.link(pageAddr, idx);

        return getRowByLink(link, hash, FULL);
    }

    /**
     * Returns a row by link.
     *
     * @param link Row link.
     * @param hash Row hash.
     * @param rowData Specifies what data to lookup.
     * @throws IgniteInternalCheckedException If failed.
     */
    public TableDataRow getRowByLink(final long link, int hash, RowData rowData) throws IgniteInternalCheckedException {
        assert link != 0;

        FragmentedByteArray keyBytes = null;
        FragmentedByteArray valueBytes = null;

        long nextLink = link;

        do {
            final long pageId = pageId(nextLink);

            final long page = pageMem.acquirePage(grpId, pageId, statisticsHolder());

            try {
                long pageAddr = pageMem.readLock(grpId, pageId, page);

                assert pageAddr != 0L : nextLink;

                try {
                    TableDataIo dataIo = pageMem.ioRegistry().resolve(pageAddr);

                    int itemId = itemId(nextLink);

                    int pageSize = pageMem.realPageSize(grpId);

                    DataPagePayload data = dataIo.readPayload(pageAddr, itemId, pageSize);

                    if (!data.hasMoreFragments() && nextLink == link) {
                        // Good luck: we can read the row without fragments.
                        return readFullRow(link, hash, rowData, pageAddr + data.offset());
                    }

                    ByteBuffer dataBuf = wrapPointer(pageAddr, pageSize);

                    dataBuf.position(data.offset());
                    dataBuf.limit(data.offset() + data.payloadSize());

                    if (keyBytes == null) {
                        keyBytes = new FragmentedByteArray();
                    }

                    keyBytes.readData(dataBuf);

                    if (keyBytes.ready()) {
                        if (rowData == KEY_ONLY) {
                            nextLink = 0;
                            continue;
                        }

                        if (valueBytes == null) {
                            valueBytes = new FragmentedByteArray();
                        }

                        valueBytes.readData(dataBuf);

                        if (valueBytes.ready()) {
                            nextLink = 0;
                            continue;
                        }
                    }

                    nextLink = data.nextLink();
                } finally {
                    pageMem.readUnlock(grpId, pageId, page);
                }
            } finally {
                pageMem.releasePage(grpId, pageId, page);
            }
        } while (nextLink != 0);

        ByteBuffer key = ByteBuffer.wrap(keyBytes.array());
        ByteBuffer value = ByteBuffer.wrap(valueBytes == null ? BYTE_EMPTY_ARRAY : valueBytes.array());

        return new TableDataRow(link, hash, key, value);
    }

    private TableDataRow readFullRow(long link, int hash, RowData rowData, long pageAddr) {
        int off = 0;

        int keyBytesLen = getInt(pageAddr, off);
        off += 4;

        byte[] keyBytes = getBytes(pageAddr, off, keyBytesLen);
        off += keyBytesLen;

        if (rowData == KEY_ONLY) {
            return new TableDataRow(link, hash, ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(BYTE_EMPTY_ARRAY));
        }

        int valueBytesLen = getInt(pageAddr, off);
        off += 4;

        byte[] valueBytes = getBytes(pageAddr, off, valueBytesLen);

        return new TableDataRow(link, hash, ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes));
    }

    private int compareRows(final long link, TableSearchRow row) throws IgniteInternalCheckedException {
        assert link != 0;

        long nextLink = link;

        int keyBytesLen = -1;
        int keyBytesOff = 0;

        do {
            final long pageId = pageId(nextLink);

            final long page = pageMem.acquirePage(grpId, pageId, statisticsHolder());

            try {
                final long pageAddr = pageMem.readLock(grpId, pageId, page);

                assert pageAddr != 0L : nextLink;

                try {
                    TableDataIo dataIo = pageMem.ioRegistry().resolve(pageAddr);

                    int itemId = itemId(nextLink);

                    int pageSize = pageMem.realPageSize(grpId);

                    DataPagePayload data = dataIo.readPayload(pageAddr, itemId, pageSize);

                    if (!data.hasMoreFragments() && nextLink == link) {
                        // Good luck: we can compare the rows without fragments.
                        return compareRowsFull(pageAddr + data.offset(), row);
                    }

                    ByteBuffer dataBuf = wrapPointer(pageAddr, pageSize);

                    dataBuf.position(data.offset());
                    dataBuf.limit(data.offset() + data.payloadSize());

                    ByteBuffer keyBuf = row.key();

                    if (keyBytesLen == -1) {
                        // Guaranteed to read because we store it in the header.
                        keyBytesLen = dataBuf.getInt();

                        int cmp = Integer.compare(keyBytesLen, keyBuf.limit());

                        if (cmp != 0) {
                            return cmp;
                        }
                    }

                    if (dataBuf.remaining() > 0) {
                        int len = Math.min(dataBuf.remaining(), keyBytesLen - keyBytesOff);

                        int dataBufPos = dataBuf.position();

                        dataBuf.position(dataBufPos);
                        dataBuf.limit(dataBufPos + len);

                        int oldKeyBufLimit = keyBuf.limit();

                        keyBuf.position(keyBytesOff);
                        keyBuf.limit(keyBytesOff + len);

                        int cmp = dataBuf.compareTo(keyBuf);

                        keyBytesOff += len;

                        keyBuf.limit(oldKeyBufLimit);

                        if (cmp != 0 || keyBytesOff == keyBytesLen) {
                            return cmp;
                        }
                    }

                    nextLink = data.nextLink();
                } finally {
                    pageMem.readUnlock(grpId, pageId, page);
                }
            } finally {
                pageMem.releasePage(grpId, pageId, page);
            }
        } while (nextLink != 0);

        throw new IgniteInternalCheckedException("Row comparison error [link=" + link + ", row=" + row + "]");
    }

    private int compareRowsFull(final long pageAddr, TableSearchRow row) {
        int off = 0;

        int keyBytesLen = getInt(pageAddr, off);
        off += 4;

        ByteBuffer key = row.key();

        int cmp = Integer.compare(keyBytesLen, key.limit());

        if (cmp != 0) {
            return cmp;
        }

        return wrapPointer(pageAddr + off, keyBytesLen).compareTo(key);
    }

    /**
     * Row data.
     */
    public enum RowData {
        /** Only {@link TableDataRow#key()}  key}. */
        KEY_ONLY,

        /** All: {@link TableDataRow#key()}  key} and {@link TableDataRow#value()}  value}. */
        FULL
    }
}
