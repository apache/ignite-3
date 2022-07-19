/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx.storage.state.inmemory;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

public class TxMetaStorageLeafIo extends BplusLeafIo<TxMetaRowWrapper> {
    /** Page IO type. */
    public static final short T_TX_STORAGE_LEAF_IO = 10003;

    public static final short MOST_SIGNIFICANT_OFF = Long.BYTES;

    public static final short LINK_OFF = MOST_SIGNIFICANT_OFF + Long.BYTES;

    /** I/O versions. */
    public static final IoVersions<TxMetaStorageLeafIo> VERSIONS = new IoVersions<>(new TxMetaStorageLeafIo(1));

    /**
     * Constructor.
     *
     * @param ver       Page format version.
     */
    protected TxMetaStorageLeafIo(int ver) {
        super(T_TX_STORAGE_LEAF_IO, ver, Long.BYTES * 3);
    }

    @Override public void store(
        long dstPageAddr,
        int dstIdx,
        BplusIo<TxMetaRowWrapper> srcIo,
        long srcPageAddr,
        int srcIdx
    ) throws IgniteInternalCheckedException {
        assertPageType(dstPageAddr);

        int srcOff = srcIdx * itemSize;
        long least = getLong(srcPageAddr, srcOff);

        srcOff += MOST_SIGNIFICANT_OFF;
        long most = getLong(srcPageAddr, srcOff);

        srcOff += LINK_OFF;
        long link = getLong(srcPageAddr, srcOff);

        // Write to destination.
        int dstOff = dstIdx * itemSize;
        putLong(dstPageAddr, dstOff, least);

        dstOff += MOST_SIGNIFICANT_OFF;
        putLong(dstPageAddr, dstOff, most);

        dstOff += LINK_OFF;
        putLong(dstPageAddr, dstOff, link);
    }

    @Override
    public void storeByOffset(long pageAddr, int off, TxMetaRowWrapper row) {
        assertPageType(pageAddr);

        putLong(pageAddr, off, row.txId().getLeastSignificantBits());

        off += MOST_SIGNIFICANT_OFF;

        putLong(pageAddr, off, row.txId().getMostSignificantBits());

        off += LINK_OFF;

        putLong(pageAddr, off, row.link());
    }

    @Override public TxMetaRowWrapper getLookupRow(
        BplusTree<TxMetaRowWrapper, ?> tree,
        long pageAddr,
        int idx
    ) {
        int off = idx * itemSize;
        long link = getLong(pageAddr, off);
        return ((TxMetaTree) tree).getRowByLink(link);
    }
}
