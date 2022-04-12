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

package org.apache.ignite.internal.storage.pagememory.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.storage.pagememory.TableTree.RowData.KEY_ONLY;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.storage.pagememory.TableSearchRow;
import org.apache.ignite.internal.storage.pagememory.TableTree;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * IO routines for {@link TableTree} inner pages.
 *
 * <p>Structure: hash(int) + link(long).
 */
public class TableInnerIo extends BplusInnerIo<TableSearchRow> implements RowIo {
    private static final int LINK_OFFSET = 4;

    /** Page IO type. */
    public static final short T_TABLE_INNER_IO = 4;

    /** I/O versions. */
    public static final IoVersions<TableInnerIo> VERSIONS = new IoVersions<>(new TableInnerIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected TableInnerIo(int ver) {
        super(
                T_TABLE_INNER_IO,
                ver,
                true,
                Integer.BYTES + Long.BYTES // hash(int) + link(long);
        );
    }

    /** {@inheritDoc} */
    @Override
    public void store(long dstPageAddr, int dstIdx, BplusIo<TableSearchRow> srcIo, long srcPageAddr, int srcIdx) {
        assertPageType(dstPageAddr);

        int srcHash = hash(srcPageAddr, srcIdx);
        long srcLink = link(srcPageAddr, srcIdx);

        int dstOff = offset(dstIdx);

        putInt(dstPageAddr, dstOff, srcHash);
        dstOff += LINK_OFFSET;

        putLong(dstPageAddr, dstOff, srcLink);
    }

    /** {@inheritDoc} */
    @Override
    public void storeByOffset(long pageAddr, int off, TableSearchRow row) {
        assertPageType(pageAddr);

        putInt(pageAddr, off, row.hash());
        off += LINK_OFFSET;

        putLong(pageAddr, off, row.link());
    }

    /** {@inheritDoc} */
    @Override
    public TableSearchRow getLookupRow(BplusTree<TableSearchRow, ?> tree, long pageAddr, int idx) throws IgniteInternalCheckedException {
        int hash = hash(pageAddr, idx);
        long link = link(pageAddr, idx);

        return ((TableTree) tree).getRowByLink(link, hash, KEY_ONLY);
    }

    /** {@inheritDoc} */
    @Override
    public long link(long pageAddr, int idx) {
        assert idx < getCount(pageAddr) : idx;

        return getLong(pageAddr, offset(idx) + LINK_OFFSET);
    }

    /** {@inheritDoc} */
    @Override
    public int hash(long pageAddr, int idx) {
        assert idx < getCount(pageAddr) : idx;

        return getInt(pageAddr, offset(idx));
    }
}
