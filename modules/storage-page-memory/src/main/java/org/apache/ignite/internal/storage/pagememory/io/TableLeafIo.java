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

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.storage.pagememory.TableSearchRow;
import org.apache.ignite.internal.storage.pagememory.TableTree;

/**
 * IO routines for {@link TableTree} leaf pages.
 *
 * <p>Structure: hash(int) + link(long).
 */
public class TableLeafIo extends BplusLeafIo<TableSearchRow> {
    /** Page IO type. */
    public static final short T_TABLE_LEAF_IO = 5;

    /** I/O versions. */
    public static final IoVersions<TableLeafIo> VERSIONS = new IoVersions<>(new TableLeafIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected TableLeafIo(int ver) {
        super(
                T_TABLE_LEAF_IO,
                ver,
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
        dstOff += 4;

        putLong(dstPageAddr, dstOff, srcLink);

    }

    /** {@inheritDoc} */
    @Override
    public void storeByOffset(long pageAddr, int off, TableSearchRow row) {
        assertPageType(pageAddr);

        putInt(pageAddr, off, row.hash());
        off += 4;

        putLong(pageAddr, off, row.link());
    }

    /** {@inheritDoc} */
    @Override
    public TableSearchRow getLookupRow(BplusTree<TableSearchRow, ?> tree, long pageAddr, int idx) {
        int hash = hash(pageAddr, idx);
        long link = link(pageAddr, idx);

        // TODO: Кажется тут нам надо загрузить только ключи.

        return null;
    }

    /**
     * Returns the link for the element in the page by index.
     *
     * @param pageAddr Page address.
     * @param idx Index.
     */
    public long link(long pageAddr, int idx) {
        assert idx < getCount(pageAddr) : idx;

        return getLong(pageAddr, offset(idx) + 4 /* hash ahead */);
    }

    /**
     * Returns the hash for the element in the page by index.
     *
     * @param pageAddr Page address.
     * @param idx Index.
     */
    public int hash(long pageAddr, int idx) {
        assert idx < getCount(pageAddr) : idx;

        return getInt(pageAddr, offset(idx));
    }
}
