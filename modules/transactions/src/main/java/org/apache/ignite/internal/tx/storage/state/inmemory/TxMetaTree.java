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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

public class TxMetaTree extends BplusTree<TxMetaRowWrapper, TxMetaRowWrapper> {
    protected TxMetaTree(
        String name,
        int grpId,
        @Nullable String grpName,
        int partId,
        PageMemory pageMem,
        PageLockListener lockLsnr,
        AtomicLong globalRmvId,
        long metaPageId,
        @Nullable ReuseList reuseList,
        boolean initNew
    ) throws IgniteInternalCheckedException {
        super(name, grpId, grpName, partId, pageMem, lockLsnr, globalRmvId, metaPageId, reuseList);

        setIos(TxMetaStorageInnerIo.VERSIONS, TxMetaStorageLeafIo.VERSIONS, TxMetaStorageMetaIo.VERSIONS);

        initTree(initNew);
    }

    @Override protected int compare(
        BplusIo<TxMetaRowWrapper> io,
        long pageAddr,
        int idx,
        TxMetaRowWrapper row
    ) throws IgniteInternalCheckedException {
        Objects.requireNonNull(row);

        TxMetaRowWrapper r = io.getLookupRow(this, pageAddr, idx);

        return r.compareTo(row);
    }

    @Override public TxMetaRowWrapper getRow(
        BplusIo<TxMetaRowWrapper> io,
        long pageAddr,
        int idx,
        Object x
    ) throws IgniteInternalCheckedException {
        return io.getLookupRow(this, pageAddr, idx);
    }
}
