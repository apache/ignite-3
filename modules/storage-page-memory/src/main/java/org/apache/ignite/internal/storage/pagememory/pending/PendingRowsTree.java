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

package org.apache.ignite.internal.storage.pagememory.pending;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;

public class PendingRowsTree extends BplusTree<PendingRowsKey, PendingRowsKey> {

    public PendingRowsTree(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            AtomicLong globalRmvId,
            long metaPageId,
            ReuseList reuseList,
            boolean initNew
    ) throws IgniteInternalCheckedException {
        super(
                "pending-row-tree_" + grpId,
                grpId,
                grpName,
                partId,
                pageMem,
                globalRmvId,
                metaPageId,
                reuseList
        );

        setIos(PendingRowsInnerIo.VERSIONS, PendingRowsLeafIo.VERSIONS, PendingRowsMetaIo.VERSIONS);

        initTree(initNew);
    }

    @Override
    protected int compare(BplusIo<PendingRowsKey> io, long pageAddr, int idx, PendingRowsKey row) throws IgniteInternalCheckedException {
        PendingRowsIo updateLogIo = (PendingRowsIo) io;

        return updateLogIo.compare(pageAddr, idx, row);
    }

    @Override
    public PendingRowsKey getRow(BplusIo<PendingRowsKey> io, long pageAddr, int idx, Object x) throws IgniteInternalCheckedException {
        PendingRowsIo updateLogIo = (PendingRowsIo) io;

        return updateLogIo.getRow(pageAddr, idx, partId);
    }
}
