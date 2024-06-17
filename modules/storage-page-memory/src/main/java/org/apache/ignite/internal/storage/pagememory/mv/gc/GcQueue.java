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

package org.apache.ignite.internal.storage.pagememory.mv.gc;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GcInnerIo;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GcIo;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GcLeafIo;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GcMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for garbage collection of obsolete row versions in version chains.
 */
public class GcQueue extends BplusTree<GcRowVersion, GcRowVersion> {
    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition id.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Global remove ID.
     * @param metaPageId Meta page ID. If 0, then allocates new page.
     * @param reuseList Reuse list.
     * @param initNew {@code True} if new tree should be created.
     * @throws IgniteInternalCheckedException If failed.
     */
    public GcQueue(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            boolean initNew
    ) throws IgniteInternalCheckedException {
        super(
                "GarbageCollectionTree_" + grpId,
                grpId,
                grpName,
                partId,
                pageMem,
                lockLsnr,
                globalRmvId,
                metaPageId,
                reuseList
        );

        setIos(GcInnerIo.VERSIONS, GcLeafIo.VERSIONS, GcMetaIo.VERSIONS);

        initTree(initNew);
    }

    @Override
    protected int compare(BplusIo<GcRowVersion> io, long pageAddr, int idx, GcRowVersion row) {
        GcIo gcIo = (GcIo) io;

        return gcIo.compare(pageAddr, idx, row);
    }

    @Override
    public GcRowVersion getRow(BplusIo<GcRowVersion> io, long pageAddr, int idx, Object x) {
        GcIo gcIo = (GcIo) io;

        return gcIo.getRow(pageAddr, idx, partId);
    }

    /**
     * Adds a row version to the garbage collection queue.
     */
    public void add(RowId rowId, HybridTimestamp timestamp, long link) {
        try {
            put(new GcRowVersion(rowId, timestamp, link));
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Error occurred while adding row version to the garbage collection queue: [rowId={}, timestamp={}, {}]",
                    e,
                    rowId, timestamp
            );
        }
    }

    /**
     * Removes a row version from the garbage collection queue.
     *
     * @return {@code true} if the row version was removed from the garbage collection queue.
     */
    public boolean remove(RowId rowId, HybridTimestamp timestamp, long link) {
        try {
            return removex(new GcRowVersion(rowId, timestamp, link));
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Error occurred while deleting row version form the garbage collection queue: [rowId={}, timestamp={}, {}]",
                    e,
                    rowId, timestamp
            );
        }
    }

    /**
     * Returns the first element from the garbage collection queue, {@code null} if the queue is empty.
     */
    public @Nullable GcRowVersion getFirst() {
        try {
            return findFirst();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error occurred while getting the first element from the garbage collection queue", e);
        }
    }
}
