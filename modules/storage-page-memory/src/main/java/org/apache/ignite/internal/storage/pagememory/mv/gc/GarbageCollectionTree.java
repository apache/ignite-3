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
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GarbageCollectionInnerIo;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GarbageCollectionIo;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GarbageCollectionLeafIo;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GarbageCollectionMetaIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for garbage collection of obsolete row versions in version chains.
 */
public class GarbageCollectionTree extends BplusTree<GarbageCollectionRowVersion, GarbageCollectionRowVersion> {
    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition id.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Global remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param initNew {@code True} if new tree should be created.
     * @throws IgniteInternalCheckedException If failed.
     */
    public GarbageCollectionTree(
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

        setIos(GarbageCollectionInnerIo.VERSIONS, GarbageCollectionLeafIo.VERSIONS, GarbageCollectionMetaIo.VERSIONS);

        initTree(initNew);
    }

    @Override
    protected int compare(BplusIo<GarbageCollectionRowVersion> io, long pageAddr, int idx, GarbageCollectionRowVersion row) {
        GarbageCollectionIo garbageCollectionIo = (GarbageCollectionIo) io;

        return garbageCollectionIo.compare(pageAddr, idx, row);
    }

    @Override
    public GarbageCollectionRowVersion getRow(BplusIo<GarbageCollectionRowVersion> io, long pageAddr, int idx, Object x) {
        GarbageCollectionIo garbageCollectionIo = (GarbageCollectionIo) io;

        return garbageCollectionIo.getRow(pageAddr, idx, partId);
    }

    /**
     * Adds a row version to the garbage collection queue.
     */
    public void addToGc(RowId rowId, HybridTimestamp timestamp) {
        try {
            put(new GarbageCollectionRowVersion(rowId, timestamp));
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
    public boolean removeFromGc(RowId rowId, HybridTimestamp timestamp) {
        try {
            return remove(new GarbageCollectionRowVersion(rowId, timestamp)) != null;
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
    public @Nullable GarbageCollectionRowVersion findFirstFromGc() {
        try {
            return findFirst();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error occurred while getting the first element from the garbage collection queue", e);
        }
    }
}
