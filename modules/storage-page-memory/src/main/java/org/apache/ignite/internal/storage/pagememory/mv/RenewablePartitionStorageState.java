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

package org.apache.ignite.internal.storage.pagememory.mv;

import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.storage.pagememory.AbstractPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;

/**
 * Simple holder class that comprises the volatile state of {@link AbstractPageMemoryMvPartitionStorage} that gets re-instantiated on
 * partition rebalance.
 */
class RenewablePartitionStorageState {
    private final VersionChainTree versionChainTree;

    private final FreeListImpl freeList;

    private final IndexMetaTree indexMetaTree;

    private final GcQueue gcQueue;

    private final IndexStorageFactory indexStorageFactory;

    /** Creates a new instance. */
    RenewablePartitionStorageState(
            AbstractPageMemoryTableStorage tableStorage,
            int partitionId,
            VersionChainTree versionChainTree,
            FreeListImpl freeList,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue
    ) {
        this.versionChainTree = versionChainTree;
        this.freeList = freeList;
        this.indexMetaTree = indexMetaTree;
        this.gcQueue = gcQueue;

        this.indexStorageFactory = new IndexStorageFactory(
                tableStorage,
                partitionId,
                indexMetaTree,
                freeList
        );
    }

    VersionChainTree versionChainTree() {
        return versionChainTree;
    }

    FreeListImpl freeList() {
        return freeList;
    }

    IndexMetaTree indexMetaTree() {
        return indexMetaTree;
    }

    GcQueue gcQueue() {
        return gcQueue;
    }

    IndexStorageFactory indexStorageFactory() {
        return indexStorageFactory;
    }
}
