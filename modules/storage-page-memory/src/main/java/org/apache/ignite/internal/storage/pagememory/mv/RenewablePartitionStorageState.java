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

import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;

/**
 * Simple holder class that comprises the volatile state of {@link AbstractPageMemoryMvPartitionStorage} that gets re-instantiated on
 * partition rebalance.
 */
class RenewablePartitionStorageState {
    private final VersionChainTree versionChainTree;

    private final RowVersionFreeList rowVersionFreeList;

    private final IndexColumnsFreeList indexFreeList;

    private final IndexMetaTree indexMetaTree;

    private final GcQueue gcQueue;

    /** Creates a new instance. */
    RenewablePartitionStorageState(
            VersionChainTree versionChainTree,
            RowVersionFreeList rowVersionFreeList,
            IndexColumnsFreeList indexFreeList,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue
    ) {
        this.versionChainTree = versionChainTree;
        this.rowVersionFreeList = rowVersionFreeList;
        this.indexFreeList = indexFreeList;
        this.indexMetaTree = indexMetaTree;
        this.gcQueue = gcQueue;
    }

    VersionChainTree versionChainTree() {
        return versionChainTree;
    }

    RowVersionFreeList rowVersionFreeList() {
        return rowVersionFreeList;
    }

    IndexColumnsFreeList indexFreeList() {
        return indexFreeList;
    }

    IndexMetaTree indexMetaTree() {
        return indexMetaTree;
    }

    GcQueue gcQueue() {
        return gcQueue;
    }
}
