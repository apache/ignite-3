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

package org.apache.ignite.internal.storage.pagememory;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaFactory;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * * Factory for creating {@link StoragePartitionMeta} instances.
 */
public class StoragePartitionMetaFactory implements PartitionMetaFactory {
    @Override
    public StoragePartitionMeta createPartitionMeta(@Nullable UUID checkpointId, PartitionMetaIo abstractMetaIo, long pageAddr) {
        StoragePartitionMetaIo metaIo = (StoragePartitionMetaIo) abstractMetaIo;

        var result = new StoragePartitionMeta(
                metaIo.getPageCount(pageAddr),
                metaIo.getLastAppliedIndex(pageAddr),
                metaIo.getLastAppliedTerm(pageAddr),
                metaIo.getLastReplicationProtocolGroupConfigFirstPageId(pageAddr),
                metaIo.getLeaseStartTime(pageAddr),
                metaIo.getPrimaryReplicaNodeIdFirstPageId(pageAddr),
                metaIo.getPrimaryReplicaNodeNameFirstPageId(pageAddr),
                StoragePartitionMetaIo.getFreeListRootPageId(pageAddr),
                metaIo.getVersionChainTreeRootPageId(pageAddr),
                metaIo.getIndexTreeMetaPageId(pageAddr),
                metaIo.getGcQueueMetaPageId(pageAddr),
                metaIo.getEstimatedSize(pageAddr)
        );

        return result.init(checkpointId);
    }

    @Override
    public StoragePartitionMetaIo partitionMetaIo() {
        return StoragePartitionMetaIo.VERSIONS.latest();
    }
}
