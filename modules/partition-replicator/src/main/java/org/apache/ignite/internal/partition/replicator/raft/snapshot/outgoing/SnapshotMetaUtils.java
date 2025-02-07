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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.raft.PartitionSnapshotMeta;
import org.apache.ignite.internal.partition.replicator.network.raft.PartitionSnapshotMetaBuilder;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.jetbrains.annotations.Nullable;

/**
 * Utils to build {@link SnapshotMeta} instances.
 */
public class SnapshotMetaUtils {
    /**
     * Builds a {@link SnapshotMeta} corresponding to RAFT state (term, configuration) at the given log index.
     *
     * @param logIndex RAFT log index.
     * @param term Term corresponding to the index.
     * @param config RAFT group configuration.
     * @param requiredCatalogVersion Catalog version that a follower/learner must have to have ability to accept this snapshot.
     * @param nextRowIdToBuildByIndexId Row ID for which the index needs to be built per building index ID at the time the snapshot meta was
     *      created.
     * @param leaseStartTime Lease start time (as {@link HybridTimestamp#longValue()}).
     * @param primaryReplicaNodeId Primary replica node ID.
     * @param primaryReplicaNodeName Primary replica node name.
     * @return SnapshotMeta corresponding to the given log index.
     */
    public static PartitionSnapshotMeta snapshotMetaAt(
            long logIndex,
            long term,
            RaftGroupConfiguration config,
            int requiredCatalogVersion,
            Map<Integer, UUID> nextRowIdToBuildByIndexId,
            long leaseStartTime,
            @Nullable UUID primaryReplicaNodeId,
            @Nullable String primaryReplicaNodeName
    ) {
        PartitionSnapshotMetaBuilder metaBuilder = new PartitionReplicationMessagesFactory().partitionSnapshotMeta()
                .cfgIndex(config.index())
                .cfgTerm(config.term())
                .lastIncludedIndex(logIndex)
                .lastIncludedTerm(term)
                .peersList(config.peers())
                .learnersList(config.learners())
                .requiredCatalogVersion(requiredCatalogVersion)
                .nextRowIdToBuildByIndexId(nextRowIdToBuildByIndexId)
                .leaseStartTime(leaseStartTime)
                .primaryReplicaNodeId(primaryReplicaNodeId)
                .primaryReplicaNodeName(primaryReplicaNodeName);

        if (!config.isStable()) {
            //noinspection ConstantConditions
            metaBuilder
                    .oldPeersList(config.oldPeers())
                    .oldLearnersList(config.oldLearners());
        }

        return metaBuilder.build();
    }

    /**
     * Collects the row ID for which the index needs to be built per building index ID at the time the snapshot meta was created.
     *
     * @param catalogService Catalog service.
     * @param partitionAccess Partition access.
     * @param catalogVersion Catalog version of interest.
     */
    public static Map<Integer, UUID> collectNextRowIdToBuildIndexes(
            CatalogService catalogService,
            PartitionAccess partitionAccess,
            int catalogVersion
    ) {
        var nextRowIdToBuildByIndexId = new HashMap<Integer, UUID>();

        Catalog catalog = catalogService.catalog(catalogVersion);
        int tableId = partitionAccess.partitionKey().tableId();
        for (CatalogIndexDescriptor index : catalog.indexes(tableId)) {
            if (index.status() == BUILDING) {
                RowId nextRowIdToBuild = partitionAccess.getNextRowIdToBuildIndex(index.id());

                if (nextRowIdToBuild != null) {
                    nextRowIdToBuildByIndexId.put(index.id(), nextRowIdToBuild.uuid());
                }
            }
        }

        return nextRowIdToBuildByIndexId;
    }
}
