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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.entity.SnapshotMetaBuilder;

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
     * @return SnapshotMeta corresponding to the given log index.
     */
    public static SnapshotMeta snapshotMetaAt(long logIndex, long term, RaftGroupConfiguration config, int requiredCatalogVersion) {
        SnapshotMetaBuilder metaBuilder = new RaftMessagesFactory().snapshotMeta()
                .lastIncludedIndex(logIndex)
                .lastIncludedTerm(term)
                .peersList(config.peers())
                .learnersList(config.learners())
                .requiredCatalogVersion(requiredCatalogVersion);

        if (!config.isStable()) {
            //noinspection ConstantConditions
            metaBuilder
                    .oldPeersList(config.oldPeers())
                    .oldLearnersList(config.oldLearners());
        }

        return metaBuilder.build();
    }
}
