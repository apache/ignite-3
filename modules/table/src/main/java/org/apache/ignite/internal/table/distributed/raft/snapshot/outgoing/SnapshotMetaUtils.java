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

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.entity.SnapshotMetaBuilder;
import org.apache.ignite.raft.jraft.storage.LogManager;

/**
 * Utils to build {@link SnapshotMeta} instances.
 */
public class SnapshotMetaUtils {
    /**
     * Builds a {@link SnapshotMeta} corresponding to RAFT state (term, configuration) at the given log index.
     *
     * @param logIndex RAFT log index.
     * @param logManager LogManager from which to load term and configuration.
     * @return SnapshotMeta corresponding to the given log index.
     */
    public static SnapshotMeta snapshotMetaAt(long logIndex, LogManager logManager) {
        ConfigurationEntry configEntry = logManager.getConfiguration(logIndex);

        SnapshotMetaBuilder metaBuilder = new RaftMessagesFactory().snapshotMeta()
                .lastIncludedIndex(logIndex)
                .lastIncludedTerm(logManager.getTerm(logIndex))
                .peersList(peersToStrings(configEntry.getConf().listPeers()))
                .learnersList(peersToStrings(configEntry.getConf().listLearners()));

        if (configEntry.getOldConf() != null) {
            metaBuilder
                    .oldPeersList(peersToStrings(configEntry.getOldConf().listPeers()))
                    .oldLearnersList(peersToStrings(configEntry.getOldConf().listLearners()));
        }

        return metaBuilder.build();
    }

    private static Collection<String> peersToStrings(List<PeerId> peers) {
        return peers.stream().map(PeerId::toString).collect(toList());
    }
}
