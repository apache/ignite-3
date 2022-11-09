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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SnapshotMetaUtilsTest {
    @Mock
    private LogManager logManager;

    @Test
    void buildsSnapshotMeta() {
        when(logManager.getTerm(100L)).thenReturn(3L);

        ConfigurationEntry configEntry = new ConfigurationEntry(new LogId(100, 3),
                configuration(List.of("peer1:3000", "peer2:3000"), List.of("learner1:3000", "learner2:3000")),
                configuration(List.of("peer1:3000"), List.of("learner1:3000"))
        );
        when(logManager.getConfiguration(100L)).thenReturn(configEntry);

        SnapshotMeta meta = SnapshotMetaUtils.snapshotMetaAt(100, logManager);

        assertThat(meta.lastIncludedIndex(), is(100L));
        assertThat(meta.lastIncludedTerm(), is(3L));
        assertThat(meta.peersList(), is(List.of("peer1:3000", "peer2:3000")));
        assertThat(meta.learnersList(), is(List.of("learner1:3000", "learner2:3000")));
        assertThat(meta.oldPeersList(), is(List.of("peer1:3000")));
        assertThat(meta.oldLearnersList(), is(List.of("learner1:3000")));
    }

    private static Configuration configuration(List<String> peers, List<String> learners) {
        return new Configuration(
                peers.stream().map(PeerId::parsePeer).collect(toList()),
                learners.stream().map(PeerId::parsePeer).collect(toList())
        );
    }

    @Test
    void doesNotIncludeOldConfigWhenItIsNotThere() {
        @SuppressWarnings("ConstantConditions")
        ConfigurationEntry configEntry = new ConfigurationEntry(new LogId(1, 1), new Configuration(), null);
        when(logManager.getConfiguration(anyLong())).thenReturn(configEntry);

        SnapshotMeta meta = SnapshotMetaUtils.snapshotMetaAt(100, logManager);

        assertThat(meta.oldPeersList(), is(nullValue()));
        assertThat(meta.oldLearnersList(), is(nullValue()));
    }
}
