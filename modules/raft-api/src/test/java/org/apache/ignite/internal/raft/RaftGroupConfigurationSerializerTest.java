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

package org.apache.ignite.internal.raft;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.List;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class RaftGroupConfigurationSerializerTest {
    private final RaftGroupConfigurationSerializer serializer = new RaftGroupConfigurationSerializer();

    @Test
    void serializationAndDeserializationWithoutNulls() {
        RaftGroupConfiguration originalConfig = new RaftGroupConfiguration(
                13L,
                37L,
                11L,
                10L,
                List.of("peer1", "peer2"),
                List.of("learner1", "learner2"),
                List.of("old-peer1", "old-peer2"),
                List.of("old-learner1", "old-learner2")
        );

        byte[] bytes = VersionedSerialization.toBytes(originalConfig, serializer);
        RaftGroupConfiguration restoredConfig = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredConfig, equalTo(originalConfig));
    }

    @Test
    void serializationAndDeserializationWithNulls() {
        RaftGroupConfiguration originalConfig = new RaftGroupConfiguration(
                13L,
                37L,
                11L,
                10L,
                List.of("peer1", "peer2"),
                List.of("learner1", "learner2"),
                null,
                null
        );

        byte[] bytes = VersionedSerialization.toBytes(originalConfig, serializer);
        RaftGroupConfiguration restoredConfig = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredConfig, equalTo(originalConfig));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwMGcGVlcjEGcGVlcjIDCWxlYXJuZXIxCWxlYXJuZXIyAwpvbGQtcGVlcjEKb2xkLXBlZXIyAw1vbGQ"
                + "tbGVhcm5lcjENb2xkLWxlYXJuZXIy");
        RaftGroupConfiguration restoredConfig = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredConfig.peers(), is(List.of("peer1", "peer2")));
        assertThat(restoredConfig.learners(), is(List.of("learner1", "learner2")));
        assertThat(restoredConfig.oldPeers(), is(List.of("old-peer1", "old-peer2")));
        assertThat(restoredConfig.oldLearners(), is(List.of("old-learner1", "old-learner2")));
        assertThat(restoredConfig.sequenceToken(), is(0L));
        assertThat(restoredConfig.oldSequenceToken(), is(0L));
    }

    @Test
    void v2CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Au++Qw0AAAAAAAAAJQAAAAAAAAADBnBlZXIxBnBlZXIyAwlsZWFybmVyMQlsZWFybmVyMgMKb2x"
                + "kLXBlZXIxCm9sZC1wZWVyMgMNb2xkLWxlYXJuZXIxDW9sZC1sZWFybmVyMg==");

        RaftGroupConfiguration restoredConfig = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredConfig.index(), is(13L));
        assertThat(restoredConfig.term(), is(37L));
        assertThat(restoredConfig.peers(), is(List.of("peer1", "peer2")));
        assertThat(restoredConfig.learners(), is(List.of("learner1", "learner2")));
        assertThat(restoredConfig.oldPeers(), is(List.of("old-peer1", "old-peer2")));
        assertThat(restoredConfig.oldLearners(), is(List.of("old-learner1", "old-learner2")));
        assertThat(restoredConfig.sequenceToken(), is(0L));
        assertThat(restoredConfig.oldSequenceToken(), is(0L));
    }

    @Test
    void v3CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("A+++Qw0AAAAAAAAAJQAAAAAAAAADBnBlZXIxBnBlZXIyAwlsZWFybmVyMQ"
                + "lsZWFybmVyMgMKb2xkLXBlZXIxCm9sZC1wZWVyMgMNb2xkLWxlYXJuZXIxDW9sZC1sZWFybmVyMgwL");

        RaftGroupConfiguration restoredConfig = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredConfig.index(), is(13L));
        assertThat(restoredConfig.term(), is(37L));
        assertThat(restoredConfig.peers(), is(List.of("peer1", "peer2")));
        assertThat(restoredConfig.learners(), is(List.of("learner1", "learner2")));
        assertThat(restoredConfig.oldPeers(), is(List.of("old-peer1", "old-peer2")));
        assertThat(restoredConfig.oldLearners(), is(List.of("old-learner1", "old-learner2")));
        assertThat(restoredConfig.sequenceToken(), is(11L));
        assertThat(restoredConfig.oldSequenceToken(), is(10L));
    }
}
