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

package org.apache.ignite.internal.cluster.management;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class ClusterStatePersistentSerializerTest {
    private static final CmgMessagesFactory CMG_MESSAGES_FACTORY = new CmgMessagesFactory();

    private final ClusterStatePersistentSerializer serializer = new ClusterStatePersistentSerializer();

    @Test
    void serializationAndDeserializationWithoutNulls() {
        ClusterState originalState = CMG_MESSAGES_FACTORY.clusterState()
                .cmgNodes(Set.of("a", "b"))
                .metaStorageNodes(Set.of("c", "d"))
                .version("3.0.0")
                .clusterTag(ClusterTag.clusterTag(CMG_MESSAGES_FACTORY, "cluster", new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)))
                .initialClusterConfiguration("config")
                .formerClusterIds(List.of(
                        new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L),
                        new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL)
                ))
                .build();

        byte[] bytes = VersionedSerialization.toBytes(originalState, serializer);
        ClusterState restoredState = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredState, is(equalTo(originalState)));
    }

    @Test
    void serializationAndDeserializationWithNulls() {
        ClusterState originalState = CMG_MESSAGES_FACTORY.clusterState()
                .cmgNodes(Set.of("a", "b"))
                .metaStorageNodes(Set.of("c", "d"))
                .version("3.0.0")
                .clusterTag(ClusterTag.clusterTag(CMG_MESSAGES_FACTORY, "cluster", new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)))
                .initialClusterConfiguration(null)
                .formerClusterIds(null)
                .build();

        byte[] bytes = VersionedSerialization.toBytes(originalState, serializer);
        ClusterState restoredState = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredState, is(equalTo(originalState)));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwMCYgJhAwJkAmMGMy4wLjAIY2x1c3Rlcu/Nq5B4VjQSIUNlhwm63P4HY29uZmlnA+/Nq5B4VhI0IUN"
                + "lhwm63P4hQ2WHCbrc/u/Nq5B4VjQS");
        ClusterState restoredState = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredState.cmgNodes(), containsInAnyOrder("a", "b"));
        assertThat(restoredState.metaStorageNodes(), containsInAnyOrder("c", "d"));
        assertThat(restoredState.version(), is("3.0.0"));
        assertThat(restoredState.clusterTag().clusterName(), is("cluster"));
        assertThat(restoredState.clusterTag().clusterId(), is(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredState.initialClusterConfiguration(), is("config"));
        assertThat(
                restoredState.formerClusterIds(),
                contains(new UUID(0x3412567890ABCDEFL, 0xFEDCBA0987654321L), new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL))
        );
    }
}
