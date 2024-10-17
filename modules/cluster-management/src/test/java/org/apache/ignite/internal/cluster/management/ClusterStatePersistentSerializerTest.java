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

import static java.util.UUID.randomUUID;
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
                .clusterTag(ClusterTag.randomClusterTag(CMG_MESSAGES_FACTORY, "cluster"))
                .initialClusterConfiguration("config")
                .formerClusterIds(List.of(randomUUID(), randomUUID()))
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
                .clusterTag(ClusterTag.randomClusterTag(CMG_MESSAGES_FACTORY, "cluster"))
                .initialClusterConfiguration(null)
                .formerClusterIds(null)
                .build();

        byte[] bytes = VersionedSerialization.toBytes(originalState, serializer);
        ClusterState restoredState = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredState, is(equalTo(originalState)));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwMBYQFiAwFjAWQFMy4wLjAHY2x1c3Rlcp1Ct7dR35ELuoeboFbabrgHY29uZmlnAztFBahaoEfJtxGam"
                + "Q6WXJNFRfruL76Bv254dP54iF6V");
        ClusterState restoredState = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredState.cmgNodes(), containsInAnyOrder("a", "b"));
        assertThat(restoredState.metaStorageNodes(), containsInAnyOrder("c", "d"));
        assertThat(restoredState.version(), is("3.0.0"));
        assertThat(restoredState.initialClusterConfiguration(), is("config"));
        assertThat(
                restoredState.formerClusterIds(),
                contains(UUID.fromString("c947a05a-a805-453b-935c-960e999a11b7"), UUID.fromString("bf81be2f-eefa-4545-955e-8878fe74786e"))
        );
    }
}
