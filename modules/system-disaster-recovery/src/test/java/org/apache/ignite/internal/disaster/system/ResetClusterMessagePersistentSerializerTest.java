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

package org.apache.ignite.internal.disaster.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class ResetClusterMessagePersistentSerializerTest {
    private static final SystemDisasterRecoveryMessagesFactory MESSAGES_FACTORY = new SystemDisasterRecoveryMessagesFactory();

    private final ResetClusterMessagePersistentSerializer serializer = new ResetClusterMessagePersistentSerializer();

    @Test
    void serializationAndDeserializationWithoutNulls() {
        ResetClusterMessage originalMessage = MESSAGES_FACTORY.resetClusterMessage()
                .newCmgNodes(Set.of("a", "b"))
                .currentMetaStorageNodes(Set.of("c", "d"))
                .clusterName("cluster")
                .clusterId(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L))
                .initialClusterConfiguration("config")
                .formerClusterIds(List.of(
                        new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL),
                        new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)
                ))
                .metastorageReplicationFactor(3)
                .conductor("a")
                .participatingNodes(Set.of("a", "b", "c"))
                .build();

        byte[] bytes = VersionedSerialization.toBytes(originalMessage, serializer);
        ResetClusterMessage restoredMessage = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMessage, equalTo(originalMessage));
    }

    @Test
    void serializationAndDeserializationWithNulls() {
        ResetClusterMessage originalMessage = MESSAGES_FACTORY.resetClusterMessage()
                .newCmgNodes(Set.of("a", "b"))
                .currentMetaStorageNodes(Set.of("c", "d"))
                .clusterName("cluster")
                .clusterId(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L))
                .initialClusterConfiguration(null)
                .formerClusterIds(List.of(
                        new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL),
                        new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)
                ))
                .metastorageReplicationFactor(null)
                .conductor(null)
                .participatingNodes(null)
                .build();

        byte[] bytes = VersionedSerialization.toBytes(originalMessage, serializer);
        ResetClusterMessage restoredMessage = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMessage, equalTo(originalMessage));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwMCYQJiAwJjAmQIY2x1c3Rlcu/Nq5B4VjQSIUNlhwm63P4HY29uZmlnAyFDZYcJutz+782rkHhWNBL"
                + "vzauQeFY0EiFDZYcJutz+BAJhBAJhAmMCYg==");
        ResetClusterMessage restoredMessage = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMessage.newCmgNodes(), equalTo(Set.of("a", "b")));
        assertThat(restoredMessage.currentMetaStorageNodes(), equalTo(Set.of("c", "d")));
        assertThat(restoredMessage.clusterName(), equalTo("cluster"));
        assertThat(restoredMessage.clusterId(), equalTo(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredMessage.initialClusterConfiguration(), is("config"));
        assertThat(restoredMessage.formerClusterIds(), equalTo(List.of(
                new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL),
                new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)
        )));
        assertThat(restoredMessage.metastorageReplicationFactor(), equalTo(3));
        assertThat(restoredMessage.conductor(), equalTo("a"));
        assertThat(restoredMessage.participatingNodes(), equalTo(Set.of("a", "b", "c")));
    }
}
