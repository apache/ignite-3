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

package org.apache.ignite.internal.cluster.management.topology.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.junit.jupiter.api.Test;

class LogicalNodeSerializerTest {
    private final LogicalNodeSerializer serializer = new LogicalNodeSerializer();

    @Test
    void serializationAndDeserialization() {
        LogicalNode originalNode = new LogicalNode(
                new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L),
                "test",
                new NetworkAddress("host", 3000),
                "9.9.9-test",
                new NodeMetadata("ext-host", 3001, 3002),
                Map.of("ukey", "uval"),
                Map.of("skey", "sval"),
                List.of("profile")
        );

        byte[] bytes = VersionedSerialization.toBytes(originalNode, serializer);
        LogicalNode restoredNode = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredNode.id(), equalTo(originalNode.id()));
        assertThat(restoredNode.name(), equalTo("test"));
        assertThat(restoredNode.address(), equalTo(new NetworkAddress("host", 3000)));
        assertThat(restoredNode.version(), equalTo("9.9.9-test"));
        assertThat(restoredNode.nodeMetadata(), equalTo(new NodeMetadata("ext-host", 3001, 3002)));
        assertThat(restoredNode.userAttributes(), equalTo(Map.of("ukey", "uval")));
        assertThat(restoredNode.systemAttributes(), equalTo(Map.of("skey", "sval")));
        assertThat(restoredNode.storageProfiles(), equalTo(List.of("profile")));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwHvvkPvzauQeFY0EiFDZYcJutz+BXRlc3QFaG9zdLkXAQlleHQtaG9zdLoXuxcCBXVrZXkFdXZhbAI"
                + "Fc2tleQVzdmFsAghwcm9maWxl");

        LogicalNode restoredNode = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredNode.id(), is(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredNode.name(), equalTo("test"));
        assertThat(restoredNode.address(), equalTo(new NetworkAddress("host", 3000)));
        assertThat(restoredNode.nodeMetadata(), equalTo(new NodeMetadata("ext-host", 3001, 3002)));
        assertThat(restoredNode.userAttributes(), equalTo(Map.of("ukey", "uval")));
        assertThat(restoredNode.systemAttributes(), equalTo(Map.of("skey", "sval")));
        assertThat(restoredNode.storageProfiles(), equalTo(List.of("profile")));
    }
}
