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

package org.apache.ignite.internal.network;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Base64;
import java.util.UUID;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.junit.jupiter.api.Test;

class InternalClusterNodeSerializerTest {
    private final ClusterNodeSerializer serializer = new ClusterNodeSerializer();

    @Test
    void serializationAndDeserialization() {
        InternalClusterNode originalNode = new ClusterNodeImpl(
                new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L),
                "test",
                new NetworkAddress("host", 3000),
                new NodeMetadata("ext-host", 3001, 3002)
        );

        byte[] bytes = VersionedSerialization.toBytes(originalNode, serializer);
        InternalClusterNode restoredNode = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredNode.id(), equalTo(originalNode.id()));
        assertThat(restoredNode.name(), equalTo("test"));
        assertThat(restoredNode.address(), equalTo(new NetworkAddress("host", 3000)));
        assertThat(restoredNode.nodeMetadata(), equalTo(new NodeMetadata("ext-host", 3001, 3002)));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++Q+/Nq5B4VjQSIUNlhwm63P4FdGVzdAVob3N0uRcBCWV4dC1ob3N0uhe7Fw==");

        InternalClusterNode restoredNode = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredNode.id(), equalTo(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredNode.name(), equalTo("test"));
        assertThat(restoredNode.address(), equalTo(new NetworkAddress("host", 3000)));
        assertThat(restoredNode.nodeMetadata(), equalTo(new NodeMetadata("ext-host", 3001, 3002)));
    }
}
