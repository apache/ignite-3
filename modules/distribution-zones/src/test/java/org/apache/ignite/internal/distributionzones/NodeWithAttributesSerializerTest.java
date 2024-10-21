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

package org.apache.ignite.internal.distributionzones;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class NodeWithAttributesSerializerTest {
    private final NodeWithAttributesSerializer serializer = new NodeWithAttributesSerializer();

    @Test
    void serializationAndDeserialization() {
        NodeWithAttributes originalNode = new NodeWithAttributes(
                "test",
                new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L),
                Map.of("a1", "v1", "a2", "v2"),
                List.of("prof1", "prof2")
        );

        byte[] bytes = VersionedSerialization.toBytes(originalNode, serializer);
        NodeWithAttributes restoredNode = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredNode.nodeId(), is(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredNode.nodeName(), is("test"));
        assertThat(restoredNode.userAttributes(), equalTo(Map.of("a1", "v1", "a2", "v2")));
        assertThat(restoredNode.storageProfiles(), contains("prof1", "prof2"));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwHvvkMFdGVzdO/Nq5B4VjQSIUNlhwm63P4DA2ExA3YxA2EyA3YyAwZwcm9mMQZwcm9mMg==");
        NodeWithAttributes restoredNode = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredNode.nodeId(), is(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredNode.nodeName(), is("test"));
        assertThat(restoredNode.userAttributes(), equalTo(Map.of("a1", "v1", "a2", "v2")));
        assertThat(restoredNode.storageProfiles(), contains("prof1", "prof2"));
    }
}
