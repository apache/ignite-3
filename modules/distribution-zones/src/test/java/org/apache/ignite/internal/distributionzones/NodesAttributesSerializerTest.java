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
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class NodesAttributesSerializerTest {
    private static final UUID NODE1_ID = new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    private static final UUID NODE2_ID = new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL);

    private static final String SERIALIZED_WITH_V1 = "Ae++QwMB775DAe++QwZ0ZXN0Me/Nq5B4VjQSIUNlhwm63P4CA2ExA3YxAgZwcm9mMQHvvkMB775DBnRlc"
            + "3QyIUNlhwm63P7vzauQeFY0EgIDYTIDdjICBnByb2Yy";

    private final NodesAttributesSerializer serializer = new NodesAttributesSerializer();

    @Test
    void serializationAndDeserialization() {
        Map<UUID, NodeWithAttributes> originalMap = originalMap();

        byte[] bytes = VersionedSerialization.toBytes(originalMap, serializer);
        Map<UUID, NodeWithAttributes> restoredMap = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMap, equalTo(originalMap));
        // Also make sure that IDs are restored correctly as they are ignored by NodeWithAttributes#equals().
        for (Map.Entry<UUID, NodeWithAttributes> entry : originalMap.entrySet()) {
            assertThat(entry.getValue().nodeId(), equalTo(entry.getKey()));
        }
    }

    private static Map<UUID, NodeWithAttributes> originalMap() {
        NodeWithAttributes node1 = new NodeWithAttributes("test1", NODE1_ID, Map.of("a1", "v1"), List.of("prof1"));
        NodeWithAttributes node2 = new NodeWithAttributes("test2", NODE2_ID, Map.of("a2", "v2"), List.of("prof2"));

        return new ConcurrentHashMap<>(Map.of(
                node1.nodeId(), node1,
                node2.nodeId(), node2
        ));
    }

    @Test
    void deserializesAsConcurrentHashMap() {
        Map<UUID, NodeWithAttributes> originalMap = originalMap();

        byte[] bytes = VersionedSerialization.toBytes(originalMap, serializer);
        Map<UUID, NodeWithAttributes> restoredMap = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMap, isA(ConcurrentHashMap.class));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(SERIALIZED_WITH_V1);
        Map<UUID, NodeWithAttributes> restoredMap = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMap, aMapWithSize(2));

        NodeWithAttributes node1 = restoredMap.get(NODE1_ID);
        assertThat(node1.nodeId(), is(NODE1_ID));
        assertThat(node1.nodeName(), is("test1"));
        assertThat(node1.userAttributes(), equalTo(Map.of("a1", "v1")));
        assertThat(node1.storageProfiles(), contains("prof1"));

        NodeWithAttributes node2 = restoredMap.get(NODE2_ID);
        assertThat(node2.nodeId(), is(NODE2_ID));
        assertThat(node2.nodeName(), is("test2"));
        assertThat(node2.userAttributes(), equalTo(Map.of("a2", "v2")));
        assertThat(node2.storageProfiles(), contains("prof2"));
    }
}
