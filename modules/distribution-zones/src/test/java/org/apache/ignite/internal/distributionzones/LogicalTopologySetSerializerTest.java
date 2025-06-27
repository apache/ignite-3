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

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class LogicalTopologySetSerializerTest {
    private static final UUID NODE1_ID = new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    private static final UUID NODE2_ID = new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL);

    private final LogicalTopologySetSerializer serializer = new LogicalTopologySetSerializer();

    @Test
    void serializationAndDeserialization() {
        Set<NodeWithAttributes> originalNodes = Set.of(
                new NodeWithAttributes(
                    "node1",
                    new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L),
                    Map.of("a1", "v1"),
                    List.of("prof1")
                ),
                new NodeWithAttributes(
                    "node2",
                    new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL),
                    Map.of("a2", "v2"),
                    List.of("prof2")
                )
        );

        byte[] bytes = VersionedSerialization.toBytes(originalNodes, serializer);
        Set<NodeWithAttributes> restoredNodes = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredNodes, equalTo(originalNodes));
        assertEquals(
                originalNodes.stream().map(NodeWithAttributes::nodeId).collect(toSet()),
                restoredNodes.stream().map(NodeWithAttributes::nodeId).collect(toSet())
        );
    }

    @Test
    void v1CanBeSerialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwMB775DAe++QwZub2RlMe/Nq5B4VjQSIUNlhwm63P4CA2ExA3YxAgZwcm9mMQHvvkMB775DBm5vZGUy"
                + "IUNlhwm63P7vzauQeFY0EgIDYTIDdjICBnByb2Yy");
        Set<NodeWithAttributes> restoredNodes = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredNodes, hasSize(2));

        NodeWithAttributes node1 = restoredNodes.stream()
                .filter(n -> n.nodeId().equals(NODE1_ID))
                .findAny().orElseThrow();
        assertThat(node1.nodeId(), is(NODE1_ID));
        assertThat(node1.nodeName(), is("node1"));
        assertThat(node1.userAttributes(), equalTo(Map.of("a1", "v1")));
        assertThat(node1.storageProfiles(), contains("prof1"));

        NodeWithAttributes node2 = restoredNodes.stream()
                .filter(n -> n.nodeId().equals(NODE2_ID))
                .findAny().orElseThrow();
        assertThat(node2.nodeId(), is(NODE2_ID));
        assertThat(node2.nodeName(), is("node2"));
        assertThat(node2.userAttributes(), equalTo(Map.of("a2", "v2")));
        assertThat(node2.storageProfiles(), contains("prof2"));
    }
}
