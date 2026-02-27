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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.junit.jupiter.api.Test;

class LogicalTopologySnapshotSerializerTest {
    private final LogicalTopologySnapshotSerializer serializer = new LogicalTopologySnapshotSerializer();

    @Test
    void serializationAndDeserialization() {
        LogicalTopologySnapshot originalSnapshot = new LogicalTopologySnapshot(123L, List.of(node(0), node(1)), new UUID(1, 2));

        byte[] bytes = VersionedSerialization.toBytes(originalSnapshot, serializer);
        LogicalTopologySnapshot restoredSnapshot = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredSnapshot.version(), is(originalSnapshot.version()));
        assertThat(restoredSnapshot.nodes(), containsInAnyOrder(originalSnapshot.nodes().toArray()));
        assertThat(restoredSnapshot.clusterId(), equalTo(originalSnapshot.clusterId()));
    }

    private static LogicalNode node(int index) {
        return new LogicalNode(
                new UUID(0xDEADBEEFCAFEBABEL, index),
                "node" + index,
                new NetworkAddress("host" + index, 3000 + index),
                new NodeMetadata("rest-host" + index, 80 + index, 443 + index),
                Map.of("ukey", "uval" + index),
                Map.of("skey", "sval" + index),
                List.of("prof1", "prof2")
        );
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++Q3wDAe++QwHvvkO+uv7K776t3gAAAAAAAAAABm5vZGUwBmhvc3QwuRcBC3Jlc3QtaG9zdDBRvAMCBXV"
                + "rZXkGdXZhbDACBXNrZXkGc3ZhbDADBnByb2YxBnByb2YyAe++QwHvvkO+uv7K776t3gEAAAAAAAAABm5vZGUxBmhvc3QxuhcBC3Jlc3QtaG9zdDFSvQ"
                + "MCBXVrZXkGdXZhbDECBXNrZXkGc3ZhbDEDBnByb2YxBnByb2YyAQAAAAAAAAACAAAAAAAAAA==");
        LogicalTopologySnapshot restoredSnapshot = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredSnapshot.version(), is(123L));
        assertThat(restoredSnapshot.nodes(), hasSize(2));

        List<LogicalNode> orderedNodes = restoredSnapshot.nodes().stream()
                .sorted(Comparator.comparing(LogicalNode::id))
                .collect(toList());

        LogicalNode node0 = orderedNodes.get(0);
        assertThat(node0.id(), is(new UUID(0xDEADBEEFCAFEBABEL, 0)));
        assertThat(node0.name(), is("node0"));
        assertThat(node0.address(), is(new NetworkAddress("host0", 3000)));
        assertThat(node0.nodeMetadata(), is(new NodeMetadata("rest-host0", 80, 443)));
        assertThat(node0.userAttributes(), is(Map.of("ukey", "uval0")));
        assertThat(node0.systemAttributes(), is(Map.of("skey", "sval0")));
        assertThat(node0.storageProfiles(), is(List.of("prof1", "prof2")));

        LogicalNode node1 = orderedNodes.get(1);
        assertThat(node1.id(), is(new UUID(0xDEADBEEFCAFEBABEL, 1)));
        assertThat(node1.name(), is("node1"));
        assertThat(node1.address(), is(new NetworkAddress("host1", 3001)));
        assertThat(node1.nodeMetadata(), is(new NodeMetadata("rest-host1", 81, 444)));
        assertThat(node1.userAttributes(), is(Map.of("ukey", "uval1")));
        assertThat(node1.systemAttributes(), is(Map.of("skey", "sval1")));
        assertThat(node1.storageProfiles(), is(List.of("prof1", "prof2")));

        assertThat(restoredSnapshot.clusterId(), equalTo(new UUID(1, 2)));
    }
}
