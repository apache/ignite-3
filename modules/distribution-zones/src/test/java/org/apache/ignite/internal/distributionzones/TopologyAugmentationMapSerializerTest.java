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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Augmentation;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class TopologyAugmentationMapSerializerTest {
    private static final UUID NODE1_ID = new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    private static final UUID NODE2_ID = new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL);

    private final TopologyAugmentationMapSerializer serializer = new TopologyAugmentationMapSerializer();

    @Test
    void serializationAndDeserialization() {
        Node node1 = new Node("node1", NODE1_ID);
        Node node2 = new Node("node2", NODE2_ID);
        ConcurrentSkipListMap<Long, Augmentation> originalMap = new ConcurrentSkipListMap<>(Map.of(
                1000L, new Augmentation(Set.of(node1, node2), true),
                2000L, new Augmentation(Set.of(node2, node1), false)
        ));

        byte[] bytes = VersionedSerialization.toBytes(originalMap, serializer);
        ConcurrentSkipListMap<Long, Augmentation> restoredMap = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMap, is(aMapWithSize(2)));

        Augmentation augmentation1 = restoredMap.get(1000L);
        assertThat(augmentation1.nodes(), containsInAnyOrder(node1, node2));
        assertThat(augmentation1.addition(), is(true));

        Augmentation augmentation2 = restoredMap.get(2000L);
        assertThat(augmentation2.nodes(), containsInAnyOrder(node1, node2));
        assertThat(augmentation2.addition(), is(false));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwPpBwHvvkMDAe++QwZub2RlMe/Nq5B4VjQSIUNlhwm63P4B775DBm5vZGUyIUNlhwm63P7vzauQeFY0"
                + "EgHRDwHvvkMDAe++QwZub2RlMiFDZYcJutz+782rkHhWNBIB775DBm5vZGUx782rkHhWNBIhQ2WHCbrc/gA=");
        ConcurrentSkipListMap<Long, Augmentation> restoredMap = VersionedSerialization.fromBytes(bytes, serializer);

        Node node1 = new Node("node1", NODE1_ID);
        Node node2 = new Node("node2", NODE2_ID);

        assertThat(restoredMap, is(aMapWithSize(2)));

        Augmentation augmentation1 = restoredMap.get(1000L);
        assertThat(augmentation1.nodes(), containsInAnyOrder(node1, node2));
        assertThat(augmentation1.addition(), is(true));

        Augmentation augmentation2 = restoredMap.get(2000L);
        assertThat(augmentation2.nodes(), containsInAnyOrder(node1, node2));
        assertThat(augmentation2.addition(), is(false));
    }
}
