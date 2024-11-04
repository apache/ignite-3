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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class DataNodesMapSerializerTest {
    private final DataNodesMapSerializer serializer = new DataNodesMapSerializer();

    @Test
    void serializationAndDeserialization() {
        Map<Node, Integer> originalMap = Map.of(
                new Node("a", new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)), 1000,
                new Node("b", new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL)), 2000
        );

        byte[] bytes = VersionedSerialization.toBytes(originalMap, serializer);
        Map<Node, Integer> restoredMap = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMap, equalTo(originalMap));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode("Ae++QwMB775DAmHvzauQeFY0EiFDZYcJutz+6QcB775DAmIhQ2WHCbrc/u/Nq5B4VjQS0Q8=");
        Map<Node, Integer> restoredMap = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredMap, is(aMapWithSize(2)));
        assertThat(restoredMap, hasEntry(new Node("a", new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)), 1000));
        assertThat(restoredMap, hasEntry(new Node("b", new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL)), 2000));
    }
}
