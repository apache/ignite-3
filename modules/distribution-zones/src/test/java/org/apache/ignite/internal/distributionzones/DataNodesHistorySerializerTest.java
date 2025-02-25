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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DataNodesHistorySerializer}.
 */
public class DataNodesHistorySerializerTest {
    private static final UUID NODE_A_ID = new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    private static final UUID NODE_B_ID = new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL);

    private static final NodeWithAttributes A = new NodeWithAttributes("node_A", NODE_A_ID, emptyMap(), emptyList());
    private static final NodeWithAttributes B = new NodeWithAttributes(
            "node_B",
            NODE_B_ID,
            Map.of("region", "US"),
            List.of("default")
    );

    private static final HybridTimestamp TIMESTAMP_0 = new HybridTimestamp(1234, 10);
    private static final HybridTimestamp TIMESTAMP_1 = new HybridTimestamp(12341234, 20);

    private static final String SERIALIZED_V1 = "Ae++QwMKANIEAAAAAAIB775DAe++Qwdub2RlX0HvzauQeFY0EiFDZYcJutz+AQEUAPJPvAAAAAMB775DAe++Qwd"
            + "ub2RlX0HvzauQeFY0EiFDZYcJutz+AQEB775DAe++Qwdub2RlX0IhQ2WHCbrc/u/Nq5B4VjQSAgdyZWdpb24DVVMCCGRlZmF1bHQ=";

    @Test
    public void serializationAndDeserializationOfEmpty() {
        DataNodesHistory history = new DataNodesHistory();
        byte[] bytes = DataNodesHistorySerializer.serialize(history);
        DataNodesHistory deserializedHistory = DataNodesHistorySerializer.deserialize(bytes);
        assertEquals(history, deserializedHistory);
        assertTrue(history.isEmpty());
    }

    @Test
    public void serializationAndDeserialization() {
        DataNodesHistory history = new DataNodesHistory();
        history = history.addHistoryEntry(TIMESTAMP_0, Set.of(A));
        history = history.addHistoryEntry(TIMESTAMP_1, Set.of(A, B));

        byte[] bytes = DataNodesHistorySerializer.serialize(history);
        DataNodesHistory deserializedHistory = DataNodesHistorySerializer.deserialize(bytes);
        assertEquals(history, deserializedHistory);

        assertTrue(deserializedHistory.entryIsPresentAtExactTimestamp(TIMESTAMP_0));
        assertTrue(deserializedHistory.entryIsPresentAtExactTimestamp(TIMESTAMP_1));

        assertEquals(new DataNodesHistoryEntry(TIMESTAMP_0, Set.of(A)), deserializedHistory.dataNodesForTimestamp(TIMESTAMP_0));
        assertEquals(new DataNodesHistoryEntry(TIMESTAMP_1, Set.of(A, B)), deserializedHistory.dataNodesForTimestamp(TIMESTAMP_1));
    }

    @Test
    public void v1CanBeDeserialized() {
        byte[] serializedBytes = Base64.getDecoder().decode(SERIALIZED_V1);
        DataNodesHistory deserializedHistory = DataNodesHistorySerializer.deserialize(serializedBytes);
        assertTrue(deserializedHistory.entryIsPresentAtExactTimestamp(TIMESTAMP_0));
        assertTrue(deserializedHistory.entryIsPresentAtExactTimestamp(TIMESTAMP_1));

        assertEquals(new DataNodesHistoryEntry(TIMESTAMP_0, Set.of(A)), deserializedHistory.dataNodesForTimestamp(TIMESTAMP_0));
        assertEquals(new DataNodesHistoryEntry(TIMESTAMP_1, Set.of(A, B)), deserializedHistory.dataNodesForTimestamp(TIMESTAMP_1));
    }
}
