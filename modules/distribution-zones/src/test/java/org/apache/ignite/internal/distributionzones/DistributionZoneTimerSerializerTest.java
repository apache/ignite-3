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
import static org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DistributionZoneTimerSerializer.deserialize;
import static org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DistributionZoneTimerSerializer.serialize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.distributionzones.DistributionZoneTimer.DistributionZoneTimerSerializer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DistributionZoneTimerSerializer}.
 */
public class DistributionZoneTimerSerializerTest {
    private static final UUID NODE_A_ID = new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    private static final UUID NODE_B_ID = new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL);

    private static final NodeWithAttributes A = new NodeWithAttributes("node_A", NODE_A_ID, emptyMap(), emptyList());
    private static final NodeWithAttributes B = new NodeWithAttributes(
            "node_B",
            NODE_B_ID,
            Map.of("region", "US"),
            List.of("default")
    );

    private static final DistributionZoneTimer TIMER = new DistributionZoneTimer(new HybridTimestamp(12341234L, 10), 10000, Set.of(A, B));
    private static final String SERIALIZED_V1 = "Ae++Q4uAyP/EF5FOAwHvvkMB775DB25vZGVfQe/Nq5B4VjQSIUNlhwm63P4BAQHvvkMB775DB25vZGVfQiFDZYc"
            + "Jutz+782rkHhWNBICB3JlZ2lvbgNVUwIIZGVmYXVsdA==";

    @Test
    public void serializationAndDeserialization() {
        DistributionZoneTimer timer0 = new DistributionZoneTimer(new HybridTimestamp(1234L, 1), 60, Set.of(A));
        DistributionZoneTimer timer1 = new DistributionZoneTimer(new HybridTimestamp(12341234L, 10), 10000, Set.of(A, B));

        DistributionZoneTimer deserializedTimer0 = deserialize(serialize(timer0));
        assertEquals(timer0, deserializedTimer0);

        DistributionZoneTimer deserializedTimer1 = deserialize(serialize(timer1));
        assertEquals(timer1, deserializedTimer1);
    }

    @Test
    public void v1CanBeDeserialized() {
        byte[] serializedBytes = Base64.getDecoder().decode(SERIALIZED_V1);
        DistributionZoneTimer deserializedTimer = deserialize(serializedBytes);
        assertEquals(TIMER, deserializedTimer);
    }
}
