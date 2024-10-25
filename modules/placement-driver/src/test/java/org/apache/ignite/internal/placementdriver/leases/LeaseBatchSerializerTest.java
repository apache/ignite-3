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

package org.apache.ignite.internal.placementdriver.leases;

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class LeaseBatchSerializerTest {
    private static final UUID NODE1_ID = new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L);
    private static final UUID NODE2_ID = new UUID(0xFEDCBA0987654321L, 0x1234567890ABCDEFL);

    private static final long STANDARD_LEASE_DURATION_MS = 5000;

    private static final String SERIALIZED_WITH_V1 = "Ae++Q4mPyJLMMQEBAwZub2RlMQZub2RlMgPvzauQeFY0EiFDZYcJutz+ASFDZYcJutz+782rkHhWNBICA"
            + "gIDBwmJJwEIAmWJJwE=";

    private final LeaseBatchSerializer serializer = new LeaseBatchSerializer();

    @Test
    void emptyBatch() {
        LeaseBatch originalBatch = new LeaseBatch(emptyList());

        verifySerializationAndDeserializationGivesSameResult(originalBatch);
    }

    private void verifySerializationAndDeserializationGivesSameResult(LeaseBatch originalBatch) {
        byte[] bytes = VersionedSerialization.toBytes(originalBatch, serializer);
        LeaseBatch restoredBatch = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredBatch.leases(), equalTo(originalBatch.leases()));
        assertEquals(
                originalBatch.leases().stream().map(Lease::proposedCandidate).collect(toList()),
                restoredBatch.leases().stream().map(Lease::proposedCandidate).collect(toList())
        );
    }

    @Test
    void batchWithTablePartitionsOnly() {
        HybridTimestamp baseTs = baseTs();

        List<Lease> originalLeases = List.of(
                new Lease("node1", NODE1_ID, baseTs, expiration(baseTs), true, true, "node2", new TablePartitionId(1, 0)),
                new Lease(
                        "node2",
                        NODE2_ID,
                        baseTs.addPhysicalTime(100),
                        expiration(baseTs.addPhysicalTime(100)),
                        false,
                        false,
                        null,
                        new TablePartitionId(1, 1)
                ),
                new Lease("node1", NODE1_ID, baseTs, expiration(baseTs), true, true, "node2", new TablePartitionId(2, 0)),
                new Lease(
                        "node2",
                        NODE2_ID,
                        baseTs.addPhysicalTime(100),
                        expiration(baseTs.addPhysicalTime(100)),
                        false,
                        false,
                        null,
                        new TablePartitionId(2, 1)
                )
        );

        LeaseBatch originalBatch = new LeaseBatch(originalLeases);

        verifySerializationAndDeserializationGivesSameResult(originalBatch);
    }

    private static HybridTimestamp baseTs() {
        long physicalBase = LocalDateTime.of(2024, Month.JANUARY, 1, 0, 0)
                .atOffset(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        return new HybridTimestamp(physicalBase, 0);
    }

    private static HybridTimestamp expiration(HybridTimestamp startTs) {
        return expiration(startTs, STANDARD_LEASE_DURATION_MS);
    }

    private static HybridTimestamp expiration(HybridTimestamp startTs, long interval) {
        return new HybridTimestamp(startTs.getPhysical() + interval, 0);
    }

    @Test
    void batchWithTablePartitionsOnlyWithNulls() {
        HybridTimestamp baseTs = baseTs();

        List<Lease> originalLeases = List.of(
                new Lease("node1", NODE1_ID, baseTs, expiration(baseTs), true, true, null, new TablePartitionId(1, 0))
        );
        LeaseBatch originalBatch = new LeaseBatch(originalLeases);

        verifySerializationAndDeserializationGivesSameResult(originalBatch);
    }

    @Test
    void batchWithTablePartitionsOnlyWithUncommonPeriod() {
        HybridTimestamp baseTs = baseTs();

        List<Lease> originalLeases = List.of(
                new Lease("node1", NODE1_ID, baseTs, expiration(baseTs), true, true, null, new TablePartitionId(1, 0)),
                new Lease("node1", NODE1_ID, baseTs, expiration(baseTs), true, true, null, new TablePartitionId(1, 1)),
                // This lease has an uncommon period (1000 instead of 5000).
                new Lease("node1", NODE1_ID, baseTs, expiration(baseTs, 1000), true, true, null, new TablePartitionId(1, 2))
        );
        LeaseBatch originalBatch = new LeaseBatch(originalLeases);

        verifySerializationAndDeserializationGivesSameResult(originalBatch);
    }

    @Test
    void batchWithExpirationTimeWithLogicalPart() {
        HybridTimestamp baseTs = baseTs();

        List<Lease> originalLeases = List.of(
                new Lease("node1", NODE1_ID, baseTs, expiration(baseTs).tick(), true, true, "node2", new TablePartitionId(1, 0))
        );

        LeaseBatch originalBatch = new LeaseBatch(originalLeases);

        verifySerializationAndDeserializationGivesSameResult(originalBatch);
    }

    @Test
    void batchWithExactly8NodeNames() {
        List<Lease> originalLeases = IntStream.range(0, 8)
                .mapToObj(n -> {
                    String nodeName = "node" + n;
                    return tableLease(nodeName, randomUUID(), nodeName, n);
                })
                .collect(toList());
        LeaseBatch originalBatch = new LeaseBatch(originalLeases);

        verifySerializationAndDeserializationGivesSameResult(originalBatch);
    }

    @Test
    void batchWithMoreThan8NodeNames() {
        List<Lease> originalLeases = IntStream.range(0, 9)
                .mapToObj(n -> tableLease("node" + n, randomUUID(), "candidate" + n, n))
                .collect(toList());
        LeaseBatch originalBatch = new LeaseBatch(originalLeases);

        verifySerializationAndDeserializationGivesSameResult(originalBatch);
    }

    private static Lease tableLease(String holderName, UUID holderId, String proposedCandidate, int partitionId) {
        TablePartitionId groupId = new TablePartitionId(1, partitionId);
        return new Lease(holderName, holderId, baseTs(), expiration(baseTs()), true, true, proposedCandidate, groupId);
    }

    @Test
    void batchWithHoleInTable() {
        List<Lease> originalLeases = IntStream.of(0, 2)
                .mapToObj(n -> {
                    String nodeName = "node" + n;
                    return tableLease(nodeName, randomUUID(), nodeName, n);
                })
                .collect(toList());
        LeaseBatch originalBatch = new LeaseBatch(originalLeases);

        verifySerializationAndDeserializationGivesSameResult(originalBatch);
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(SERIALIZED_WITH_V1);
        LeaseBatch restoredBatch = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredBatch.leases(), hasSize(2));
        Iterator<Lease> iterator = restoredBatch.leases().iterator();

        Lease lease1 = iterator.next();
        assertThat(
                lease1,
                equalTo(new Lease("node1", NODE1_ID, baseTs(), expiration(baseTs()), true, true, "node2", new TablePartitionId(1, 0)))
        );
        assertThat(lease1.proposedCandidate(), is("node2"));

        Lease lease2 = iterator.next();
        assertThat(
                lease2,
                equalTo(new Lease(
                        "node2",
                        NODE2_ID,
                        baseTs().addPhysicalTime(100),
                        expiration(baseTs().addPhysicalTime(100)),
                        false,
                        false,
                        null,
                        new TablePartitionId(1, 1)
                ))
        );
        assertThat(lease2.proposedCandidate(), is(nullValue()));
    }

    @SuppressWarnings("unused")
    private String v1LeaseBatchAsBase64() {
        HybridTimestamp baseTs = baseTs();

        List<Lease> originalLeases = List.of(
                new Lease("node1", NODE1_ID, baseTs, expiration(baseTs), true, true, "node2", new TablePartitionId(1, 0)),
                new Lease(
                        "node2",
                        NODE2_ID,
                        baseTs.addPhysicalTime(100),
                        expiration(baseTs.addPhysicalTime(100)),
                        false,
                        false,
                        null,
                        new TablePartitionId(1, 1)
                )
        );

        LeaseBatch originalBatch = new LeaseBatch(originalLeases);

        byte[] originalBytes = VersionedSerialization.toBytes(originalBatch, serializer);
        return Base64.getEncoder().encodeToString(originalBytes);
    }
}
