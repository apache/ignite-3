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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Tests for lease encoding and decoding from byte arrays. */
public class LeaseSerializationTest {
    @Test
    public void testLeaseSerialization() {
        long now = System.currentTimeMillis();
        ReplicationGroupId groupId = new TablePartitionId(1, 1);

        checksSerialization(Lease.emptyLease(groupId));

        checksSerialization(newLease("node1", timestamp(now, 1), timestamp(now + 1_000_000, 100), true, true, null, groupId));

        checksSerialization(newLease("node1", timestamp(now, 1), timestamp(now + 1_000_000, 100), false, false, "node2", groupId));

        checksSerialization(newLease("node1", timestamp(now, 1), timestamp(now + 1_000_000, 100), false, true, "node2", groupId));

        checksSerialization(newLease("node1", timestamp(now, 1), timestamp(now + 1_000_000, 100), true, false, null, groupId));

        checksSerialization(newLease(null, timestamp(1, 1), timestamp(2 + 1_000_000, 100), true, true, null, groupId));

        checksSerialization(newLease("node" + new String(new byte[1000]), timestamp(1, 1), timestamp(2, 100), false, false, null, groupId));
    }

    @Test
    public void testLeaseBatchSerialization() {
        var leases = new ArrayList<Lease>();

        ReplicationGroupId groupId = new TablePartitionId(1, 1);

        for (int i = 0; i < 25; i++) {
            leases.add(newLease(
                    "node" + i,
                    timestamp(1, i),
                    timestamp(1, i + 1),
                    i % 2 == 0,
                    i % 2 == 1,
                    i % 2 == 0 ? null : "node" + i,
                    groupId
            ));
        }

        byte[] leaseBatchBytes = new LeaseBatch(leases).bytes();

        assertEquals(leases, LeaseBatch.fromBytes(wrap(leaseBatchBytes)).leases());
    }

    private static void checksSerialization(Lease lease) {
        assertEquals(lease, Lease.fromBytes(wrap(lease.bytes())));
    }

    private static Lease newLease(
            @Nullable String leaseholder,
            HybridTimestamp startTime,
            HybridTimestamp expirationTime,
            boolean prolong,
            boolean accepted,
            @Nullable String proposedCandidate,
            ReplicationGroupId replicationGroupId
    ) {
        return new Lease(
                leaseholder,
                leaseholder == null ? null : leaseholder + "_id",
                startTime,
                expirationTime,
                prolong,
                accepted,
                proposedCandidate,
                replicationGroupId,
                Set.of(1)
        );
    }

    private static HybridTimestamp timestamp(long physical, int logical) {
        return new HybridTimestamp(physical, logical);
    }

    private static ByteBuffer wrap(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
    }
}
