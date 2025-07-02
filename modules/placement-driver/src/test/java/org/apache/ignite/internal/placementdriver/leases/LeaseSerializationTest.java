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
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Tests for lease encoding and decoding from byte arrays. */
public class LeaseSerializationTest {
    @Test
    public void testLeaseBatchSerialization() {
        var leases = new ArrayList<Lease>();

        for (int i = 0; i < 25; i++) {
            leases.add(newLease(
                    "node" + i,
                    timestamp(1, i),
                    timestamp(1, i + 1),
                    i % 2 == 0,
                    i % 2 == 1,
                    i % 2 == 0 ? null : "node" + i,
                    new TablePartitionId(1, i)
            ));
        }

        byte[] leaseBatchBytes = new LeaseBatch(leases).bytes();

        assertEquals(leases, LeaseBatch.fromBytes(leaseBatchBytes).leases());
    }

    private static Lease newLease(
            String leaseholder,
            HybridTimestamp startTime,
            HybridTimestamp expirationTime,
            boolean prolong,
            boolean accepted,
            @Nullable String proposedCandidate,
            ReplicationGroupId replicationGroupId
    ) {
        return new Lease(
                leaseholder,
                randomUUID(),
                startTime,
                expirationTime,
                prolong,
                accepted,
                proposedCandidate,
                replicationGroupId
        );
    }

    private static HybridTimestamp timestamp(long physical, int logical) {
        return new HybridTimestamp(physical, logical);
    }

    private static ByteBuffer wrap(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
    }
}
