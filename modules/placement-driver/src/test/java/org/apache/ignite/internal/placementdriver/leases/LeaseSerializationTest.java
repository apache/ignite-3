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

import static org.apache.ignite.internal.placementdriver.leases.Lease.fromBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.junit.jupiter.api.Test;

/**
 * Tests for lease encoding and decoding from byte arrays.
 */
public class LeaseSerializationTest {
    @Test
    public void test() {
        Lease lease;

        long now = System.currentTimeMillis();
        ReplicationGroupId groupId = new TablePartitionId(1, 1);

        lease = Lease.EMPTY_LEASE;
        assertEquals(lease, fromBytes(ByteBuffer.wrap(lease.bytes()).order(ByteOrder.LITTLE_ENDIAN)));

        lease = new Lease("node1", new HybridTimestamp(now, 1), new HybridTimestamp(now + 1_000_000, 100), true, true, groupId);
        assertEquals(lease, fromBytes(ByteBuffer.wrap(lease.bytes()).order(ByteOrder.LITTLE_ENDIAN)));

        lease = new Lease("node1", new HybridTimestamp(now, 1), new HybridTimestamp(now + 1_000_000, 100), false, false, groupId);
        assertEquals(lease, fromBytes(ByteBuffer.wrap(lease.bytes()).order(ByteOrder.LITTLE_ENDIAN)));

        lease = new Lease("node1", new HybridTimestamp(now, 1), new HybridTimestamp(now + 1_000_000, 100), false, true, groupId);
        assertEquals(lease, fromBytes(ByteBuffer.wrap(lease.bytes()).order(ByteOrder.LITTLE_ENDIAN)));

        lease = new Lease("node1", new HybridTimestamp(now, 1), new HybridTimestamp(now + 1_000_000, 100), true, false, groupId);
        assertEquals(lease, fromBytes(ByteBuffer.wrap(lease.bytes()).order(ByteOrder.LITTLE_ENDIAN)));

        lease = new Lease(null, new HybridTimestamp(1, 1), new HybridTimestamp(2 + 1_000_000, 100), true, true, groupId);
        assertEquals(lease, fromBytes(ByteBuffer.wrap(lease.bytes()).order(ByteOrder.LITTLE_ENDIAN)));

        lease = new Lease("node" + new String(new byte[1000]), new HybridTimestamp(1, 1), new HybridTimestamp(2, 100), false, false,
                groupId);
        assertEquals(lease, fromBytes(ByteBuffer.wrap(lease.bytes()).order(ByteOrder.LITTLE_ENDIAN)));
    }
}
