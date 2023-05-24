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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Tests for lease encoding and decoding from byte arrays.
 */
public class LeaseSerializationTest {
    @Test
    public void test() {
        Lease lease;

        long now = System.currentTimeMillis();

        lease = Lease.EMPTY_LEASE;
        assertEquals(lease, fromBytes(lease.bytes()));

        lease = new Lease("node1", new HybridTimestamp(now, 1), new HybridTimestamp(now + 1_000_000, 100), true, true);
        assertEquals(lease, fromBytes(lease.bytes()));

        lease = new Lease("node1", new HybridTimestamp(now, 1), new HybridTimestamp(now + 1_000_000, 100), false, false);
        assertEquals(lease, fromBytes(lease.bytes()));

        lease = new Lease("node1", new HybridTimestamp(now, 1), new HybridTimestamp(now + 1_000_000, 100), false, true);
        assertEquals(lease, fromBytes(lease.bytes()));

        lease = new Lease("node1", new HybridTimestamp(now, 1), new HybridTimestamp(now + 1_000_000, 100), true, false);
        assertEquals(lease, fromBytes(lease.bytes()));

        lease = new Lease(null, new HybridTimestamp(1, 1), new HybridTimestamp(2 + 1_000_000, 100), true, true);
        assertEquals(lease, fromBytes(lease.bytes()));

        lease = new Lease("node" + new String(new byte[1000]), new HybridTimestamp(1, 1), new HybridTimestamp(2, 100), false, false);
        assertEquals(lease, fromBytes(lease.bytes()));
    }
}
