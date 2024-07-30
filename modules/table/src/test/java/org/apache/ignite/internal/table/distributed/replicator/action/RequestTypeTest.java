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

package org.apache.ignite.internal.table.distributed.replicator.action;


import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RO_SCAN;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_DELETE_EXACT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_DELETE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET_AND_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_INSERT_ALL;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_REPLACE_IF_EXIST;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_SCAN;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_UPSERT_ALL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.junit.jupiter.api.Test;

class RequestTypeTest {
    @Test
    void isRwReadWorksAsExpected() {
        Set<RequestType> expectedRwReads = EnumSet.of(RW_GET, RW_GET_ALL, RW_SCAN);

        for (RequestType requestType : RequestType.values()) {
            if (expectedRwReads.contains(requestType)) {
                assertTrue(requestType.isRwRead(), requestType + " must be an RW read");
            } else {
                assertFalse(requestType.isRwRead(), requestType + " must not be an RW read");
            }
        }
    }

    @Test
    void isWriteWorksAsExpected() {
        Set<RequestType> expectedNonWrites = EnumSet.of(RW_GET, RW_GET_ALL, RW_SCAN, RO_GET, RO_GET_ALL, RO_SCAN);

        for (RequestType requestType : RequestType.values()) {
            if (expectedNonWrites.contains(requestType)) {
                assertFalse(requestType.isWrite(), requestType + " must not be a write");
            } else {
                assertTrue(requestType.isWrite(), requestType + " must be a write");
            }
        }
    }

    /** Checks that the ordinal does not change, since the enum will be transfer in the {@link NetworkMessage}. */
    @Test
    void testFromOrdinal() {
        assertEquals(RW_GET, RequestType.fromOrdinal(0));

        assertEquals(RW_GET_ALL, RequestType.fromOrdinal(1));

        assertEquals(RW_DELETE, RequestType.fromOrdinal(2));

        assertEquals(RW_DELETE_ALL, RequestType.fromOrdinal(3));

        assertEquals(RW_DELETE_EXACT, RequestType.fromOrdinal(4));

        assertEquals(RW_DELETE_EXACT_ALL, RequestType.fromOrdinal(5));

        assertEquals(RW_INSERT, RequestType.fromOrdinal(6));

        assertEquals(RW_INSERT_ALL, RequestType.fromOrdinal(7));

        assertEquals(RW_UPSERT, RequestType.fromOrdinal(8));

        assertEquals(RW_UPSERT_ALL, RequestType.fromOrdinal(9));

        assertEquals(RW_REPLACE, RequestType.fromOrdinal(10));

        assertEquals(RW_REPLACE_IF_EXIST, RequestType.fromOrdinal(11));

        assertEquals(RW_GET_AND_DELETE, RequestType.fromOrdinal(12));

        assertEquals(RW_GET_AND_REPLACE, RequestType.fromOrdinal(13));

        assertEquals(RW_GET_AND_UPSERT, RequestType.fromOrdinal(14));

        assertEquals(RW_SCAN, RequestType.fromOrdinal(15));

        assertEquals(RO_GET, RequestType.fromOrdinal(16));

        assertEquals(RO_GET_ALL, RequestType.fromOrdinal(17));

        assertEquals(RO_SCAN, RequestType.fromOrdinal(18));

        assertThrows(IllegalArgumentException.class, () -> RequestType.fromOrdinal(-1));
        assertThrows(IllegalArgumentException.class, () -> RequestType.fromOrdinal(19));
    }
}
