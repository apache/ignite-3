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


package org.apache.ignite.internal.partition.replicator.network.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;

/** For {@link RequestType} testing. */
public class RequestTypeTest {
    /** Checks that the ordinal does not change, since the enum will be transfer in the {@link NetworkMessage}. */
    @Test
    void testFromOrdinal() {
        assertEquals(RequestType.RW_GET, RequestType.fromOrdinal(0));

        assertEquals(RequestType.RW_GET_ALL, RequestType.fromOrdinal(1));

        assertEquals(RequestType.RW_DELETE, RequestType.fromOrdinal(2));

        assertEquals(RequestType.RW_DELETE_ALL, RequestType.fromOrdinal(3));

        assertEquals(RequestType.RW_DELETE_EXACT, RequestType.fromOrdinal(4));

        assertEquals(RequestType.RW_DELETE_EXACT_ALL, RequestType.fromOrdinal(5));

        assertEquals(RequestType.RW_INSERT, RequestType.fromOrdinal(6));

        assertEquals(RequestType.RW_INSERT_ALL, RequestType.fromOrdinal(7));

        assertEquals(RequestType.RW_UPSERT, RequestType.fromOrdinal(8));

        assertEquals(RequestType.RW_UPSERT_ALL, RequestType.fromOrdinal(9));

        assertEquals(RequestType.RW_REPLACE, RequestType.fromOrdinal(10));

        assertEquals(RequestType.RW_REPLACE_IF_EXIST, RequestType.fromOrdinal(11));

        assertEquals(RequestType.RW_GET_AND_DELETE, RequestType.fromOrdinal(12));

        assertEquals(RequestType.RW_GET_AND_REPLACE, RequestType.fromOrdinal(13));

        assertEquals(RequestType.RW_GET_AND_UPSERT, RequestType.fromOrdinal(14));

        assertEquals(RequestType.RW_SCAN, RequestType.fromOrdinal(15));

        assertEquals(RequestType.RO_GET, RequestType.fromOrdinal(16));

        assertEquals(RequestType.RO_GET_ALL, RequestType.fromOrdinal(17));

        assertEquals(RequestType.RO_SCAN, RequestType.fromOrdinal(18));

        assertThrows(IllegalArgumentException.class, () -> RequestType.fromOrdinal(-1));
        assertThrows(IllegalArgumentException.class, () -> RequestType.fromOrdinal(19));
    }
}
