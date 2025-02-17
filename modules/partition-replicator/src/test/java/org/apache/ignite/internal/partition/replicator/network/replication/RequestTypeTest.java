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

import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;

/** For {@link RequestType} testing. */
public class RequestTypeTest {
    /** Checks that the transferable ID does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @Test
    void testTransferableId() {
        assertEquals(0, RequestType.RW_GET.transferableId());
        assertEquals(1, RequestType.RW_GET_ALL.transferableId());
        assertEquals(2, RequestType.RW_DELETE.transferableId());
        assertEquals(3, RequestType.RW_DELETE_ALL.transferableId());
        assertEquals(4, RequestType.RW_DELETE_EXACT.transferableId());
        assertEquals(5, RequestType.RW_DELETE_EXACT_ALL.transferableId());
        assertEquals(6, RequestType.RW_INSERT.transferableId());
        assertEquals(7, RequestType.RW_INSERT_ALL.transferableId());
        assertEquals(8, RequestType.RW_UPSERT.transferableId());
        assertEquals(9, RequestType.RW_UPSERT_ALL.transferableId());
        assertEquals(10, RequestType.RW_REPLACE.transferableId());
        assertEquals(11, RequestType.RW_REPLACE_IF_EXIST.transferableId());
        assertEquals(12, RequestType.RW_GET_AND_DELETE.transferableId());
        assertEquals(13, RequestType.RW_GET_AND_REPLACE.transferableId());
        assertEquals(14, RequestType.RW_GET_AND_UPSERT.transferableId());
        assertEquals(15, RequestType.RW_SCAN.transferableId());
        assertEquals(16, RequestType.RO_GET.transferableId());
        assertEquals(17, RequestType.RO_GET_ALL.transferableId());
        assertEquals(18, RequestType.RO_SCAN.transferableId());
    }
}
