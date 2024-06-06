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


import static org.apache.ignite.internal.partition.replica.network.replication.RequestType.RO_GET;
import static org.apache.ignite.internal.partition.replica.network.replication.RequestType.RO_GET_ALL;
import static org.apache.ignite.internal.partition.replica.network.replication.RequestType.RO_SCAN;
import static org.apache.ignite.internal.partition.replica.network.replication.RequestType.RW_GET;
import static org.apache.ignite.internal.partition.replica.network.replication.RequestType.RW_GET_ALL;
import static org.apache.ignite.internal.partition.replica.network.replication.RequestType.RW_SCAN;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import org.apache.ignite.internal.partition.replica.network.replication.RequestType;
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
}
