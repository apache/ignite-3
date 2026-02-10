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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** For {@link RequestType} testing. */
public class RequestTypeTest {
    private static final Set<RequestType> EXPECTED_RW_READS = EnumSet.of(RW_GET, RW_GET_ALL, RW_SCAN);
    private static final Set<RequestType> EXPECTED_NON_WRITES = EnumSet.of(RW_GET, RW_GET_ALL, RW_SCAN, RO_GET, RO_GET_ALL, RO_SCAN);

    private static Stream<Arguments> requestTypeIds() {
        return Stream.of(
                arguments(RW_GET, 0),
                arguments(RW_GET_ALL, 1),
                arguments(RW_DELETE, 2),
                arguments(RW_DELETE_ALL, 3),
                arguments(RW_DELETE_EXACT, 4),
                arguments(RW_DELETE_EXACT_ALL, 5),
                arguments(RW_INSERT, 6),
                arguments(RW_INSERT_ALL, 7),
                arguments(RW_UPSERT, 8),
                arguments(RW_UPSERT_ALL, 9),
                arguments(RW_REPLACE, 10),
                arguments(RW_REPLACE_IF_EXIST, 11),
                arguments(RW_GET_AND_DELETE, 12),
                arguments(RW_GET_AND_REPLACE, 13),
                arguments(RW_GET_AND_UPSERT, 14),
                arguments(RW_SCAN, 15),
                arguments(RO_GET, 16),
                arguments(RO_GET_ALL, 17),
                arguments(RO_SCAN, 18)
        );
    }

    @ParameterizedTest
    @MethodSource("requestTypeIds")
    void testId(RequestType requestType, int expectedId) {
        assertEquals(expectedId, requestType.id());
    }

    /** Checks that the ID does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @ParameterizedTest
    @MethodSource("requestTypeIds")
    void testFromId(RequestType expectedEnumEntry, int id) {
        assertEquals(expectedEnumEntry, RequestType.fromId(id));

    }

    @Test
    void testFromIdThrows() {
        assertThrows(IllegalArgumentException.class, () -> RequestType.fromId(-1));
        assertThrows(IllegalArgumentException.class, () -> RequestType.fromId(19));
    }

    @ParameterizedTest
    @EnumSource(RequestType.class)
    void isRwReadWorksAsExpected(RequestType requestType) {
        if (EXPECTED_RW_READS.contains(requestType)) {
            assertTrue(requestType.isRwRead(), requestType + " must be an RW read");
        } else {
            assertFalse(requestType.isRwRead(), requestType + " must not be an RW read");
        }
    }

    @ParameterizedTest
    @EnumSource(RequestType.class)
    void isWriteWorksAsExpected(RequestType requestType) {
        if (EXPECTED_NON_WRITES.contains(requestType)) {
            assertFalse(requestType.isWrite(), requestType + " must not be a write");
        } else {
            assertTrue(requestType.isWrite(), requestType + " must be a write");
        }
    }
}
