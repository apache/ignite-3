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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** For {@link RequestType} testing. */
public class RequestTypeTest {
    private static Stream<Arguments> requestTypeIds() {
        return Stream.of(
                arguments(RequestType.RW_GET, 0),
                arguments(RequestType.RW_GET_ALL, 1),
                arguments(RequestType.RW_DELETE, 2),
                arguments(RequestType.RW_DELETE_ALL, 3),
                arguments(RequestType.RW_DELETE_EXACT, 4),
                arguments(RequestType.RW_DELETE_EXACT_ALL, 5),
                arguments(RequestType.RW_INSERT, 6),
                arguments(RequestType.RW_INSERT_ALL, 7),
                arguments(RequestType.RW_UPSERT, 8),
                arguments(RequestType.RW_UPSERT_ALL, 9),
                arguments(RequestType.RW_REPLACE, 10),
                arguments(RequestType.RW_REPLACE_IF_EXIST, 11),
                arguments(RequestType.RW_GET_AND_DELETE, 12),
                arguments(RequestType.RW_GET_AND_REPLACE, 13),
                arguments(RequestType.RW_GET_AND_UPSERT, 14),
                arguments(RequestType.RW_SCAN, 15),
                arguments(RequestType.RO_GET, 16),
                arguments(RequestType.RO_GET_ALL, 17),
                arguments(RequestType.RO_SCAN, 18)
        );
    }

    @ParameterizedTest
    @MethodSource("requestTypeIds")
    void testId(RequestType requestType, int expectedId) {
        assertEquals(expectedId, requestType.id());
    }

    /** Checks that the ordinal does not change, since the enum will be transferred in the {@link NetworkMessage}. */
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
}
