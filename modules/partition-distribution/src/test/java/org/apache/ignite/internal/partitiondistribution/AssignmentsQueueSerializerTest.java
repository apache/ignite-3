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

package org.apache.ignite.internal.partitiondistribution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class AssignmentsQueueSerializerTest {
    private static final String ASSIGNMENTS_QUEUE_V1 =
            "Ae++QwQDBGFiYwEEZGVmAAFRwowBAPQGAAMEYWJjAQRkZWYAAFHCjAEA9AYBAwRhYmMBBGRlZgAAUcKMAQD0BgA=";

    private static final long BASE_PHYSICAL_TIME = LocalDateTime.of(2024, Month.JANUARY, 1, 0, 0)
            .atOffset(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli();

    private final AssignmentsQueueSerializer serializer = new AssignmentsQueueSerializer();

    @CartesianTest
    void serializationAndDeserialization(
            @Values(booleans = {true, false}) boolean force,
            @Values(booleans = {true, false}) boolean fromReset
    ) {
        AssignmentsQueue originalAssignmentsQueue = new AssignmentsQueue(testAssignments(force, fromReset));

        byte[] bytes = VersionedSerialization.toBytes(originalAssignmentsQueue, serializer);
        AssignmentsQueue restoredAssignmentsQueue = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredAssignmentsQueue, equalTo(originalAssignmentsQueue));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(ASSIGNMENTS_QUEUE_V1);
        // In order to maintain 3.1 to 3.0 backward compatibility we drop AssignmentsQueue v1 support.
        assertThrows(IgniteInternalException.class, () -> VersionedSerialization.fromBytes(bytes, serializer));
    }

    @Test
    void assignmentsCanBeDeserialized() {
        AssignmentsSerializer assignmentsSerializer = new AssignmentsSerializer();
        Assignments assignments = testAssignments(true, false);
        byte[] assignmentsBytes = VersionedSerialization.toBytes(assignments, assignmentsSerializer);

        AssignmentsQueue restoredAssignmentsQueue = VersionedSerialization.fromBytes(assignmentsBytes, serializer);

        assertThat(restoredAssignmentsQueue.poll(), equalTo(assignments));
    }

    private static Assignments testAssignments(boolean force, boolean fromReset) {
        Set<Assignment> nodes = Set.of(Assignment.forPeer("abc"), Assignment.forLearner("def"));

        return force
                ? Assignments.forced(nodes, baseTimestamp(5))
                : Assignments.of(nodes, baseTimestamp(5), fromReset);
    }

    private static long baseTimestamp(int logical) {
        return new HybridTimestamp(BASE_PHYSICAL_TIME, logical).longValue();
    }

    @SuppressWarnings("unused")
    private static <T> String v1Base64(T object, VersionedSerializer<T> serializer) {
        byte[] v1Bytes = VersionedSerialization.toBytes(object, serializer);
        return Base64.getEncoder().encodeToString(v1Bytes);
    }
}
