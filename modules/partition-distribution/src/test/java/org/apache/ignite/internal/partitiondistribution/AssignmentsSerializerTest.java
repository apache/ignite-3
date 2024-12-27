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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class AssignmentsSerializerTest {
    private static final String NOT_FORCED_NOT_RESET_ASSIGNMENTS_SERIALIZED_WITH_V1 = "Ae++QwMEYWJjAQRkZWYAAFHCjAEA9AYA";
    private static final String NOT_FORCED_RESET_ASSIGNMENTS_SERIALIZED_WITH_V1 = "Ae++QwMEYWJjAQRkZWYAAFHCjAEA9AYB";
    private static final String FORCED_ASSIGNMENTS_SERIALIZED_WITH_V1 = "Ae++QwMEYWJjAQRkZWYAAVHCjAEA9AYA";

    private final AssignmentsSerializer serializer = new AssignmentsSerializer();

    private static final long BASE_PHYSICAL_TIME = LocalDateTime.of(2024, Month.JANUARY, 1, 0, 0)
            .atOffset(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli();

    private static long baseTimestamp(int logical) {
        return new HybridTimestamp(BASE_PHYSICAL_TIME, logical).longValue();
    }

    @CartesianTest
    void serializationAndDeserialization(
            @Values(booleans = {true, false}) boolean force,
            @Values(booleans = {true, false}) boolean fromReset
    ) {
        Assignments originalAssignments = testAssignments(force, fromReset);

        byte[] bytes = VersionedSerialization.toBytes(originalAssignments, serializer);
        Assignments restoredAssignments = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredAssignments, equalTo(originalAssignments));
    }

    private static Assignments testAssignments(boolean force, boolean fromReset) {
        Set<Assignment> nodes = Set.of(Assignment.forPeer("abc"), Assignment.forLearner("def"));

        return force
                ? Assignments.forced(nodes, baseTimestamp(5))
                : Assignments.of(nodes, baseTimestamp(5), fromReset);
    }

    @Test
    void v1NotForcedNotResetCanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(NOT_FORCED_NOT_RESET_ASSIGNMENTS_SERIALIZED_WITH_V1);
        Assignments restoredAssignments = VersionedSerialization.fromBytes(bytes, serializer);

        assertNodesFromV1(restoredAssignments);

        assertThat(restoredAssignments.force(), is(false));
        assertThat(restoredAssignments.fromReset(), is(false));
        assertThat(restoredAssignments.timestamp(), is(baseTimestamp(5)));
    }

    @Test
    void v1NotForcedResetCanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(NOT_FORCED_RESET_ASSIGNMENTS_SERIALIZED_WITH_V1);
        Assignments restoredAssignments = VersionedSerialization.fromBytes(bytes, serializer);

        assertNodesFromV1(restoredAssignments);

        assertThat(restoredAssignments.force(), is(false));
        assertThat(restoredAssignments.fromReset(), is(true));
        assertThat(restoredAssignments.timestamp(), is(baseTimestamp(5)));
    }

    @Test
    void v1ForcedCanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(FORCED_ASSIGNMENTS_SERIALIZED_WITH_V1);
        Assignments restoredAssignments = VersionedSerialization.fromBytes(bytes, serializer);

        assertNodesFromV1(restoredAssignments);

        assertThat(restoredAssignments.force(), is(true));
        assertThat(restoredAssignments.timestamp(), is(baseTimestamp(5)));
    }

    @SuppressWarnings("unused")
    private String v1Base64(boolean force, boolean fromReset) {
        Assignments originalAssignments = testAssignments(force, fromReset);
        byte[] v1Bytes = VersionedSerialization.toBytes(originalAssignments, serializer);
        return Base64.getEncoder().encodeToString(v1Bytes);
    }

    private static void assertNodesFromV1(Assignments restoredAssignments) {
        assertThat(restoredAssignments.nodes(), hasSize(2));
        List<Assignment> orderedNodes = restoredAssignments.nodes().stream()
                .sorted(comparing(Assignment::consistentId))
                .collect(toList());

        Assignment assignment1 = orderedNodes.get(0);
        assertThat(assignment1.consistentId(), is("abc"));
        assertThat(assignment1.isPeer(), is(true));

        Assignment assignment2 = orderedNodes.get(1);
        assertThat(assignment2.consistentId(), is("def"));
        assertThat(assignment2.isPeer(), is(false));
    }
}
