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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class AssignmentsChainSerializerTest {
    private static final String ASSIGNMENTS_CHAIN_SERIALIZED_WITH_V1 =
            "Ae++QwMB775DAe++QwMEYWJjAQRkZWYAAFHCjAEA9AYAAwUB775DAe++QwIEZGVmAABRwowBAPQGAAQG";

    private final AssignmentsChainSerializer serializer = new AssignmentsChainSerializer();

    private static final long BASE_PHYSICAL_TIME = LocalDateTime.of(2024, Month.JANUARY, 1, 0, 0)
            .atOffset(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli();

    private static long baseTimestamp(int logical) {
        return new HybridTimestamp(BASE_PHYSICAL_TIME, logical).longValue();
    }

    @CartesianTest
    void serializationAndDeserialization(
            @Values(booleans = {false, true}) boolean force,
            @Values(booleans = {false, true}) boolean fromReset
    ) {
        AssignmentsChain originalAssignmentsChain =
                AssignmentsChain.of(2, 4, testAssignments1(force, fromReset));

        originalAssignmentsChain.addLast(testAssignments2(force, fromReset), 3, 5);

        byte[] bytes = VersionedSerialization.toBytes(originalAssignmentsChain, serializer);
        AssignmentsChain restoredAssignmentsChain = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredAssignmentsChain, equalTo(originalAssignmentsChain));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(ASSIGNMENTS_CHAIN_SERIALIZED_WITH_V1);
        AssignmentsChain restoredAssignmentsChain = VersionedSerialization.fromBytes(bytes, serializer);

        assertChainFromV1(restoredAssignmentsChain);
    }

    private static void assertChainFromV1(AssignmentsChain restoredChain) {
        assertThat(restoredChain.size(), is(2));
        Iterator<AssignmentsLink> iterator = restoredChain.iterator();

        assertThat(iterator.hasNext(), is(true));
        AssignmentsLink link0 = iterator.next();
        assertThat(link0.configurationIndex(), is(4L));
        assertThat(link0.configurationTerm(), is(2L));

        assertThat(iterator.hasNext(), is(true));
        AssignmentsLink link1 = iterator.next();
        assertThat(link1.configurationIndex(), is(5L));
        assertThat(link1.configurationTerm(), is(3L));

        assertNodes1FromV1(link0.assignments());
        assertNodes2FromV1(link1.assignments());
    }

    private static void assertNodes1FromV1(Assignments restoredAssignments) {
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

    private static void assertNodes2FromV1(Assignments restoredAssignments) {
        assertThat(restoredAssignments.nodes(), hasSize(1));
        List<Assignment> orderedNodes = restoredAssignments.nodes().stream()
                .sorted(comparing(Assignment::consistentId))
                .collect(toList());

        Assignment assignment1 = orderedNodes.get(0);
        assertThat(assignment1.consistentId(), is("def"));
        assertThat(assignment1.isPeer(), is(false));
    }

    private static Assignments testAssignments1(boolean force, boolean fromReset) {
        Set<Assignment> nodes = Set.of(Assignment.forPeer("abc"), Assignment.forLearner("def"));

        return force
                ? Assignments.forced(nodes, baseTimestamp(5))
                : Assignments.of(nodes, baseTimestamp(5), fromReset);
    }

    private static Assignments testAssignments2(boolean force, boolean fromReset) {
        Set<Assignment> nodes = Set.of(Assignment.forLearner("def"));

        return force
                ? Assignments.forced(nodes, baseTimestamp(5))
                : Assignments.of(nodes, baseTimestamp(5), fromReset);
    }
}
