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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.iterableWithSize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.affinity.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.largecluster.LargeClusterFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.smallcluster.SmallClusterFactory;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class to verify {@link ExecutionTargetFactory} implementations.
 *
 * @see SmallClusterFactory
 * @see LargeClusterFactory
 */
public class ExecutionTargetFactorySelfTest {
    private static final List<String> ALL_NODES = List.of("node1", "node2", "node3", "node4", "node5");
    private static final List<String> NODE_SET = List.of("node2", "node4", "node5");
    private static final List<String> NODE_SET2 = List.of("node2", "node3", "node5");
    private static final List<String> NODE_SUBSET = List.of("node2", "node5");
    private static final List<String> SINGLE_NODE_SET = List.of("node4");
    private static final List<String> INVALID_NODE_SET = List.of("node0");

    private static List<ExecutionTargetFactory> clusterFactory() {
        return List.of(
                new SmallClusterFactory(ALL_NODES),
                new LargeClusterFactory(ALL_NODES)
        );
    }

    @SuppressWarnings({"ResultOfObjectAllocationIgnored", "ThrowableNotThrown"})
    @Test
    void smallClusterFactory() {
        List<String> nodes = IntStream.range(0, 65).mapToObj(i -> "node").collect(Collectors.toList());

        assertThrows(IllegalArgumentException.class, () -> new SmallClusterFactory(nodes), "Supported up to 64 nodes");
    }

    @ParameterizedTest
    @MethodSource("clusterFactory")
    void targetsResolution(ExecutionTargetFactory f) {
        assertThat(f.resolveNodes(f.allOf(NODE_SET)), equalTo(NODE_SET));
        assertThat(f.resolveNodes(f.allOf(NODE_SET)), equalTo(NODE_SET));
        assertThat(f.resolveNodes(f.someOf(NODE_SET)), hasItems(in(NODE_SET)));
        assertThat(f.resolveNodes(f.oneOf(NODE_SET)), containsSingleFrom(NODE_SET));
        assertThat(f.resolveNodes(f.partitioned(assignmentFromPrimaries(NODE_SET))), equalTo(NODE_SET));
    }

    @ParameterizedTest
    @MethodSource("clusterFactory")
    void emptyTargets(ExecutionTargetFactory f) {
        assertThrows(AssertionError.class, () -> f.allOf(List.of()), null);
        assertThrows(AssertionError.class, () -> f.someOf(List.of()), null);
        assertThrows(AssertionError.class, () -> f.oneOf(List.of()), null);
        assertThrows(AssertionError.class, () -> f.partitioned(List.of()), null);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22969")
    @ParameterizedTest
    @MethodSource("clusterFactory")
    void invalidTargets(ExecutionTargetFactory f) {
        assertThrows(Throwable.class, () -> f.allOf(INVALID_NODE_SET), "invalid node");
        assertThrows(Throwable.class, () -> f.someOf(INVALID_NODE_SET), "invalid node");
        assertThrows(Throwable.class, () -> f.oneOf(INVALID_NODE_SET), "invalid node");
        assertThrows(Throwable.class, () -> f.partitioned(assignmentFromPrimaries(INVALID_NODE_SET)), "invalid node");
    }

    @ParameterizedTest
    @MethodSource("clusterFactory")
    void allOfTargets(ExecutionTargetFactory f) throws Exception {
        // Self colocation
        assertColocated(f, f.allOf(NODE_SET), f.allOf(NODE_SET), equalTo(NODE_SET));
        assertNotColocated(f.allOf(NODE_SET), f.allOf(NODE_SUBSET));
        assertNotColocated(f.allOf(NODE_SUBSET), f.allOf(NODE_SET));

        // Colocation with SomeOf
        assertColocated(f, f.allOf(NODE_SET), f.someOf(NODE_SET), equalTo(NODE_SET));
        assertNotColocated(f.allOf(NODE_SET), f.someOf(NODE_SUBSET));
        assertColocated(f, f.allOf(NODE_SUBSET), f.someOf(NODE_SET), equalTo(NODE_SUBSET));

        // Colocation with OneOf
        assertNotColocated(f.allOf(NODE_SET), f.oneOf(NODE_SET));
        assertNotColocated(f.allOf(NODE_SET), f.oneOf(NODE_SUBSET));
        assertNotColocated(f.allOf(NODE_SUBSET), f.oneOf(NODE_SET));
        assertColocated(f, f.allOf(SINGLE_NODE_SET), f.oneOf(NODE_SET), equalTo(SINGLE_NODE_SET));

        // Colocation with Partitioned
        assertNotColocated(f.allOf(NODE_SET), f.partitioned(assignmentFromPrimaries(NODE_SET)),
                "AllOf target and Partitioned can't be colocated");
        assertNotColocated(f.allOf(SINGLE_NODE_SET), f.partitioned(assignmentFromPrimaries(NODE_SET)),
                "AllOf target and Partitioned can't be colocated");
        assertNotColocated(f.allOf(SINGLE_NODE_SET), f.partitioned(assignmentFromPrimaries(SINGLE_NODE_SET)),
                "AllOf target and Partitioned can't be colocated");
    }

    @ParameterizedTest
    @MethodSource("clusterFactory")
    void someOfTargets(ExecutionTargetFactory f) throws Exception {
        // Self colocation
        assertColocated(f, f.someOf(NODE_SET), f.someOf(NODE_SET), equalTo(NODE_SET));
        assertColocated(f, f.someOf(NODE_SET), f.someOf(NODE_SUBSET), equalTo(NODE_SUBSET));
        assertColocated(f, f.someOf(NODE_SUBSET), f.someOf(NODE_SET), equalTo(NODE_SUBSET));
        assertNotColocated(f.someOf(SINGLE_NODE_SET), f.someOf(NODE_SET2)); // Disjoint sets.

        // Colocation with AllOf
        assertColocated(f, f.someOf(NODE_SET), f.allOf(NODE_SET), equalTo(NODE_SET));
        assertColocated(f, f.someOf(NODE_SET), f.allOf(NODE_SUBSET), equalTo(NODE_SUBSET));
        assertNotColocated(f.someOf(NODE_SUBSET), f.allOf(NODE_SET));

        // Colocation with OneOf
        assertColocated(f, f.someOf(NODE_SET), f.oneOf(NODE_SET), containsSingleFrom(NODE_SET));
        assertColocated(f, f.someOf(NODE_SUBSET), f.oneOf(NODE_SET), containsSingleFrom(NODE_SUBSET));
        assertColocated(f, f.someOf(NODE_SET), f.oneOf(NODE_SUBSET), containsSingleFrom(NODE_SUBSET));
        assertNotColocated(f.someOf(SINGLE_NODE_SET), f.oneOf(NODE_SET2)); // Disjoint sets.

        // Colocation with Partitioned
        assertColocated(f, f.someOf(NODE_SET), f.partitioned(assignmentFromPrimaries(NODE_SET)), equalTo(NODE_SET));
        assertColocated(f, f.someOf(NODE_SET), f.partitioned(assignmentFromPrimaries(NODE_SUBSET)), equalTo(NODE_SUBSET));
        assertNotColocated(f.someOf(NODE_SUBSET), f.partitioned(assignmentFromPrimaries(NODE_SET)));
    }

    @ParameterizedTest
    @MethodSource("clusterFactory")
    void oneOfTargets(ExecutionTargetFactory f) throws Exception {
        // Self colocation
        assertColocated(f, f.oneOf(NODE_SET), f.oneOf(NODE_SET), containsSingleFrom(NODE_SET));
        assertColocated(f, f.oneOf(NODE_SET), f.oneOf(shuffle(NODE_SET)), containsSingleFrom(NODE_SET));
        assertColocated(f, f.oneOf(NODE_SUBSET), f.oneOf(NODE_SET), containsSingleFrom(NODE_SUBSET));
        assertColocated(f, f.oneOf(NODE_SET), f.oneOf(NODE_SUBSET), containsSingleFrom(NODE_SUBSET));
        assertNotColocated(f.oneOf(NODE_SET2), f.oneOf(SINGLE_NODE_SET)); // Disjoint sets.

        // Colocation with AllOf
        assertNotColocated(f.oneOf(NODE_SET), f.allOf(NODE_SET));
        assertColocated(f, f.oneOf(NODE_SET), f.allOf(SINGLE_NODE_SET), equalTo(SINGLE_NODE_SET));
        assertNotColocated(f.oneOf(NODE_SUBSET), f.allOf(NODE_SET));
        assertNotColocated(f.oneOf(NODE_SET2), f.allOf(SINGLE_NODE_SET)); // Disjoint sets.

        // Colocation with someOf
        assertColocated(f, f.oneOf(NODE_SET), f.someOf(NODE_SET), containsSingleFrom(NODE_SET));
        assertColocated(f, f.oneOf(NODE_SUBSET), f.someOf(NODE_SET), containsSingleFrom(NODE_SUBSET));
        assertColocated(f, f.oneOf(NODE_SET), f.someOf(NODE_SUBSET), containsSingleFrom(NODE_SUBSET));
        assertNotColocated(f.oneOf(SINGLE_NODE_SET), f.someOf(NODE_SET2)); // Disjoint sets.

        // Colocation with Partitioned
        assertNotColocated(f.oneOf(NODE_SET), f.partitioned(assignmentFromPrimaries(NODE_SET)));
        assertColocated(f, f.oneOf(NODE_SET), f.partitioned(assignmentFromPrimaries(SINGLE_NODE_SET)), equalTo(SINGLE_NODE_SET));
        assertNotColocated(f.oneOf(NODE_SET2), f.partitioned(assignmentFromPrimaries(SINGLE_NODE_SET))); // Disjoint sets.
    }

    @ParameterizedTest
    @MethodSource("clusterFactory")
    void partitionedPrimaryTargets(ExecutionTargetFactory f) throws Exception {
        // Self colocation
        assertColocated(f, f.partitioned(assignmentFromPrimaries(NODE_SET)), f.partitioned(assignmentFromPrimaries(NODE_SET)),
                equalTo(NODE_SET));
        assertNotColocated(f.partitioned(assignmentFromPrimaries(NODE_SET)), f.partitioned(shuffle(assignmentFromPrimaries(NODE_SET))));
        assertNotColocated(f.partitioned(assignmentFromPrimaries(NODE_SUBSET)), f.partitioned(assignmentFromPrimaries(NODE_SET)),
                "Partitioned targets with mot matching numbers of partitioned are not colocated");
        assertNotColocated(f.partitioned(assignmentFromPrimaries(NODE_SET)), f.partitioned(assignmentFromPrimaries(NODE_SUBSET)),
                "Partitioned targets with mot matching numbers of partitioned are not colocated");

        assertNotColocated(f.partitioned(singleWithToken("node1", 1)), f.partitioned(singleWithToken("node1", 2)),
                "Partitioned targets have different terms");

        // Colocation with AllOf
        assertNotColocated(f.partitioned(assignmentFromPrimaries(NODE_SET)), f.allOf(NODE_SET),
                "AllOf target and Partitioned can't be colocated");
        assertNotColocated(f.partitioned(assignmentFromPrimaries(NODE_SET)), f.allOf(SINGLE_NODE_SET),
                "AllOf target and Partitioned can't be colocated");

        // Colocation with someOf
        assertColocated(f, f.partitioned(assignmentFromPrimaries(NODE_SET)), f.someOf(NODE_SET), equalTo(NODE_SET));
        assertColocated(f, f.partitioned(assignmentFromPrimaries(NODE_SUBSET)), f.someOf(NODE_SET), equalTo(NODE_SUBSET));
        assertNotColocated(f.partitioned(assignmentFromPrimaries(NODE_SET)), f.someOf(NODE_SUBSET));
        assertNotColocated(f.partitioned(assignmentFromPrimaries(SINGLE_NODE_SET)), f.someOf(NODE_SET2)); // Disjoint sets.

        // Colocation with oneOf
        assertNotColocated(f.partitioned(assignmentFromPrimaries(NODE_SET)), f.oneOf(NODE_SET));
        assertColocated(f, f.partitioned(assignmentFromPrimaries(SINGLE_NODE_SET)), f.oneOf(NODE_SET), equalTo(SINGLE_NODE_SET));
        assertNotColocated(f.partitioned(assignmentFromPrimaries(SINGLE_NODE_SET)), f.oneOf(NODE_SET2)); // Disjoint
    }

    @ParameterizedTest
    @MethodSource("clusterFactory")
    void partitionedWithBackupsTargets(ExecutionTargetFactory f) throws Exception {
        // Self colocation
        assertColocated(f, f.partitioned(assignment(NODE_SET, NODE_SET2)), f.partitioned(assignment(NODE_SET, NODE_SET2)),
                hasItems(in(NODE_SUBSET)));
        assertColocated(f, f.partitioned(assignment(NODE_SET, NODE_SET2)), f.partitioned(assignment(NODE_SET2, NODE_SET)),
                hasItems(in(NODE_SUBSET)));

        // Colocation with AllOf
        assertNotColocated(f.partitioned(assignment(NODE_SET, NODE_SET2)), f.allOf(NODE_SUBSET),
                "AllOf target and Partitioned can't be colocated");
        assertNotColocated(f.partitioned(assignment(NODE_SET, NODE_SET2)), f.allOf(SINGLE_NODE_SET),
                "AllOf target and Partitioned can't be colocated");

        // Colocation with someOf
        assertColocated(f, f.partitioned(assignment(NODE_SET, NODE_SET2)), f.someOf(NODE_SET), hasItems((in(NODE_SET))));
        assertNotColocated(f.partitioned(assignment(NODE_SET, NODE_SET2)), f.someOf(SINGLE_NODE_SET)); // Disjoint sets.

        // Colocation with oneOf
        assertColocated(f, f.partitioned(assignment(NODE_SET, NODE_SET2)), f.someOf(NODE_SET), hasItems((in(NODE_SUBSET))));
        assertColocated(f, f.partitioned(assignment(NODE_SET, NODE_SET2)), f.oneOf(NODE_SET), containsSingleFrom(NODE_SUBSET));
        assertNotColocated(f.partitioned(assignment(NODE_SET, NODE_SET2)), f.oneOf(SINGLE_NODE_SET)); // Disjoint
    }

    private static List<TokenizedAssignments> assignmentFromPrimaries(List<String> nodes) {
        return nodes.stream()
                .map(n -> new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(n)), 1))
                .collect(Collectors.toList());
    }

    private static List<TokenizedAssignments> assignment(List<String> part1Nodes, List<String> part2Nodes) {
        Set<Assignment> part1 = part1Nodes.stream().map(Assignment::forPeer).collect(Collectors.toSet());
        Set<Assignment> part2 = part2Nodes.stream().map(Assignment::forPeer).collect(Collectors.toSet());

        return List.of(
                new TokenizedAssignmentsImpl(part1, 1),
                new TokenizedAssignmentsImpl(part2, 2)
        );
    }

    private static List<TokenizedAssignments> singleWithToken(String name, int token) {
        return List.of(new TokenizedAssignmentsImpl(Set.of(Assignment.forPeer(name)), token));
    }

    private static <T> ArrayList<T> shuffle(List<T> nodeSetWithTokens) {
        ArrayList<T> shuffled = new ArrayList<>(nodeSetWithTokens);
        Collections.reverse(shuffled);
        return shuffled;
    }

    private static Matcher<Iterable<String>> containsSingleFrom(List<String> items) {
        return allOf(iterableWithSize(1), hasItems(in(items)));
    }

    private static void assertColocated(
            ExecutionTargetFactory factory,
            ExecutionTarget target,
            ExecutionTarget other,
            Matcher<Iterable<String>> matcher
    ) throws ColocationMappingException {
        assertThat(factory.resolveNodes(target.colocateWith(other)), matcher);
    }

    private static void assertNotColocated(ExecutionTarget target, ExecutionTarget other) {
        assertNotColocated(target, other, "Targets are not colocated");
    }

    @SuppressWarnings("ThrowableNotThrown")
    private static void assertNotColocated(ExecutionTarget target, ExecutionTarget other, String errorMessageFragment) {
        assertThrows(ColocationMappingException.class, () -> target.colocateWith(other), errorMessageFragment);
    }
}
