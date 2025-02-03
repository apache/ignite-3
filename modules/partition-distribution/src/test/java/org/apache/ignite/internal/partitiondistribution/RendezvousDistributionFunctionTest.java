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

import static java.util.Collections.emptyList;
import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for Rendezvous distribution function.
 */
public class RendezvousDistributionFunctionTest {
    /** Distribution deviation ratio. */
    private static final double DISTRIBUTION_DEVIATION_RATIO = 0.2;

    @Test
    public void testPartitionDistribution() {
        int nodeCount = 50;

        int parts = 10_000;

        int replicas = 4;

        List<String> nodes = prepareNetworkTopology(nodeCount);

        assertTrue(parts > nodeCount, "Partitions should be more than nodes");

        int ideal = (parts * replicas) / nodeCount;

        List<Set<Assignment>> assignment = RendezvousDistributionFunction.assignPartitions(
                nodes,
                parts,
                replicas,
                replicas,
                false,
                null
        );

        HashMap<String, ArrayList<Integer>> assignmentByNode = new HashMap<>(nodeCount);

        int part = 0;

        for (Set<Assignment> partNodes : assignment) {
            for (Assignment node : partNodes) {
                ArrayList<Integer> nodeParts = assignmentByNode.computeIfAbsent(node.consistentId(), k -> new ArrayList<>());

                nodeParts.add(part);
            }

            part++;
        }

        for (String node : nodes) {
            ArrayList<Integer> nodeParts = assignmentByNode.get(node);

            assertNotNull(nodeParts);

            assertTrue(nodeParts.size() > ideal * (1 - DISTRIBUTION_DEVIATION_RATIO)
                            && nodeParts.size() < ideal * (1 + DISTRIBUTION_DEVIATION_RATIO),
                    "Partition distribution is too far from ideal [node=" + node
                            + ", size=" + nodeParts.size()
                            + ", idealSize=" + ideal
                            + ", parts=" + compact(nodeParts) + ']');
        }
    }

    @Test
    public void testAllPartitionsDistribution() {
        int nodeCount = 50;

        int parts = 10_000;

        int replicas = DistributionAlgorithm.ALL_REPLICAS;

        int consensusGroupSize = 4;

        List<String> nodes = prepareNetworkTopology(nodeCount);

        assertTrue(parts > nodeCount, "Partitions should be more than nodes");

        List<Set<Assignment>> assignment = RendezvousDistributionFunction.assignPartitions(
                nodes,
                parts,
                replicas,
                consensusGroupSize,
                false,
                null
        );

        assertEquals(parts, assignment.size());

        assignment.forEach(a -> {
            assertEquals(nodeCount, a.size());
            assertEquals(consensusGroupSize, a.stream().filter(Assignment::isPeer).count());
        });
    }

    @ParameterizedTest
    @MethodSource("distributionWithLearnersArguments")
    public void testDistributionWithLearners(int nodeCount, int partitions, int replicas, int consensusReplicas) {
        List<String> nodes = prepareNetworkTopology(nodeCount);

        DistributionAlgorithm distributionAlgorithm = new RendezvousDistributionFunction();

        List<Set<Assignment>> assignments = distributionAlgorithm
                .assignPartitions(nodes, emptyList(), partitions, replicas, consensusReplicas);

        for (int p = 0; p < partitions; p++) {
            Set<Assignment> a = assignments.get(p);
            assertEquals(replicas, a.size());
            assertEquals(consensusReplicas, a.stream().filter(Assignment::isPeer).count());
        }

        List<Set<Assignment>> allAssignments = distributionAlgorithm
                .assignPartitions(nodes, emptyList(), 1, replicas, consensusReplicas);

        Set<Assignment> partitionAssignments = allAssignments.get(0);

        assertEquals(replicas, partitionAssignments.size());
        assertEquals(consensusReplicas, partitionAssignments.stream().filter(Assignment::isPeer).count());
    }

    private static Stream<Arguments> distributionWithLearnersArguments() {
        List<Arguments> arg = new ArrayList<>();

        for (Integer nodes : List.of(1, 2, 3, 4, 5, 7, 10, 20, 50, 100)) {
            for (Integer partitions : List.of(1, 10, 25, 50, 100)) {
                for (Integer replicas : List.of(1, 2, 3, 4, 5, 7, 10, 50, 100)) {
                    for (Integer consensusReplicas : List.of(1, 2, 3, 5, 7, 100)) {
                        if (replicas <= nodes && consensusReplicas <= replicas) {
                            arg.add(Arguments.of(nodes, partitions, replicas, consensusReplicas));
                        }
                    }
                }
            }
        }

        return arg.stream();
    }

    @Test
    public void testConsensusGroupSizeValidation() {
        DistributionAlgorithm distributionAlgorithm = new RendezvousDistributionFunction();

        assertThrows(AssertionError.class, () -> distributionAlgorithm.assignPartitions(prepareNetworkTopology(3), emptyList(), 1, 3, 4));
    }

    private static List<String> prepareNetworkTopology(int nodes) {
        return IntStream.range(0, nodes)
                .mapToObj(i -> "Node " + i)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}. Two nearby numbers with difference at most 1 are compacted
     * to one continuous segment. E.g. collection of [1, 2, 3, 5, 6, 7, 10] will be compacted to [1-3, 5-7, 10].
     *
     * @param col Collection of integers.
     * @return Compacted string representation of given collections.
     */
    private static String compact(Collection<Integer> col) {
        return compact(col, i -> i + 1);
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}. Two nearby numbers are compacted to one continuous segment.
     * E.g. collection of [1, 2, 3, 5, 6, 7, 10] with {@code nextValFun = i -> i + 1} will be compacted to [1-3, 5-7, 10].
     *
     * @param col        Collection of numbers.
     * @param nextValFun Function to get nearby number.
     * @return Compacted string representation of given collections.
     */
    private static <T extends Number & Comparable<? super T>> String compact(
            Collection<T> col,
            Function<T, T> nextValFun
    ) {
        assert nonNull(col);
        assert nonNull(nextValFun);

        if (col.isEmpty()) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');

        List<T> l = new ArrayList<>(col);
        Collections.sort(l);

        T left = l.get(0);
        T right = left;
        for (int i = 1; i < l.size(); i++) {
            T val = l.get(i);

            if (right.compareTo(val) == 0 || nextValFun.apply(right).compareTo(val) == 0) {
                right = val;
                continue;
            }

            if (left.compareTo(right) == 0) {
                sb.append(left);
            } else {
                sb.append(left).append('-').append(right);
            }

            sb.append(',').append(' ');

            left = right = val;
        }

        if (left.compareTo(right) == 0) {
            sb.append(left);
        } else {
            sb.append(left).append('-').append(right);
        }

        sb.append(']');

        return sb.toString();
    }
}
