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

package org.apache.ignite.internal.affinity;

import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;

/**
 * Test for affinity function.
 */
public class RendezvousAffinityFunctionTest {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RendezvousAffinityFunctionTest.class);

    /** Affinity deviation ratio. */
    public static final double AFFINITY_DEVIATION_RATIO = 0.2;

    @Test
    public void testPartitionDistribution() {
        int nodeCount = 50;

        int parts = 10_000;

        int replicas = 4;

        List<String> nodes = prepareNetworkTopology(nodeCount);

        assertTrue(parts > nodeCount, "Partitions should be more than nodes");

        int ideal = (parts * replicas) / nodeCount;

        List<List<String>> assignment = RendezvousAffinityFunction.assignPartitions(
                nodes,
                parts,
                replicas,
                false,
                null
        );

        HashMap<String, ArrayList<Integer>> assignmentByNode = new HashMap<>(nodeCount);

        int part = 0;

        for (List<String> partNodes : assignment) {
            for (String node : partNodes) {
                ArrayList<Integer> nodeParts = assignmentByNode.get(node);

                if (nodeParts == null) {
                    assignmentByNode.put(node, nodeParts = new ArrayList<>());
                }

                nodeParts.add(part);
            }

            part++;
        }

        for (String node : nodes) {
            ArrayList<Integer> nodeParts = assignmentByNode.get(node);

            assertNotNull(nodeParts);

            assertTrue(nodeParts.size() > ideal * (1 - AFFINITY_DEVIATION_RATIO)
                            && nodeParts.size() < ideal * (1 + AFFINITY_DEVIATION_RATIO),
                    "Partition distribution is too far from ideal [node=" + node
                            + ", size=" + nodeParts.size()
                            + ", idealSize=" + ideal
                            + ", parts=" + compact(nodeParts) + ']');
        }
    }

    private List<String> prepareNetworkTopology(int nodes) {
        return IntStream.range(0, nodes)
                .mapToObj(i -> "Node " + i)
                .collect(Collectors.toUnmodifiableList());
    }

    @Test
    public void serializeAssignment() {
        int nodeCount = 50;

        int parts = 10_000;

        int replicas = 4;

        List<String> nodes = prepareNetworkTopology(nodeCount);

        assertTrue(parts > nodeCount, "Partitions should be more than nodes");

        List<List<String>> assignment = RendezvousAffinityFunction.assignPartitions(
                nodes,
                parts,
                replicas,
                false,
                null
        );

        byte[] assignmentBytes = ByteUtils.toBytes(assignment);

        LOG.info("Assignment is serialized successfully [bytes={}]", assignmentBytes.length);

        List<List<ClusterNode>> deserializedAssignment = (List<List<ClusterNode>>) ByteUtils.fromBytes(assignmentBytes);

        assertEquals(assignment, deserializedAssignment);
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}. Two nearby numbers with difference at most 1 are compacted
     * to one continuous segment. E.g. collection of [1, 2, 3, 5, 6, 7, 10] will be compacted to [1-3, 5-7, 10].
     *
     * @param col Collection of integers.
     * @return Compacted string representation of given collections.
     */
    public static String compact(Collection<Integer> col) {
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
    public static <T extends Number & Comparable<? super T>> String compact(
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
