/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.lang.util.SerializationUtils;
import org.apache.ignite.network.NetworkMember;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for affinity function.
 */
public class RendezvousAffinityFunctionTest {

    /** Affinity deviation ratio. */
    public static final double AFFINITY_DEVIATION_RATIO = 0.2;

    /** Logger. */
    private LogWrapper log = new LogWrapper(RendezvousAffinityFunctionTest.class);

    @Test
    public void partitionDistribution() {
        int nodes = 50;

        int parts = 10_000;

        int replicas = 4;

        ArrayList<NetworkMember> members = prepareNetworkTopology(nodes);

        assertTrue(parts > nodes, "Partitions should be more that nodes");

        int ideal = (parts * replicas) / nodes;

        List<List<NetworkMember>> assignment = RendezvousAffinityFunction.assignPartitions(
            members,
            parts,
            replicas,
            false,
            null
        );

        HashMap<NetworkMember, ArrayList<Integer>> assignmentByMember = new HashMap<>(nodes);

        int part = 0;

        for (List<NetworkMember> partMembers : assignment) {
            for (NetworkMember member : partMembers) {
                ArrayList<Integer> memberParts = assignmentByMember.get(member);

                if (memberParts == null)
                    assignmentByMember.put(member, memberParts = new ArrayList<>());

                memberParts.add(part);
            }

            part++;
        }

        for (NetworkMember member : members) {
            ArrayList<Integer> memberParts = assignmentByMember.get(member);

            assertNotNull(memberParts);

            assertTrue(memberParts.size() > ideal * (1 - AFFINITY_DEVIATION_RATIO)
                    && memberParts.size() < ideal * (1 + AFFINITY_DEVIATION_RATIO),
                "Partition distribution too fare from ideal [member=" + member
                    + ", size=" + memberParts.size()
                    + ", idealSize=" + ideal
                    + ", parts=" + compact(memberParts) + ']');
        }
    }

    @NotNull private ArrayList<NetworkMember> prepareNetworkTopology(int nodes) {
        ArrayList<NetworkMember> members = new ArrayList<>(nodes);

        for (int i = 0; i < nodes; i++)
            members.add(new NetworkMember("Member " + i));
        return members;
    }

    @Test
    public void serializeAssignment() {
        int nodes = 50;

        int parts = 10_000;

        int replicas = 4;

        ArrayList<NetworkMember> members = prepareNetworkTopology(nodes);

        assertTrue(parts > nodes, "Partitions should be more that nodes");

        List<List<NetworkMember>> assignment = RendezvousAffinityFunction.assignPartitions(
            members,
            parts,
            replicas,
            false,
            null
        );

        byte[] assignmentBytes = SerializationUtils.toBytes(assignment);

        assertNotNull(assignment);

        log.info("Assignment is serialized successfully [bytes=" + assignmentBytes.length + ']');

        List<List<NetworkMember>> deserializedAssignment = (List<List<NetworkMember>>)SerializationUtils.fromBytes(assignmentBytes);

        assertNotNull(deserializedAssignment);

        assertEquals(assignment, deserializedAssignment);
    }



    /**
     * Returns sorted and compacted string representation of given {@code col}. Two nearby numbers with difference at
     * most 1 are compacted to one continuous segment. E.g. collection of [1, 2, 3, 5, 6, 7, 10] will be compacted to
     * [1-3, 5-7, 10].
     *
     * @param col Collection of integers.
     * @return Compacted string representation of given collections.
     */
    public static String compact(Collection<Integer> col) {
        return compact(col, i -> i + 1);
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}. Two nearby numbers are compacted to one
     * continuous segment. E.g. collection of [1, 2, 3, 5, 6, 7, 10] with {@code nextValFun = i -> i + 1} will be
     * compacted to [1-3, 5-7, 10].
     *
     * @param col Collection of numbers.
     * @param nextValFun Function to get nearby number.
     * @return Compacted string representation of given collections.
     */
    public static <T extends Number & Comparable<? super T>> String compact(
        Collection<T> col,
        Function<T, T> nextValFun
    ) {
        assert nonNull(col);
        assert nonNull(nextValFun);

        if (col.isEmpty())
            return "[]";

        StringBuffer sb = new StringBuffer();
        sb.append('[');

        List<T> l = new ArrayList<>(col);
        Collections.sort(l);

        T left = l.get(0), right = left;
        for (int i = 1; i < l.size(); i++) {
            T val = l.get(i);

            if (right.compareTo(val) == 0 || nextValFun.apply(right).compareTo(val) == 0) {
                right = val;
                continue;
            }

            if (left.compareTo(right) == 0)
                sb.append(left);
            else
                sb.append(left).append('-').append(right);

            sb.append(',').append(' ');

            left = right = val;
        }

        if (left.compareTo(right) == 0)
            sb.append(left);
        else
            sb.append(left).append('-').append(right);

        sb.append(']');

        return sb.toString();
    }
}
