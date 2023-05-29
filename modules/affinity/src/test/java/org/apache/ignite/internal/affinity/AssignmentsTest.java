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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.affinity.Assignment.forLearner;
import static org.apache.ignite.internal.affinity.Assignment.forPeer;
import static org.apache.ignite.internal.affinity.Assignments.fromBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Assignments}.
 */
public class AssignmentsTest {
    @Test
    public void testSerializationDeserialization() {
        Assignments assignments;

        assignments = new Assignments(new ArrayList<>());
        assertEquals(assignments, fromBytes(assignments.bytes()));

        assignments = new Assignments(List.of(new HashSet<>()));
        assertEquals(assignments, fromBytes(assignments.bytes()));

        Set<Assignment> set1 = Set.of(forPeer("id1"), forLearner("id2"));
        Set<Assignment> set2 = Set.of(forPeer("id2"), forLearner("id1"));
        assignments = new Assignments(List.of(set1, set2));
        assertEquals(assignments, fromBytes(assignments.bytes()));

        List<String> consistentIds = IntStream.range(0, 100).mapToObj(i -> UUID.randomUUID().toString()).collect(toList());
        List<Set<Assignment>> a = new ArrayList<>();
        Random random = new Random();
        long seed = System.nanoTime();
        random.setSeed(seed);
        for (int i = 0; i < 1000; i++) {
            Set<Assignment> set = new HashSet<>();
            if (i % 10 == 0) {
                for (int j = 0; j < 1000; j++) {
                    if (j % 10 == 0) {
                        set.add(forPeer(consistentIds.get(random.nextInt(100))));
                    } else {
                        set.add(forLearner(consistentIds.get(random.nextInt(100))));
                    }
                }
            } else {
                for (int j = 0; j < 5; j++) {
                    set.add(forPeer(consistentIds.get(random.nextInt(100))));
                }
            }

            a.add(set);
        }
        assignments = new Assignments(a);
        assertEquals(assignments, fromBytes(assignments.bytes()), "assignments are not equal, seed = " + seed);
    }
}
