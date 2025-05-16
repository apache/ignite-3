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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import org.junit.jupiter.api.Test;

class PendingAssignmentsCalculatorTest {

    @Test
    void testAddRemoveOnly() {
        Assignments stable = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forLearner("remove")
        ), 0L);

        Assignments target = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forPeer("newPeer"),
                Assignment.forLearner("newLearner")
        ), 0L);

        AssignmentsQueue queue = PendingAssignmentsCalculator.pendingAssignmentsCalculator()
                .stable(stable)
                .target(target)
                .toQueue();

        assertThat("adding/removing without promote/demote in single configuration switch", queue.size(), is(1));
        assertThat("queue first element is equal to target", queue.peekFirst(), is(target));
    }

    @Test
    void testPromoteOnly() {
        Assignments stable = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forLearner("promote")
        ), 0L);

        Assignments target = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forPeer("promote")
        ), 0L);

        AssignmentsQueue queue = PendingAssignmentsCalculator.pendingAssignmentsCalculator()
                .stable(stable)
                .target(target)
                .toQueue();

        assertThat("promote in two configuration switches", queue.size(), is(2));
        assertThat(queue.peekFirst().nodes(), contains(Assignment.forPeer("asIs")));
        assertThat(queue.peekLast().nodes(), containsInAnyOrder(Assignment.forPeer("asIs"), Assignment.forPeer("promote")));
    }

    @Test
    void testDemoteOnly() {
        Assignments stable = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forPeer("demote")
        ), 0L);

        Assignments target = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forLearner("demote")
        ), 0L);

        AssignmentsQueue queue = PendingAssignmentsCalculator.pendingAssignmentsCalculator()
                .stable(stable)
                .target(target)
                .toQueue();

        assertThat("demote only in two configuration switches", queue.size(), is(2));
        assertThat(queue.peekFirst().nodes(), contains(Assignment.forPeer("asIs")));
        assertThat(queue.peekLast().nodes(), containsInAnyOrder(Assignment.forPeer("asIs"), Assignment.forLearner("demote")));

    }

    @Test
    void testComplexAddRemovePromoteDemote() {
        Assignments stable = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forLearner("promote"),
                Assignment.forPeer("demote"),
                Assignment.forLearner("remove")
        ), 0L);

        Assignments target = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forPeer("promote"),
                Assignment.forLearner("demote"),
                Assignment.forLearner("newLearner"),
                Assignment.forPeer("newPeer")
        ), 0L);

        AssignmentsQueue queue = PendingAssignmentsCalculator.pendingAssignmentsCalculator()
                .stable(stable)
                .target(target)
                .toQueue();

        assertThat("promoting/demoting elements in three configuration switches", queue.size(), is(3));
        assertThat("first configuration switch removes promoted learners, contains new elements, keeps old peers to prevent majority loss",
                queue.poll().nodes(), containsInAnyOrder(
                        Assignment.forPeer("asIs"),
                        Assignment.forPeer("demote"), // still peer
                        Assignment.forLearner("newLearner"),
                        Assignment.forPeer("newPeer")
                ));
        assertThat("second configuration switch removes demoted peers, adds promoted learners (as peers)",
                queue.poll().nodes(), containsInAnyOrder(
                        Assignment.forPeer("asIs"),
                        Assignment.forPeer("promote"),
                        Assignment.forLearner("newLearner"),
                        Assignment.forPeer("newPeer")
                ));
        assertThat("third configuration switch matches target", queue.poll(), is(target));
    }

    @Test
    void testForceFlag() {
        Assignments stable = Assignments.of(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forLearner("promote"),
                Assignment.forPeer("demote"),
                Assignment.forLearner("remove")
        ), 0L);

        // without forced should be 3 elements in queue
        Assignments target = Assignments.forced(Set.of(
                Assignment.forPeer("asIs"),
                Assignment.forPeer("promote"),
                Assignment.forLearner("demote"),
                Assignment.forLearner("newLearner"),
                Assignment.forPeer("newPeer")
        ), 0L);

        AssignmentsQueue queue = PendingAssignmentsCalculator.pendingAssignmentsCalculator()
                .stable(stable)
                .target(target)
                .toQueue();

        assertThat("forcing elements in single configuration switch", queue.size(), is(1));
        assertThat("queue first element is equal to target", queue.peekFirst(), is(target));
    }
}
