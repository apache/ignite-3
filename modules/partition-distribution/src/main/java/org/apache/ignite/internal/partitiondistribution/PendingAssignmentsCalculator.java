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

import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.IgniteUtils.newHashSet;

import java.util.Objects;
import java.util.Set;

/**
 * Calculates the pending assignments queue between current stable and target assignments.
 */
public class PendingAssignmentsCalculator {
    private Assignments stable;
    private Assignments target;

    private PendingAssignmentsCalculator() {}

    public static PendingAssignmentsCalculator pendingAssignmentsCalculator() {
        return new PendingAssignmentsCalculator();
    }

    /**
     * Current stable assignments.
     */
    public PendingAssignmentsCalculator stable(Assignments stable) {
        this.stable = stable;
        return this;
    }

    /**
     * Target assignments.
     */
    public PendingAssignmentsCalculator target(Assignments target) {
        this.target = target;
        return this;
    }

    /**
     * Calculates the pending assignments queue between current stable (not included) and target assignments (included, last element).
     */
    public AssignmentsQueue toQueue() {
        assert stable != null;
        assert target != null;

        if (target.force() || target.fromReset()) {
            return new AssignmentsQueue(target);
        }

        int size = target.nodes().size();
        Set<Assignment> base = newHashSet(size);
        Set<Assignment> promoted = newHashSet(size);
        Set<Assignment> demoted = newHashSet(size);
        Set<Assignment> demotedPeers = newHashSet(size);

        for (Assignment t : target.nodes()) {
            boolean found = false;

            for (Assignment s : stable.nodes()) {
                if (s.equals(t)) {
                    found = true;
                    base.add(t);
                    continue;
                }

                if (t.consistentId().equals(s.consistentId())) {
                    found = true;

                    if (!s.isPeer() && t.isPeer()) {
                        promoted.add(t);
                    }

                    if (s.isPeer() && !t.isPeer()) {
                        demoted.add(t);
                        demotedPeers.add(s); // prevent majority loss by keeping old peers at first step
                    }

                }

            }

            if (!found) {
                base.add(t); // add new items safely
            }
        }

        AssignmentsQueue queue;

        if (promoted.isEmpty() && demoted.isEmpty()) {
            queue = new AssignmentsQueue(Assignments.of(base, target.timestamp()));

        } else if (promoted.isEmpty() || demoted.isEmpty()) {
            Set<Assignment> withPromotedOrDemoted = union(base, promoted.isEmpty() ? demoted : promoted);
            queue = new AssignmentsQueue(
                    Assignments.of(base, target.timestamp()),
                    Assignments.of(withPromotedOrDemoted, target.timestamp())
            );

        } else {
            Set<Assignment> withOldPeers = union(base, demotedPeers); // prevent majority loss by keeping old peers at first step
            Set<Assignment> withPromoted = union(base, promoted);
            Set<Assignment> withDemotedPeers = union(withPromoted, demoted);

            queue = new AssignmentsQueue(
                    Assignments.of(withOldPeers, target.timestamp()),
                    Assignments.of(withPromoted, target.timestamp()),
                    Assignments.of(withDemotedPeers, target.timestamp())
            );
        }

        assert Objects.equals(queue.peekLast().nodes(), target.nodes())
                : "Target assignments should be equal to the last element in the queue";
        return queue;
    }
}
