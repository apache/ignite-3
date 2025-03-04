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

import java.util.Objects;

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
     * Calculates the pending assignments queue between current stable and target assignments.
     */
    public AssignmentsQueue toQueue() {
        assert stable != null;
        assert target != null;

        // naive version with only one configuration switch, calculation will be changed during IGNITE-23790 epic
        AssignmentsQueue queue = new AssignmentsQueue(target);
        assert Objects.equals(queue.peekLast(), target) : "Target assignments should be equal to the last element in the queue";
        return queue;
    }
}
