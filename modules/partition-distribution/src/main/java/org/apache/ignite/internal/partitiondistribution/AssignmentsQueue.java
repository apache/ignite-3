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

import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Class that encapsulates a queue with consequent configuration switches
 * e.g. promoting a learner to a peer or demoting a peer to a learner in following steps:
 * <ul>
 *   <li> node removed from configuration
 *   <li> node added to configuration with different type (peer or learner)
 * </ul>
 */
public class AssignmentsQueue implements Iterable<Assignments> {
    @IgniteToStringInclude
    private final Deque<Assignments> queue;

    /** Constructor. */
    public AssignmentsQueue(AssignmentsQueue... assignmentsQueues) {
        LinkedList<Assignments> assigments = new LinkedList<>();
        for (AssignmentsQueue assignmentsQueue : assignmentsQueues) {
            assigments.addAll(assignmentsQueue.queue);
        }
        this.queue = assigments;
    }

    /** Constructor. */
    public AssignmentsQueue(Assignments... assignments) {
        this(Arrays.asList(assignments));
    }

    /** Constructor. */
    AssignmentsQueue(Collection<Assignments> assignments) {
        this.queue = new LinkedList<>(assignments);
    }

    /**
     * Retrieves and removes the head of this queue.
     *
     * @return the head of this queue
     */
    public Assignments poll() {
        assert !queue.isEmpty() : "Assignments queue must contain at least one element.";
        return queue.poll();
    }

    /**
     * Retrieves, but does not remove, the first element of this queue.
     *
     * @return the tail of this queue
     */
    public Assignments peekFirst() {
        assert !queue.isEmpty() : "Assignments queue must contain at least one element.";
        return queue.peekFirst();
    }

    /**
     * Retrieves, but does not remove, the last element of this queue.
     *
     * @return the tail of this queue
     */
    public Assignments peekLast() {
        assert !queue.isEmpty() : "Assignments queue must contain at least one element.";
        return queue.peekLast();
    }

    public byte[] toBytes() {
        return VersionedSerialization.toBytes(this, AssignmentsQueueSerializer.INSTANCE);
    }

    public static byte[] toBytes(Assignments... assignments) {
        return toBytes(Arrays.asList(assignments));
    }

    public static byte[] toBytes(Collection<Assignments> assignments) {
        return new AssignmentsQueue(assignments).toBytes();
    }

    @Nullable
    @Contract("null -> null; !null -> !null")
    public static AssignmentsQueue fromBytes(byte @Nullable [] bytes) {
        return bytes == null ? null : VersionedSerialization.fromBytes(bytes, AssignmentsQueueSerializer.INSTANCE);
    }

    /**
     * Returns the number of elements in this queue.
     *
     * @return the number of elements in this queue
     */
    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Returns an iterator over the elements in this queue in proper sequence.
     * The elements will be returned in order from first (head) to last (tail).
     *
     * @return an iterator over the elements in this queue in proper sequence
     */
    @Override
    public Iterator<Assignments> iterator() {
        return queue.iterator();
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AssignmentsQueue that = (AssignmentsQueue) o;
        return Objects.equals(queue, that.queue);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queue);
    }
}
