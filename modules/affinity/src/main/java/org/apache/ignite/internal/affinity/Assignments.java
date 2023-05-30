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

import static org.apache.ignite.internal.affinity.Assignment.forLearner;
import static org.apache.ignite.internal.affinity.Assignment.forPeer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Represents affinity assignments.
 */
public class Assignments {
    private final List<Set<Assignment>> assignments;

    public Assignments(List<Set<Assignment>> assignments) {
        this.assignments = assignments;
    }

    public List<Set<Assignment>> assignments() {
        return assignments;
    }

    /**
     * Partitions count.
     *
     * @return Partitions count.
     */
    public int size() {
        return assignments.size();
    }

    /**
     * Gets an assignments for the given partition.
     *
     * @param part Partition.
     * @return Assignments for the given partition.
     */
    public Set<Assignment> get(int part) {
        return assignments.get(part);
    }

    /**
     * Sets an assignment for the given partition.
     *
     * @param part Partition.
     * @param assignment Assignments for the given partition.
     */
    public void set(int part, Set<Assignment> assignment) {
        assignments.set(part, assignment);
    }

    /**
     * Bytes representation of the assignments.
     *
     * @return Bytes representation of the assignments.
     */
    public byte[] bytes() {
        Map<String, Integer> consistentIds = new LinkedHashMap<>();

        int idx = 0;
        for (Set<Assignment> assignmentSet : assignments) {
            for (Assignment a : assignmentSet) {
                if (!consistentIds.containsKey(a.consistentId())) {
                    consistentIds.put(a.consistentId(), idx++);
                }
            }
        }

        byte[] consistentIdsBytes = collectionToBytes(consistentIds.keySet(), String::getBytes);

        byte[] assignmentsBytes = collectionToBytes(
                assignments,
                set -> collectionToBytes(set, a -> assignmentToBytes(a, consistentIds::get))
        );

        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * 2 + consistentIdsBytes.length + assignmentsBytes.length);

        buf.putInt(consistentIdsBytes.length);
        buf.put(consistentIdsBytes);
        buf.putInt(assignmentsBytes.length);
        buf.put(assignmentsBytes);

        return buf.array();
    }

    /**
     * Creates {@link Assignments} from the given byte array.
     *
     * @param bytes Byte array representation of the assignments.
     * @return Assignments.
     */
    public static Assignments fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        int consistentIdsBytesLength = buf.getInt();
        byte[] consistentIdsBytes = new byte[consistentIdsBytesLength];
        buf.get(consistentIdsBytes);

        int assignmentsBytesLength = buf.getInt();
        byte[] assignmentsBytes = new byte[assignmentsBytesLength];
        buf.get(assignmentsBytes);

        List<String> consistentIds = bytesToList(consistentIdsBytes, b -> new String(b, StandardCharsets.UTF_8));

        List<Set<Assignment>> assignments = bytesToList(
                assignmentsBytes,
                b -> new HashSet<>(bytesToList(b, ab -> bytesToAssignment(ab, consistentIds::get)))
        );

        return new Assignments(assignments);
    }

    /**
     * Serializes collection to bytes.
     *
     * @param collection Collection.
     * @param transform Tranform function for the collection element.
     * @return Byte array.
     */
    private <T> byte[] collectionToBytes(Collection<T> collection, Function<T, byte[]> transform) {
        int bytesObjects = 0;
        List<byte[]> objects = new ArrayList<>();

        for (T o : collection) {
            byte[] b = transform.apply(o);
            objects.add(b);
            bytesObjects += b.length;
        }

        bytesObjects += Short.BYTES * (objects.size() + 1);

        ByteBuffer buf = ByteBuffer.allocate(bytesObjects);

        buf.putShort((short) objects.size());

        for (byte[] o : objects) {
            buf.putShort((short) o.length);
            buf.put(o);
        }

        return buf.array();
    }

    /**
     * Deserializes the list from byte array.
     *
     * @param bytes Byte array.
     * @param transform Transform function to create list element.
     * @return List.
     */
    private static <T> List<T> bytesToList(byte[] bytes, Function<byte[], T> transform) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        short length = buf.getShort();
        List<T> result = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            short size = buf.getShort();
            byte[] arr = new byte[size];
            buf.get(arr);
            result.add(transform.apply(arr));
        }

        return result;
    }

    private static byte[] assignmentToBytes(Assignment assignment, Function<String, Integer> consistentIdToIndex) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Byte.BYTES);
        buf.putInt(consistentIdToIndex.apply(assignment.consistentId()));
        buf.put((byte) (assignment.isPeer() ? 1 : 0));
        return buf.array();
    }

    private static Assignment bytesToAssignment(byte[] bytes, Function<Integer, String> indexToConsistentId) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int index = buf.getInt();
        boolean isPeer = buf.get() == 1;
        String consistentId = indexToConsistentId.apply(index);
        return isPeer ? forPeer(consistentId) : forLearner(consistentId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Assignments that = (Assignments) o;

        return assignments != null ? assignments.equals(that.assignments) : that.assignments == null;
    }

    @Override
    public int hashCode() {
        return assignments != null ? assignments.hashCode() : 0;
    }
}
