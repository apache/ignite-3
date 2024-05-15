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

import static java.util.Collections.unmodifiableSet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ByteUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Class that encapsulates a set of nodes and its metadata.
 */
public class Assignments implements Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = -59553172012153869L;

    /** Empty assignments. */
    public static final Assignments EMPTY = new Assignments(Collections.emptySet(), false);

    /** Set of nodes. */
    @IgniteToStringInclude
    private final HashSet<Assignment> nodes;

    /**
     * Force flag.
     *
     * @see #force()
     */
    private final boolean force;

    /**
     * Constructor.
     */
    private Assignments(Collection<Assignment> nodes, boolean force) {
        // A set of nodes must be a HashSet in order for serialization to produce stable results,
        // that could be compared as byte arrays.
        this.nodes = nodes instanceof HashSet ? ((HashSet<Assignment>) nodes) : new HashSet<>(nodes);
        this.force = force;
    }

    /**
     * Creates a new instance.
     *
     * @param nodes Set of nodes.
     */
    public static Assignments of(Set<Assignment> nodes) {
        return new Assignments(nodes, false);
    }

    /**
     * Creates a new instance.
     *
     * @param nodes Array of nodes.
     */
    public static Assignments of(Assignment... nodes) {
        return new Assignments(Arrays.asList(nodes), false);
    }

    /**
     * Creates a new instance with {@code force} flag set on.
     *
     * @param nodes Set of nodes.
     * @see #force()
     */
    public static Assignments forced(Set<Assignment> nodes) {
        return new Assignments(nodes, true);
    }

    /**
     * Returns a set of nodes, represented by this assignments instance.
     */
    public Set<Assignment> nodes() {
        return unmodifiableSet(nodes);
    }

    /**
     * Returns a force flag that indicates ignoring of current stable assignments. Force flag signifies that previous assignments for the
     * group must be ignored, and new assignments must be accepted without questions. This flag should be used in disaster recovery
     * situations when there's no way to recover the group using regular reassignment. For example, group majority is lost.
     */
    public boolean force() {
        return force;
    }

    /**
     * Adds an assignment to this collection of assignments.
     *
     * @param assignment Assignment to add.
     */
    public void add(Assignment assignment) {
        nodes.add(assignment);
    }

    /**
     * Returns {@code true} if this collection has no assignments, {@code false} if it has some assignments.
     */
    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    /**
     * Serializes the instance into an array of bytes.
     */
    public byte[] toBytes() {
        return ByteUtils.toBytes(this);
    }

    /**
     * Serializes assignments into an array of bytes.
     *
     * @see #toBytes()
     */
    public static byte[] toBytes(Set<Assignment> assignments) {
        return new Assignments(assignments, false).toBytes();
    }

    /**
     * Deserializes assignments from the array of bytes. Returns {@code null} if the argument is {@code null}.
     */
    @Nullable
    @Contract("null -> null; !null -> !null")
    public static Assignments fromBytes(byte @Nullable [] bytes) {
        return bytes == null ? null : ByteUtils.fromBytes(bytes);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Creates a string representation of the given assignments list for logging usage purpose mostly.
     *
     * @param assignments List of assignments to present as string.
     * @return String representation of the given assignments list.
     */
    public static String assignmentListToString(List<Assignments> assignments) {
        return S.toString(assignments, (sb, e, i) -> sb.app(i).app('=').app(e.nodes()));
    }
}
