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
import java.util.Collections;
import java.util.HashSet;
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
    public static final Assignments EMPTY = new Assignments(Collections.emptySet());

    /** Set of nodes. */
    @IgniteToStringInclude
    private final Set<Assignment> nodes;

    /**
     * Constructor.
     *
     * @param nodes Set of nodes.
     */
    private Assignments(Set<Assignment> nodes) {
        this.nodes = nodes;
    }

    /**
     * Creates a new instance.
     *
     * @param nodes Set of nodes.
     */
    public static Assignments of(Set<Assignment> nodes) {
        return new Assignments(new HashSet<>(nodes));
    }

    /**
     * Creates a new instance.
     *
     * @param nodes Array of nodes.
     */
    public static Assignments of(Assignment... nodes) {
        return new Assignments(Set.of(nodes));
    }

    /**
     * Returns a set of nodes, represented by this assignments instance.
     */
    public Set<Assignment> nodes() {
        return unmodifiableSet(nodes);
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
        return new Assignments(assignments).toBytes();
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
}
