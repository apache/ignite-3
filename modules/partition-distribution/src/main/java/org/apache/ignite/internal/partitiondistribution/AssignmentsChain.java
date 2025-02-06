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

import static java.util.stream.Collectors.toList;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Contains the chain of changed assignments.
 */
public class AssignmentsChain implements Iterable<AssignmentsLink> {
    /** Chain of assignments. */
    @IgniteToStringInclude
    private final List<AssignmentsLink> chain;

    private AssignmentsChain(List<AssignmentsLink> chain) {
        assert !chain.isEmpty() : "Chain should not be empty";

        this.chain = chain;
    }

    /**
     * Returns the number of links in this chain.
     *
     * @return the number of links in this chain.
     */
    public int size() {
        return chain.size();
    }

    /**
     * Replace the last link in the chain with the provided one.
     *
     * @param newLast New last link.
     * @return the created {@link AssignmentsLink}
     */
    public AssignmentsLink replaceLast(Assignments newLast, long configurationTerm, long configurationIndex) {
        assert !chain.isEmpty() : "Assignments chain is empty.";

        AssignmentsLink link = new AssignmentsLink(newLast, configurationTerm, configurationIndex);

        chain.set(chain.size() - 1, link);

        if (chain.size() > 1) {
            chain.get(chain.size() - 2).next(link);
        }

        return link;
    }

    /**
     * Add a new link to the end of the chain.
     *
     * @param newLast New last link.
     * @return the created {@link AssignmentsLink}
     */
    public AssignmentsLink addLast(Assignments newLast, long configurationTerm, long configurationIndex) {
        assert !chain.isEmpty() : "Assignments chain is empty.";

        AssignmentsLink link = new AssignmentsLink(newLast, configurationTerm, configurationIndex);

        chain.get(chain.size() - 1).next(link);

        chain.add(link);

        return link;
    }

    public AssignmentsLink firstLink() {
        return chain.get(0);
    }

    /**
     * Returns the last {@link AssignmentsLink} in the chain that contains the specified node. {@code}
     * <pre>
     *   on input link1([A,B,C,D,E,F,G]) > link2([E,F,G]) > link3([G])
     *   chain.lastLink(F) should return link2(E,F,G).
     * </pre>
     *
     * @param nodeConsistentId The consistent identifier of the node to search for.
     * @return The last {@link AssignmentsLink} that contains the node, or {@code null} if no such link exists.
     */
    public @Nullable AssignmentsLink lastLink(String nodeConsistentId) {
        for (int i = chain.size() - 1; i >= 0; i--) {
            AssignmentsLink link = chain.get(i);
            if (link.hasNode(nodeConsistentId)) {
                return link;
            }
        }

        return null;
    }

    /**
     * Creates a new instance. The configuration term and index are set to -1 for all links. Only for testing purposes.
     *
     * @param assignments Partition assignments.
     */
    @TestOnly
    public static AssignmentsChain of(Assignments... assignments) {
        return of(-1L, -1L, assignments);
    }

    /**
     * Creates a new instance.
     *
     * @param assignments Partition assignments.
     */
    public static AssignmentsChain of(long configurationTerm, long configurationIndex, Assignments... assignments) {
        return of(Stream.of(assignments)
                .map(assignment -> new AssignmentsLink(assignment, configurationTerm, configurationIndex))
                .collect(toList()));
    }

    /**
     * Creates a new instance.
     *
     * @param assignmentsChain Chain of partition assignments.
     */
    static AssignmentsChain of(List<AssignmentsLink> assignmentsChain) {
        for (int i = 1; i < assignmentsChain.size(); i++) {
            assignmentsChain.get(i - 1).next(assignmentsChain.get(i));
        }
        return new AssignmentsChain(assignmentsChain);
    }

    public byte[] toBytes() {
        return VersionedSerialization.toBytes(this, AssignmentsChainSerializer.INSTANCE);
    }

    /**
     * Deserializes assignments from the array of bytes. Returns {@code null} if the argument is {@code null}.
     */
    @Nullable
    @Contract("null -> null; !null -> !null")
    public static AssignmentsChain fromBytes(byte @Nullable [] bytes) {
        return bytes == null ? null : VersionedSerialization.fromBytes(bytes, AssignmentsChainSerializer.INSTANCE);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AssignmentsChain that = (AssignmentsChain) o;

        return Objects.equals(chain, that.chain);
    }

    @Override
    public int hashCode() {
        return chain.hashCode();
    }

    @Override
    public Iterator<AssignmentsLink> iterator() {
        return chain.iterator();
    }
}
