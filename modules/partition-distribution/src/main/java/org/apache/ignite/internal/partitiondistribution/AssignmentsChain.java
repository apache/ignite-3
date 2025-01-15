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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Contains the chain of changed assignments.
 */
public class AssignmentsChain {

    private static final long DEFAULT_CONF_TERM = -1;
    private static final long DEFAULT_CONF_IDX = -1;

    /** Chain of assignments. */
    @IgniteToStringInclude
    private final List<AssignmentsLink> chain;

    private AssignmentsChain(List<AssignmentsLink> chain) {
        this.chain = chain;
    }

    public List<AssignmentsLink> chain() {
        return chain;
    }

    /**
     * Create a new {@link AssignmentsChain} with the last link in the chain replaced with the provided one.
     *
     * @param newLast New last link.
     * @return new AssignmentsChain.
     */
    public AssignmentsChain replaceLast(Assignments newLast, long configurationTerm, long configurationIndex) {
        assert !chain.isEmpty() : "Assignments chain is empty.";

        List<AssignmentsLink> newChain = new ArrayList<>(chain);

        newChain.set(newChain.size() - 1, new AssignmentsLink(newLast, configurationTerm, configurationIndex));

        return new AssignmentsChain(newChain);
    }

    /**
     * Create a new {@link AssignmentsChain} with a new link added to the chain.
     *
     * @param newLast New last link.
     * @return new AssignmentsChain.
     */
    public AssignmentsChain addLast(Assignments newLast, long configurationTerm, long configurationIndex) {
        assert !chain.isEmpty() : "Assignments chain is empty.";

        List<AssignmentsLink> newChain = new ArrayList<>(chain);

        newChain.add(new AssignmentsLink(newLast, configurationTerm, configurationIndex));

        return new AssignmentsChain(newChain);
    }


    /**
     * Gets the next link in the chain after the given link.
     *
     * @param link The link to get the next one from.
     * @return The next link in the chain, or {@code null} if the given link is the last one in the chain.
     */
    public @Nullable AssignmentsLink nextLink(AssignmentsLink link) {
        int i = chain.indexOf(link);

        return i < 0 || i == chain().size() - 1 ? null : chain.get(i + 1);
    }

    /**
     * Returns the last {@link AssignmentsLink} in the chain that contains the specified node.
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

    public static AssignmentsChain of(Assignments... assignments) {
        return of(DEFAULT_CONF_TERM, DEFAULT_CONF_IDX, assignments);
    }

    /**
     * Creates a new instance.
     *
     * @param assignments Partition assignments.
     */
    public static AssignmentsChain of(long configurationTerm, long configurationIndex, Assignments... assignments) {
        List<AssignmentsLink> links =
                Stream.of(assignments)
                        .map(assignment -> new AssignmentsLink(assignment, configurationTerm, configurationIndex))
                        .collect(toList());

        return new AssignmentsChain(links);
    }

    /**
     * Creates a new instance.
     *
     * @param assignmentsChain Chain of partition assignments.
     */
    public static AssignmentsChain of(List<AssignmentsLink> assignmentsChain) {
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

}
