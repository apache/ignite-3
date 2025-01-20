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

import static java.util.stream.Collectors.toSet;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a link in the chain of assignments.
 *
 * <p>An AssignmentsLink instance encapsulates a set of node assignments along with the associated
 * configuration term and index. This is used to keep track of changes in the node assignments for a partition over time.
 */
public class AssignmentsLink {
    private final Assignments assignments;
    private final long configurationIndex;
    private final long configurationTerm;
    private final Function<AssignmentsLink, AssignmentsLink> nextLinkFunc;

    AssignmentsLink(
            Assignments assignments,
            long configurationTerm,
            long configurationIndex,
            @Nullable Function<AssignmentsLink, AssignmentsLink> nextLinkFunc
    ) {
        this.assignments = assignments;
        this.configurationIndex = configurationIndex;
        this.configurationTerm = configurationTerm;
        this.nextLinkFunc = nextLinkFunc;
    }

    AssignmentsLink(
            AssignmentsLink other,
            @Nullable Function<AssignmentsLink, AssignmentsLink> nextLinkFunc
    ) {
        this.assignments = other.assignments;
        this.configurationIndex = other.configurationIndex;
        this.configurationTerm = other.configurationTerm;
        this.nextLinkFunc = nextLinkFunc;
    }

    public Assignments assignments() {
        return assignments;
    }

    /**
     * Gets the next link in the chain after the given link.
     *
     * @return The next link in the chain, or {@code null} if the given link is the last one in the chain.
     */
    public @Nullable AssignmentsLink nextLink() {
        return nextLinkFunc == null ? null : nextLinkFunc.apply(this);
    }

    /**
     * Checks if the specified node is part of the current assignments.
     *
     * @param nodeConsistentId The consistent identifier of the node to check.
     * @return {@code true} if the node is present in the assignments, otherwise {@code false}.
     */

    public boolean hasNode(String nodeConsistentId) {
        return assignments.nodes().stream().map(Assignment::consistentId).anyMatch(consistentId -> consistentId.equals(nodeConsistentId));
    }

    /**
     * Returns a set of consistent nodes present in the current assignments.
     *
     * @return Set of consistent node identifiers.
     */
    public Set<String> nodeNames() {
        return assignments.nodes().stream().map(Assignment::consistentId).collect(toSet());
    }

    public long configurationIndex() {
        return configurationIndex;
    }

    public long configurationTerm() {
        return configurationTerm;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AssignmentsLink link = (AssignmentsLink) o;
        return configurationIndex == link.configurationIndex && configurationTerm == link.configurationTerm && Objects.equals(
                assignments, link.assignments);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(assignments);
        result = 31 * result + Long.hashCode(configurationIndex);
        result = 31 * result + Long.hashCode(configurationTerm);
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
