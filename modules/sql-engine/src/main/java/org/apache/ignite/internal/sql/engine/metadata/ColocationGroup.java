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

package org.apache.ignite.internal.sql.engine.metadata;

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.firstNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * ColocationGroup.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ColocationGroup implements Serializable {
    private final List<Long> sourceIds;

    private final List<String> nodeNames;

    private final List<List<NodeWithTerm>> assignments;

    /**
     * ForNodes.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static ColocationGroup forNodes(List<String> nodeNames) {
        return new ColocationGroup(null, nodeNames, null);
    }

    /**
     * ForAssignments.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static ColocationGroup forAssignments(List<List<NodeWithTerm>> assignments) {
        return new ColocationGroup(null, null, assignments);
    }

    /**
     * ForSourceId.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static ColocationGroup forSourceId(long sourceId) {
        return new ColocationGroup(Collections.singletonList(sourceId), null, null);
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    private ColocationGroup(List<Long> sourceIds, List<String> nodeNames, List<List<NodeWithTerm>> assignments) {
        this.sourceIds = sourceIds;
        this.nodeNames = nodeNames;
        this.assignments = assignments;
    }

    /**
     * Get lists of colocation group sources.
     */
    public List<Long> sourceIds() {
        return sourceIds == null ? Collections.emptyList() : sourceIds;
    }

    /**
     * Get lists of nodes capable to execute a query fragment for what the mapping is calculated.
     */
    public List<String> nodeNames() {
        return nodeNames == null ? Collections.emptyList() : nodeNames;
    }

    /**
     * Get list of partitions (index) and nodes (items) having an appropriate partition in OWNING state, calculated for
     * distributed tables, involved in query execution.
     */
    public List<List<NodeWithTerm>> assignments() {
        return assignments == null ? Collections.emptyList() : assignments;
    }

    /**
     * Prunes involved partitions (hence nodes, involved in query execution) on the basis of filter, its distribution,
     * query parameters and original nodes mapping.
     *
     * @param rel Filter.
     * @return Resulting nodes mapping.
     */
    public ColocationGroup prune(IgniteRel rel) {
        return this; // TODO https://issues.apache.org/jira/browse/IGNITE-12455
    }

    /**
     * Belongs.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public boolean belongs(long sourceId) {
        return sourceIds != null && sourceIds.contains(sourceId);
    }

    /**
     * Merges this mapping with given one.
     *
     * @param other Mapping to merge with.
     * @return Merged nodes mapping.
     * @throws ColocationMappingException If involved nodes intersection is empty, hence there is no nodes capable to
     *     execute being calculated fragment.
     */
    public ColocationGroup colocate(ColocationGroup other) throws ColocationMappingException {
        List<Long> sourceIds;
        if (this.sourceIds == null || other.sourceIds == null) {
            sourceIds = firstNotNull(this.sourceIds, other.sourceIds);
        } else {
            sourceIds = Commons.combine(this.sourceIds, other.sourceIds);
        }

        List<String> nodeNames;
        if (this.nodeNames == null || other.nodeNames == null) {
            nodeNames = firstNotNull(this.nodeNames, other.nodeNames);
        } else {
            nodeNames = Commons.intersect(other.nodeNames, this.nodeNames);
        }

        if (nodeNames != null && nodeNames.isEmpty()) {
            throw new ColocationMappingException("Failed to map fragment to location. "
                    + "Replicated query parts are not co-located on all nodes");
        }

        List<List<NodeWithTerm>> assignments;
        Set<String> nodeNamesSet = nodeNames == null ? null : new HashSet<>(nodeNames);
        Predicate<String> nodeNamesFilter = nodeNames == null ? v -> true : nodeNamesSet::contains;

        if (this.assignments == null || other.assignments == null) {
            assignments = firstNotNull(this.assignments, other.assignments);

            if (assignments != null && nodeNamesSet != null) {
                List<List<NodeWithTerm>> assignments0 = new ArrayList<>(assignments.size());

                for (int i = 0; i < assignments.size(); i++) {
                    List<NodeWithTerm> assignment = filterByNodeNames(assignments.get(i), nodeNamesFilter);

                    if (assignment.isEmpty()) {
                        throw new ColocationMappingException("Failed to map fragment to location. "
                                + "Partition mapping is empty [part=" + i + "]");
                    }

                    assignments0.add(assignment);
                }

                assignments = assignments0;
            }
        } else {
            assert this.assignments.size() == other.assignments.size();

            assignments = new ArrayList<>(this.assignments.size());

            for (int p = 0; p < this.assignments.size(); p++) {
                List<NodeWithTerm> assignment0 = this.assignments.get(p);
                List<NodeWithTerm> assignment1 = other.assignments.get(p);

                List<NodeWithTerm> assignment = intersect(assignment0, assignment1, nodeNamesFilter, p);

                if (assignment.isEmpty()) {
                    throw new ColocationMappingException("Failed to map fragment to location. Partition mapping is empty [part=" + p + "]");
                }

                assignments.add(assignment);
            }
        }

        return new ColocationGroup(sourceIds, nodeNames, assignments);
    }

    private List<NodeWithTerm> intersect(
            List<NodeWithTerm> assignment0, 
            List<NodeWithTerm> assignment1,
            Predicate<String> filter, 
            int p
    ) throws ColocationMappingException {
        if (assignment0.size() == 1 && assignment1.size() == 1) {
            NodeWithTerm first = assignment0.get(0);
            NodeWithTerm second = assignment1.get(0);

            if (filter.test(first.name()) && Objects.equals(first.name(), second.name())) {
                validateTerm(first, second, p);

                return assignment0;
            }

            return Collections.emptyList();
        }

        if (assignment0.size() > assignment1.size()) {
            List<NodeWithTerm> tmp = assignment0;
            assignment0 = assignment1;
            assignment1 = tmp;
        }

        List<NodeWithTerm> assignment = new ArrayList<>();

        // Filter and hash a smaller list.
        Map<String, NodeWithTerm> nameToAssignmentMapping = assignment0.stream()
                .filter(v -> filter.test(v.name()))
                .collect(Collectors.toMap(NodeWithTerm::name, nodeWithTerm -> nodeWithTerm));

        // Iterate over a larger list.
        for (NodeWithTerm first : assignment1) {
            NodeWithTerm second = nameToAssignmentMapping.get(first.name());

            if (second == null) {
                continue;
            }

            validateTerm(first, second, p);

            assignment.add(first);
        }

        return assignment;
    }

    private void validateTerm(NodeWithTerm first, NodeWithTerm second, int partId) throws ColocationMappingException {
        if (first.term() != second.term()) {
            throw new ColocationMappingException("Primary replica term has been changed during mapping ["
                    + "node=" + first.name()
                    + ", expectedTerm=" + first.term()
                    + ", actualTerm=" + second.term()
                    + ", part=" + partId
                    + ']');
        }
    }

    private List<NodeWithTerm> filterByNodeNames(List<NodeWithTerm> assignment, Predicate<String> filter) {
        List<NodeWithTerm> res = new ArrayList<>(assignment.size());

        for (NodeWithTerm nodeWithTerm : assignment) {
            if (!filter.test(nodeWithTerm.name())) {
                continue;
            }

            res.add(nodeWithTerm);
        }

        return res;
    }

    /**
     * Creates a new colocation group using only primary assignments.
     *
     * @return Colocation group with primary assignments.
     */
    public ColocationGroup complete() {
        if (assignments != null) {
            List<List<NodeWithTerm>> assignments = new ArrayList<>(this.assignments.size());
            Set<String> nodes = new HashSet<>();
            for (List<NodeWithTerm> assignment : this.assignments) {
                NodeWithTerm first = first(assignment);
                if (first != null) {
                    nodes.add(first.name());
                }
                assignments.add(first != null ? Collections.singletonList(first) : Collections.emptyList());
            }

            return new ColocationGroup(sourceIds, new ArrayList<>(nodes), assignments);
        }

        return mapToNodes(nodeNames);
    }

    /**
     * MapToNodes.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ColocationGroup mapToNodes(List<String> nodeNames) {
        return !nullOrEmpty(this.nodeNames) ? this : forNodes0(nodeNames);
    }

    private ColocationGroup forNodes0(List<String> nodeNames) {
        return new ColocationGroup(sourceIds, nodeNames, assignments);
    }

    /**
     * Returns list of pairs containing the partition number to scan on the given node with the corresponding primary replica term.
     *
     * @param nodeName Cluster node consistent ID.
     * @return List of pairs containing the partition number to scan on the given node with the corresponding primary replica term.
     */
    public List<PartitionWithTerm> partitionsWithTerms(String nodeName) {
        List<PartitionWithTerm> partsWithTerms = new ArrayList<>();

        for (int p = 0; p < assignments.size(); p++) {
            List<NodeWithTerm> assignment = assignments.get(p);

            NodeWithTerm nodeWithTerm = first(assignment);

            assert nodeWithTerm != null : "part=" + p;

            if (Objects.equals(nodeName, nodeWithTerm.name())) {
                partsWithTerms.add(new PartitionWithTerm(p, nodeWithTerm.term()));
            }
        }

        return partsWithTerms;
    }
}
