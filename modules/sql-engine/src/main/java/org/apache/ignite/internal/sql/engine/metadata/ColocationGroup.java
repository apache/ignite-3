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

import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.firstNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.IgniteIntList;
import org.jetbrains.annotations.NotNull;

/**
 * ColocationGroup.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ColocationGroup implements Serializable {
    private static final int SYNTHETIC_PARTITIONS_COUNT = 512;
    // TODO: IgniteSystemProperties.getInteger("IGNITE_CALCITE_SYNTHETIC_PARTITIONS_COUNT", 512);

    private final List<Long> sourceIds;

    private final List<String> nodeNames;

    private final List<List<String>> assignments;

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
    public static ColocationGroup forAssignments(List<List<String>> assignments) {
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
    private ColocationGroup(List<Long> sourceIds, List<String> nodeNames, List<List<String>> assignments) {
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
    public List<List<String>> assignments() {
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

        List<List<String>> assignments;
        if (this.assignments == null || other.assignments == null) {
            assignments = firstNotNull(this.assignments, other.assignments);

            if (assignments != null && nodeNames != null) {
                Set<String> filter = new HashSet<>(nodeNames);
                List<List<String>> assignments0 = new ArrayList<>(assignments.size());

                for (int i = 0; i < assignments.size(); i++) {
                    List<String> assignment = Commons.intersect(filter, assignments.get(i));

                    if (assignment.isEmpty()) { // TODO check with partition filters
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
            Set<String> filter = nodeNames == null ? null : new HashSet<>(nodeNames);
            for (int i = 0; i < this.assignments.size(); i++) {
                List<String> assignment = Commons.intersect(this.assignments.get(i), other.assignments.get(i));

                if (filter != null) {
                    assignment.retainAll(filter);
                }

                if (assignment.isEmpty()) { // TODO check with partition filters
                    throw new ColocationMappingException("Failed to map fragment to location. Partition mapping is empty [part=" + i + "]");
                }

                assignments.add(assignment);
            }
        }

        return new ColocationGroup(sourceIds, nodeNames, assignments);
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ColocationGroup finalaze() {
        if (assignments == null && nodeNames == null) {
            return this;
        }

        if (assignments != null) {
            List<List<String>> assignments = new ArrayList<>(this.assignments.size());
            Set<String> nodes = new HashSet<>();
            for (List<String> assignment : this.assignments) {
                String first = first(assignment);
                if (first != null) {
                    nodes.add(first);
                }
                assignments.add(first != null ? Collections.singletonList(first) : Collections.emptyList());
            }

            return new ColocationGroup(sourceIds, new ArrayList<>(nodes), assignments);
        }

        return forNodes0(nodeNames);
    }

    /**
     * MapToNodes.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ColocationGroup mapToNodes(List<String> nodeNames) {
        return !nullOrEmpty(this.nodeNames) ? this : forNodes0(nodeNames);
    }

    @NotNull
    private ColocationGroup forNodes0(List<String> nodeNames) {
        List<List<String>> assignments = new ArrayList<>(SYNTHETIC_PARTITIONS_COUNT);
        for (int i = 0; i < SYNTHETIC_PARTITIONS_COUNT; i++) {
            assignments.add(asList(nodeNames.get(i % nodeNames.size())));
        }
        return new ColocationGroup(sourceIds, nodeNames, assignments);
    }

    /**
     * Returns List of partitions to scan on the given node.
     *
     * @param nodeNames Cluster node consistent ID.
     * @return List of partitions to scan on the given node.
     */
    public int[] partitions(String nodeNames) {
        IgniteIntList parts = new IgniteIntList(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            List<String> assignment = assignments.get(i);
            if (Objects.equals(nodeNames, first(assignment))) {
                parts.add(i);
            }
        }

        return parts.array();
    }
}
