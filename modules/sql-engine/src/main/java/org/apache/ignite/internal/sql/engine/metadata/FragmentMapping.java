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
import static org.apache.ignite.internal.util.IgniteUtils.firstNotNull;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * FragmentMapping.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class FragmentMapping implements Serializable {
    private final List<ColocationGroup> colocationGroups;

    /**
     * Assignments of a table that will be updated by the fragment.
     *
     * <p>Currently only one table can be modified by query. Used to dispatch the request.
     */
    private final @Nullable List<NodeWithTerm> updatingTableAssignments;

    private FragmentMapping(ColocationGroup colocationGroup) {
        this(asList(colocationGroup));
    }

    private FragmentMapping(List<ColocationGroup> colocationGroups) {
        this(null, colocationGroups);
    }

    private FragmentMapping(@Nullable List<NodeWithTerm> updatingTableAssignments, List<ColocationGroup> colocationGroups) {
        this.updatingTableAssignments = updatingTableAssignments;
        this.colocationGroups = colocationGroups;
    }

    /**
     * Create.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static FragmentMapping create(String nodeName) {
        return new FragmentMapping(ColocationGroup.forNodes(Collections.singletonList(nodeName)));
    }

    /**
     * Create.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static FragmentMapping create(long sourceId) {
        return new FragmentMapping(ColocationGroup.forSourceId(sourceId));
    }

    /**
     * Create.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static FragmentMapping create(long sourceId, ColocationGroup group) {
        try {
            return new FragmentMapping(ColocationGroup.forSourceId(sourceId).colocate(group));
        } catch (ColocationMappingException e) {
            throw new AssertionError(e); // Cannot happen
        }
    }

    /**
     * Get colocated flag.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public boolean colocated() {
        return colocationGroups.isEmpty() || colocationGroups.size() == 1;
    }

    /**
     * Prune.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public FragmentMapping prune(IgniteRel rel) {
        if (colocationGroups.size() != 1) {
            return this;
        }

        return new FragmentMapping(updatingTableAssignments, List.of(first(colocationGroups).prune(rel)));
    }

    /**
     * Enriches the mapping with assignments of the table that will be modified by the fragment.
     *
     * <p>This assignments will be used by execution to dispatch the update request to the node managing
     * the primary replica of particular partition.
     *
     * @param updatingTableAssignments Assignments of the table that will be modified.
     * @return Enriched mapping.
     */
    public FragmentMapping updatingTableAssignments(List<NodeWithTerm> updatingTableAssignments) {
        // currently only one table can be modified by query
        assert this.updatingTableAssignments == null;

        return new FragmentMapping(updatingTableAssignments, colocationGroups);
    }

    public List<NodeWithTerm> updatingTableAssignments() {
        return updatingTableAssignments;
    }

    /**
     * Combine.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public FragmentMapping combine(FragmentMapping other) {
        assert updatingTableAssignments == null || other.updatingTableAssignments == null;

        List<NodeWithTerm> updatingTableAssignments = firstNotNull(this.updatingTableAssignments, other.updatingTableAssignments);

        return new FragmentMapping(updatingTableAssignments, Commons.combine(colocationGroups, other.colocationGroups));
    }

    /**
     * Colocate.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public FragmentMapping colocate(FragmentMapping other) throws ColocationMappingException {
        assert colocated() && other.colocated();
        assert updatingTableAssignments == null || other.updatingTableAssignments == null;

        ColocationGroup first = first(colocationGroups);
        ColocationGroup second = first(other.colocationGroups);
        List<NodeWithTerm> updatingTableAssignments = firstNotNull(this.updatingTableAssignments, other.updatingTableAssignments);

        if (first == null && second == null && updatingTableAssignments == null) {
            return this;
        } else if (first == null && second == null) {
            return new FragmentMapping(updatingTableAssignments, List.of());
        } else if (first == null || second == null) {
            return new FragmentMapping(updatingTableAssignments, List.of(firstNotNull(first, second)));
        } else {
            return new FragmentMapping(updatingTableAssignments, List.of(first.colocate(second)));
        }
    }

    /**
     * NodeIds.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public List<String> nodeNames() {
        return colocationGroups.stream()
                .flatMap(g -> g.nodeNames().stream())
                .distinct().collect(Collectors.toList());
    }

    /**
     * Finalize.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public FragmentMapping finalize(Supplier<List<String>> nodesSource) {
        if (colocationGroups.isEmpty()) {
            return this;
        }

        List<ColocationGroup> colocationGroups = this.colocationGroups;

        colocationGroups = Commons.transform(colocationGroups, ColocationGroup::complete);

        List<String> nodes = nodeNames();
        List<String> nodes0 = nodes.isEmpty() ? nodesSource.get() : nodes;

        colocationGroups = Commons.transform(colocationGroups, g -> g.mapToNodes(nodes0));

        return new FragmentMapping(updatingTableAssignments, colocationGroups);
    }

    /**
     * FindGroup.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public @NotNull ColocationGroup findGroup(long sourceId) {
        List<ColocationGroup> groups = colocationGroups.stream()
                .filter(c -> c.belongs(sourceId))
                .collect(Collectors.toList());

        if (groups.isEmpty()) {
            throw new IllegalStateException("Failed to find group with given id. [sourceId=" + sourceId + "]");
        } else if (groups.size() > 1) {
            throw new IllegalStateException("Multiple groups with the same id found. [sourceId=" + sourceId + "]");
        }

        return first(groups);
    }
}
