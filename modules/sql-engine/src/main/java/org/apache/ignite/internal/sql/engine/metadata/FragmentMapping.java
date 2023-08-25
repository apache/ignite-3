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
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.FragmentSplitter;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * FragmentMapping.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class FragmentMapping implements Serializable {

    private static final int MAPPING_ATTEMPTS = 3;

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
    public ColocationGroup findGroup(long sourceId) {
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

    /**
     * Computes mapping for each fragment of the given list of not mapped fragments.
     */
    public static List<Fragment> mapFragments(MappingQueryContext ctx, List<Fragment> notMappedFragments,
            Map<Integer, ColocationGroup> colocationGroups) {

        List<Fragment> fragments = Commons.transform(notMappedFragments, fragment -> fragment.attach(ctx.cluster()));

        Exception ex = null;
        RelMetadataQuery mq = first(fragments).root().getCluster().getMetadataQuery();
        for (int i = 0; i < MAPPING_ATTEMPTS; i++) {
            try {
                return doMap(fragments, ctx, mq, colocationGroups);
            } catch (FragmentMappingException e) {
                if (ex == null) {
                    ex = e;
                } else {
                    ex.addSuppressed(e);
                }

                fragments = replace(fragments, e.fragment(), new FragmentSplitter(e.node()).go(e.fragment()));
            }
        }

        throw new IgniteInternalException(INTERNAL_ERR, "Failed to map query.", ex);
    }

    private static List<Fragment> doMap(List<Fragment> fragments, MappingQueryContext ctx, RelMetadataQuery mq,
            Map<Integer, ColocationGroup> colocationGroups) {

        List<Fragment> frgs = new ArrayList<>(fragments.size());

        RelOptCluster cluster = Commons.cluster();

        for (Fragment fragment : fragments) {
            if (fragment.mapping() != null) {
                continue;
            }

            Supplier<List<String>> getNodes = () -> ctx.mappingService().executionNodes(fragment.single(), null);
            FragmentMapping mapping1 = mapFragment(fragment, ctx, mq, getNodes, colocationGroups);
            Fragment mapped = fragment.withMapping(mapping1);

            frgs.add(mapped.attach(cluster));
        }

        return frgs;
    }

    private static FragmentMapping mapFragment(Fragment fragment, MappingQueryContext ctx,
            RelMetadataQuery mq, Supplier<List<String>> nodesSource, Map<Integer, ColocationGroup> colocationGroups) {

        try {
            FragmentMapping mapping = new IgniteFragmentMapping(mq, ctx, colocationGroups).computeMapping(fragment.root());

            if (fragment.rootFragment()) {
                mapping = create(ctx.locNodeName()).colocate(mapping);
            }

            if (fragment.single() && mapping.nodeNames().size() > 1) {
                // this is possible when the fragment contains scan of a replicated cache, which brings
                // several nodes (actually all containing nodes) to the colocation group, but this fragment
                // supposed to be executed on a single node, so let's choose one wisely
                mapping = create(mapping.nodeNames()
                        .get(ThreadLocalRandom.current().nextInt(mapping.nodeNames().size()))).colocate(mapping);
            }

            return mapping.finalize(nodesSource);
        } catch (NodeMappingException e) {
            throw new FragmentMappingException("Failed to calculate physical distribution", fragment, e.node(), e);
        } catch (ColocationMappingException e) {
            throw new FragmentMappingException("Failed to calculate physical distribution", fragment, fragment.root(), e);
        }
    }

    private static List<Fragment> replace(List<Fragment> fragments, Fragment fragment, List<Fragment> replacement) {
        assert !nullOrEmpty(replacement);

        Long2LongOpenHashMap newTargets = new Long2LongOpenHashMap();
        for (Fragment fragment0 : replacement) {
            for (IgniteReceiver remote : fragment0.remotes()) {
                newTargets.put(remote.exchangeId(), fragment0.fragmentId());
            }
        }

        List<Fragment> fragments0 = new ArrayList<>(fragments.size() + replacement.size() - 1);
        for (Fragment fragment0 : fragments) {
            if (fragment0 == fragment) {
                fragment0 = first(replacement);
            } else if (!fragment0.rootFragment()) {
                IgniteSender sender = (IgniteSender) fragment0.root();

                long newTargetId = newTargets.getOrDefault(sender.exchangeId(), Long.MIN_VALUE);

                if (newTargetId != Long.MIN_VALUE) {
                    sender = new IgniteSender(sender.getCluster(), sender.getTraitSet(),
                            sender.getInput(), sender.exchangeId(), newTargetId, sender.distribution());

                    fragment0 = new Fragment(fragment0.fragmentId(), fragment0.correlated(), sender, fragment0.remotes());
                }
            }

            fragments0.add(fragment0);
        }

        fragments0.addAll(replacement.subList(1, replacement.size()));

        return fragments0;
    }
}
