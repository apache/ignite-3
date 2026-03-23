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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * A result of a mapping.
 *
 * <p>Encloses original fragment and information describing topology of a given fragment. That is,
 * the list of nodes this fragment should be executed on, target group this fragment will send rows to,
 * list of rows by exchange id to provide every receiver in this fragment with list of nodes the receiver
 * expects rows from, and colocation group for every source in this fragment.
 */
public class MappedFragment {
    private final Fragment fragment;
    private final List<ColocationGroup> groups;
    private final List<String> nodes;
    private final Long2ObjectMap<ColocationGroup> groupsBySourceId;
    private final @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId;
    private final @Nullable ColocationGroup target;
    private final PartitionPruningMetadata partitionPruningMetadata;

    /** Constructor. */
    MappedFragment(
            Fragment fragment,
            List<ColocationGroup> groups,
            @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId,
            @Nullable ColocationGroup target,
            @Nullable PartitionPruningMetadata pruningMetadata
    ) {
        this.fragment = fragment;
        this.groups = groups;

        Set<String> nodes = new HashSet<>();

        Long2ObjectMap<ColocationGroup> groupsBySourceId = new Long2ObjectOpenHashMap<>();
        for (ColocationGroup group : groups) {
            nodes.addAll(group.nodeNames());

            for (long sourceId : group.sourceIds()) {
                groupsBySourceId.put(sourceId, group);
            }
        }

        this.nodes = List.copyOf(nodes);
        this.groupsBySourceId = groupsBySourceId;
        this.sourcesByExchangeId = sourcesByExchangeId;
        this.target = target;
        this.partitionPruningMetadata = pruningMetadata;
    }

    /** Constructor. */
    private MappedFragment(
            Fragment fragment,
            List<ColocationGroup> groups, List<String> nodes,
            Long2ObjectMap<ColocationGroup> groupsBySourceId,
            @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId,
            @Nullable ColocationGroup target,
            @Nullable PartitionPruningMetadata pruningMetadata
    ) {
        this.fragment = fragment;
        this.nodes = List.copyOf(nodes);
        this.groupsBySourceId = groupsBySourceId;
        this.sourcesByExchangeId = sourcesByExchangeId;
        this.target = target;
        this.groups = groups;
        this.partitionPruningMetadata = pruningMetadata;
    }

    public Fragment fragment() {
        return fragment;
    }

    public List<ColocationGroup> groups() {
        return groups;
    }

    public List<String> nodes() {
        return nodes;
    }

    public Long2ObjectMap<ColocationGroup> groupsBySourceId() {
        return groupsBySourceId;
    }

    public @Nullable ColocationGroup target() {
        return target;
    }

    public @Nullable Long2ObjectMap<List<String>> sourcesByExchangeId() {
        return sourcesByExchangeId;
    }

    public @Nullable PartitionPruningMetadata partitionPruningMetadata() {
        return partitionPruningMetadata;
    }

    /**
     * Creates a fragment by replacing the given colocation groups.
     *
     * @param replacedGroups Groups to replace.
     *
     * @return New mapped fragment.
     */
    public MappedFragment replaceColocationGroups(Long2ObjectMap<ColocationGroup> replacedGroups) {
        List<ColocationGroup> newGroups = new ArrayList<>(groupsBySourceId.size());

        // Because a colocation group may contain multiple sources, partition pruning (PP) splits a colocation group in multiple groups.
        // Each source id affected by PP goes into a separate group.
        //
        // Consider the following scenario:
        // ColocationGroup [ sourceIds = [0, 1], ... ] where sourceIds point to the same table but the one with sourceId=0
        // has a predicate and the one with source=1 does not. In this case we need to create two colocation groups:
        // one for sourceId=0 and another for sourceId=1.
        //
        // We should get these colocation groups in the end:
        //
        // - ColocationGroup [ sourceId = [0] ... ] this one has the number of partitions reduced.
        // - ColocationGroup [ sourceId = [1] ... ] this one has all partitions.
        //
        for (Entry<ColocationGroup> e : groupsBySourceId.long2ObjectEntrySet()) {
            ColocationGroup newGroup = replacedGroups.get(e.getLongKey());
            if (newGroup != null) {
                newGroups.add(newGroup);
            } else {
                ColocationGroup existing = e.getValue();
                existing = existing.removeSources(replacedGroups.keySet());

                assert !existing.sourceIds().isEmpty() : "ColocationGroup has no sources. Source id: "
                        + e.getLongKey() + " node names: " + existing.nodeNames() + " assignments: " + existing.assignments()
                        + " replaced groups: " + replacedGroups.keySet();

                newGroups.add(existing);
            }
        }

        return new MappedFragment(fragment, newGroups, sourcesByExchangeId, target, partitionPruningMetadata);
    }

    /**
     * Replaces nodes for the given exchangeId.
     *
     * @param exchangeId Exchange id.
     * @param newNodes New nodes.
     *
     * @return New mapped fragment.
     */
    public MappedFragment replaceExchangeSources(long exchangeId, List<String> newNodes) {
        assert sourcesByExchangeId != null : "No sourcesByExchangeId";
        assert !newNodes.isEmpty() : "New nodes are empty for exchange#" + exchangeId;

        Long2ObjectMap<List<String>> newSourcesByExchangeId = new Long2ObjectArrayMap<>(sourcesByExchangeId);
        newSourcesByExchangeId.put(exchangeId, newNodes);

        // The nodes should remain the same in order to preserve connectivity between fragments.
        return new MappedFragment(fragment,  groups, nodes, groupsBySourceId, newSourcesByExchangeId, target, partitionPruningMetadata);
    }

    /** Adds partition pruning metadata to this fragment. */
    public MappedFragment withPartitionPruningMetadata(PartitionPruningMetadata pruningMetadata) {
        return new MappedFragment(fragment, groups, sourcesByExchangeId, target, pruningMetadata);
    }
}
