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

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.ErrorGroups.Sql;

/**
 * An implementation of {@link MappingService}.
 *
 * <p>This particular implementation keeps track of changes in logical cluster topology.
 * Always uses latest topology snapshot to map query.
 */
public class MappingServiceImpl implements MappingService, LogicalTopologyEventListener {
    private static final int MAPPING_ATTEMPTS = 3;

    private final AtomicReference<Set<String>> nodesSetRef = new AtomicReference<>(Set.of());

    private final String localNodeName;
    private final ExecutionTargetProvider targetProvider;

    public MappingServiceImpl(
            String localNodeName,
            ExecutionTargetProvider targetProvider
    ) {
        this.localNodeName = localNodeName;
        this.targetProvider = targetProvider;
    }

    @Override
    public CompletableFuture<List<MappedFragment>> map(MultiStepPlan multiStepPlan) {
        List<Fragment> fragments = multiStepPlan.fragments();

        List<String> nodes = List.copyOf(nodesSetRef.get());

        MappingContext context = new MappingContext(localNodeName, nodes);

        List<Fragment> fragments0 = Commons.transform(fragments, fragment -> fragment.attach(context.cluster()));

        List<CompletableFuture<IntObjectPair<ExecutionTarget>>> targets =
                fragments0.stream().flatMap(fragment -> Stream.concat(
                        fragment.tables().stream()
                                .map(table -> targetProvider.forTable(context.targetFactory(), table)
                                        .thenApply(target -> IntObjectPair.of(table.id(), target))
                                ),
                        fragment.systemViews().stream()
                                .map(view -> targetProvider.forSystemView(context.targetFactory(), view)
                                        .thenApply(target -> IntObjectPair.of(view.id(), target))
                                )
                ))
                .collect(Collectors.toList());

        return allOf(targets.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> {
                    Int2ObjectMap<ExecutionTarget> targetsById = new Int2ObjectOpenHashMap<>();

                    for (CompletableFuture<IntObjectPair<ExecutionTarget>> fut : targets) {
                        // this is a safe join, because we have waited for all futures to be complete
                        IntObjectPair<ExecutionTarget> pair = fut.join();

                        targetsById.put(pair.firstInt(), pair.second());
                    }

                    FragmentMapper mapper = new FragmentMapper(context.cluster().getMetadataQuery(), context, targetsById);

                    Long2ObjectMap<FragmentMapping> mappingByFragmentId = new Long2ObjectOpenHashMap<>();
                    Long2ObjectMap<ColocationGroup> groupsBySourceId = new Long2ObjectOpenHashMap<>();
                    Long2ObjectMap<List<String>> allSourcesByExchangeId = new Long2ObjectOpenHashMap<>();
                    Exception ex = null;
                    boolean lastAttemptSucceed = true;
                    List<Fragment> fragmentsToMap = fragments0;
                    for (int attempt = 0; attempt < MAPPING_ATTEMPTS; attempt++) {
                        Fragment currentFragment = null;
                        try {
                            for (Fragment fragment : fragmentsToMap) {
                                currentFragment = fragment;

                                if (mappingByFragmentId.containsKey(fragment.fragmentId())) {
                                    continue;
                                }

                                FragmentMapping mapping = mapper.map(fragment);

                                mappingByFragmentId.put(fragment.fragmentId(), mapping);
                                for (ColocationGroup group : mapping.groups()) {
                                    for (long sourceId : group.sourceIds()) {
                                        groupsBySourceId.put(sourceId, group);
                                    }
                                }

                                if (!fragment.rootFragment()) {
                                    IgniteSender sender = (IgniteSender) fragment.root();

                                    List<String> nodeNames = mapping.groups().stream()
                                            .flatMap(g -> g.nodeNames().stream())
                                            .distinct().collect(Collectors.toList());

                                    allSourcesByExchangeId.put(sender.exchangeId(), nodeNames);
                                }

                            }

                            lastAttemptSucceed = true;
                        } catch (FragmentMappingException mappingException) {
                            if (ex == null) {
                                ex = mappingException;
                            } else {
                                ex.addSuppressed(mappingException);
                            }

                            fragmentsToMap = replace(
                                    fragmentsToMap,
                                    currentFragment,
                                    new FragmentSplitter(mappingException.node()).go(currentFragment)
                            );

                            lastAttemptSucceed = false;
                        }
                    }

                    if (!lastAttemptSucceed) {
                        throw new IgniteInternalException(Sql.MAPPING_ERR, "Unable to map query: " + ex.getMessage(), ex);
                    }

                    List<MappedFragment> result = new ArrayList<>(fragmentsToMap.size());
                    for (Fragment fragment : fragmentsToMap) {
                        FragmentMapping mapping = mappingByFragmentId.get(fragment.fragmentId());

                        ColocationGroup targetGroup = null;
                        if (!fragment.rootFragment()) {
                            IgniteSender sender = (IgniteSender) fragment.root();

                            targetGroup = groupsBySourceId.get(sender.exchangeId());
                        }

                        Long2ObjectMap<List<String>> sourcesByExchangeId = null;
                        for (IgniteReceiver receiver : fragment.remotes()) {
                            if (sourcesByExchangeId == null) {
                                sourcesByExchangeId = new Long2ObjectOpenHashMap<>();
                            }

                            long exchangeId = receiver.exchangeId();

                            sourcesByExchangeId.put(exchangeId, allSourcesByExchangeId.get(exchangeId));
                        }

                        result.add(
                                new MappedFragment(
                                        fragment,
                                        mapping.groups(),
                                        sourcesByExchangeId,
                                        targetGroup
                                )
                        );
                    }

                    return result;
                });
    }

    @Override
    public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
        nodesSetRef.set(deriveNodeNames(newTopology));
    }

    @Override
    public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
        nodesSetRef.set(deriveNodeNames(newTopology));
    }

    @Override
    public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
        nodesSetRef.set(deriveNodeNames(newTopology));
    }

    private static Set<String> deriveNodeNames(LogicalTopologySnapshot topology) {
        return topology.nodes().stream()
                .map(LogicalNode::name)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static List<Fragment> replace(
            List<Fragment> originalFragments,
            Fragment fragmentToReplace,
            List<Fragment> replacement
    ) {
        assert !nullOrEmpty(replacement);

        Long2LongOpenHashMap newTargets = new Long2LongOpenHashMap();
        for (Fragment fragment0 : replacement) {
            for (IgniteReceiver remote : fragment0.remotes()) {
                newTargets.put(remote.exchangeId(), fragment0.fragmentId());
            }
        }

        List<Fragment> newFragments = new ArrayList<>(originalFragments.size() + replacement.size() - 1);
        for (Fragment fragment : originalFragments) {
            if (fragment == fragmentToReplace) {
                fragment = first(replacement);
            } else if (!fragment.rootFragment()) {
                IgniteSender sender = (IgniteSender) fragment.root();

                long newTargetId = newTargets.getOrDefault(sender.exchangeId(), Long.MIN_VALUE);

                if (newTargetId != Long.MIN_VALUE) {
                    sender = new IgniteSender(sender.getCluster(), sender.getTraitSet(),
                            sender.getInput(), sender.exchangeId(), newTargetId, sender.distribution());

                    fragment = new Fragment(fragment.fragmentId(), fragment.correlated(), sender,
                            fragment.remotes(), fragment.tables(), fragment.systemViews());
                }
            }

            newFragments.add(fragment);
        }

        newFragments.addAll(replacement.subList(1, replacement.size()));

        return newFragments;
    }
}
