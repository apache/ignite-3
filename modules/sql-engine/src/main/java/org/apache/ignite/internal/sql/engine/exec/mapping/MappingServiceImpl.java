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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PlanId;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.lang.ErrorGroups.Sql;

/**
 * An implementation of {@link MappingService}.
 *
 * <p>This particular implementation keeps track of changes in logical cluster topology.
 * Always uses latest topology snapshot to map query.
 */
public class MappingServiceImpl implements MappingService, LogicalTopologyEventListener {
    private static final int MAPPING_ATTEMPTS = 3;

    private final LogicalTopologyHolder topologyHolder = new LogicalTopologyHolder();
    private final CompletableFuture<Void> initialTopologyFuture = new CompletableFuture<>();

    private final String localNodeName;
    private final ExecutionTargetProvider targetProvider;
    private final Cache<PlanId, FragmentsTemplate> templatesCache;
    private final Executor taskExecutor;


    /**
     * Constructor.
     *
     * @param localNodeName Name of the current Ignite node.
     * @param targetProvider Execution target provider.
     * @param cacheFactory A factory to create cache of fragments.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param taskExecutor Mapper service task executor.
     */
    public MappingServiceImpl(
            String localNodeName,
            ExecutionTargetProvider targetProvider,
            CacheFactory cacheFactory,
            int cacheSize,
            Executor taskExecutor
    ) {
        this.localNodeName = localNodeName;
        this.targetProvider = targetProvider;
        this.templatesCache = cacheFactory.create(cacheSize);
        this.taskExecutor = taskExecutor;
    }

    @Override
    public CompletableFuture<List<MappedFragment>> map(MultiStepPlan multiStepPlan) {
        if (initialTopologyFuture.isDone()) {
            return map0(multiStepPlan);
        }

        return initialTopologyFuture.thenComposeAsync(ignore -> map0(multiStepPlan), taskExecutor);
    }

    private CompletableFuture<List<MappedFragment>> map0(MultiStepPlan multiStepPlan) {
        List<String> nodes = topologyHolder.nodes();
        MappingContext context = new MappingContext(localNodeName, nodes);

        FragmentsTemplate template = getOrCreateTemplate(multiStepPlan, context);

        IdGenerator idGenerator = new IdGenerator(template.nextId);
        List<Fragment> fragments = new ArrayList<>(template.fragments);

        List<CompletableFuture<IntObjectPair<ExecutionTarget>>> targets =
                fragments.stream().flatMap(fragment -> Stream.concat(
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

                    FragmentMapper mapper = new FragmentMapper(template.cluster.getMetadataQuery(), context, targetsById);

                    Long2ObjectMap<FragmentMapping> mappingByFragmentId = new Long2ObjectOpenHashMap<>();
                    Long2ObjectMap<ColocationGroup> groupsBySourceId = new Long2ObjectOpenHashMap<>();
                    Long2ObjectMap<List<String>> allSourcesByExchangeId = new Long2ObjectOpenHashMap<>();
                    Exception ex = null;
                    boolean lastAttemptSucceed = true;
                    List<Fragment> fragmentsToMap = fragments;
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
                                    new FragmentSplitter(idGenerator, mappingException.node()).go(currentFragment)
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
        topologyHolder.update(newTopology);
    }

    @Override
    public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
        topologyHolder.update(newTopology);
    }

    @Override
    public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
        topologyHolder.update(newTopology);
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

    /**
     * Holder for topology snapshots that guarantees monotonically increasing versions.
     */
    class LogicalTopologyHolder {
        private volatile List<String> nodes = List.of();
        private long ver = Long.MIN_VALUE;

        void update(LogicalTopologySnapshot topologySnapshot) {
            synchronized (this) {
                if (ver < topologySnapshot.version()) {
                    nodes = deriveNodeNames(topologySnapshot);
                    ver = topologySnapshot.version();
                }

                if (initialTopologyFuture.isDone() || !nodes.contains(localNodeName)) {
                    return;
                }
            }

            initialTopologyFuture.complete(null);
        }

        List<String> nodes() {
            return nodes;
        }

        private List<String> deriveNodeNames(LogicalTopologySnapshot topology) {
            return topology.nodes().stream()
                    .map(LogicalNode::name)
                    .collect(Collectors.toUnmodifiableList());
        }
    }

    private FragmentsTemplate getOrCreateTemplate(MultiStepPlan plan, MappingContext context) {
        // QuerySplitter is deterministic, thus we can cache result in order to reuse it next time
        return templatesCache.get(plan.id(), key -> {
            IdGenerator idGenerator = new IdGenerator(0);

            List<Fragment> fragments = new QuerySplitter(idGenerator, context.cluster()).split(plan.root());

            return new FragmentsTemplate(
                    idGenerator.nextId(), context.cluster(), fragments
            );
        });
    }

    private static class FragmentsTemplate {
        private final long nextId;
        private final RelOptCluster cluster;
        private final List<Fragment> fragments;

        FragmentsTemplate(long nextId, RelOptCluster cluster, List<Fragment> fragments) {
            this.nextId = nextId;
            this.cluster = cluster;
            this.fragments = fragments;
        }
    }
}
