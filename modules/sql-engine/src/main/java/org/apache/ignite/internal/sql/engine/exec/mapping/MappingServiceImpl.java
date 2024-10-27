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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.ExecutionDistributionProvider;
import org.apache.ignite.internal.sql.engine.ExecutionDistributionProviderImpl.DistributionHolder;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PlanId;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruner;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;

/**
 * An implementation of {@link MappingService}.
 *
 * <p>This particular implementation keeps track of changes in logical cluster topology.
 * Always uses latest topology snapshot to map query.
 */
public class MappingServiceImpl implements MappingService {
    private final String localNodeName;
    private final ClockService clock;
    private final Cache<PlanId, FragmentsTemplate> templatesCache;
    private final Cache<MappingsCacheKey, MappingsCacheValue> mappingsCache;
    private final PartitionPruner partitionPruner;
    private final Supplier<LogicalTopologySnapshot> logicalTopologySupplier;
    private final SystemViewManager systemViewManager;
    private final ExecutionDistributionProvider distributionProvider;

    /**
     * Constructor.
     *
     * @param localNodeName Name of the current Ignite node.
     * @param clock Clock service to get actual time.
     * @param cacheFactory A factory to create cache of fragments.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param partitionPruner Partition pruner.
     * @param logicalTopologySupplier Logical topology supplier.
     * @param systemViewManager System view manager.
     * @param distributionProvider Execution distribution provider.
     */
    public MappingServiceImpl(
            String localNodeName,
            ClockService clock,
            CacheFactory cacheFactory,
            int cacheSize,
            PartitionPruner partitionPruner,
            Supplier<LogicalTopologySnapshot> logicalTopologySupplier,
            SystemViewManager systemViewManager,
            ExecutionDistributionProvider distributionProvider
    ) {
        this.localNodeName = localNodeName;
        this.clock = clock;
        this.templatesCache = cacheFactory.create(cacheSize);
        this.mappingsCache = cacheFactory.create(cacheSize);
        this.partitionPruner = partitionPruner;
        this.logicalTopologySupplier = logicalTopologySupplier;
        this.systemViewManager = systemViewManager;
        this.distributionProvider = distributionProvider;
    }

    /** Called when the primary replica has expired. */
    public CompletableFuture<Boolean> onPrimaryReplicaExpired(PrimaryReplicaEventParameters parameters) {
        assert parameters != null;
        assert parameters.groupId() instanceof TablePartitionId;

        int tabId = ((TablePartitionId) parameters.groupId()).tableId();

        // TODO https://issues.apache.org/jira/browse/IGNITE-21201 Move complex computations to a different thread.
        mappingsCache.removeIfValue(value -> value.tableIds.contains(tabId));

        return CompletableFutures.falseCompletedFuture();
    }

    @Override
    public CompletableFuture<List<MappedFragment>> map(MultiStepPlan multiStepPlan, MappingParameters parameters) {
        FragmentsTemplate template = getOrCreateTemplate(multiStepPlan, MappingContext.CLUSTER);

        boolean mapOnBackups = parameters.mapOnBackups();

        MappingsCacheValue cacheValue = mappingsCache.compute(
                new MappingsCacheKey(multiStepPlan.id(), mapOnBackups),
                (key, val) -> {
                    if (val == null) {
                        IntSet tableIds = new IntOpenHashSet();
                        boolean topologyAware = false;

                        for (Fragment fragment : template.fragments) {
                            topologyAware = topologyAware || !fragment.systemViews().isEmpty();
                            for (IgniteDataSource source : fragment.tables().values()) {
                                tableIds.add(source.id());
                            }
                        }

                        long topVer = topologyAware ? logicalTopologySupplier.get().version() : Long.MAX_VALUE;

                        return new MappingsCacheValue(topVer, tableIds, mapFragments(template, mapOnBackups));
                    }

                    long topologyVer = logicalTopologySupplier.get().version();

                    if (val.topologyVersion < topologyVer) {
                        return new MappingsCacheValue(topologyVer, val.tableIds, mapFragments(template, mapOnBackups));
                    }

                    return val;
                });

        return cacheValue.mappedFragments.thenApply(frags -> applyPartitionPruning(frags.fragments, parameters));
    }

    private CompletableFuture<MappedFragments> mapFragments(
            FragmentsTemplate template,
            boolean mapOnBackups
    ) {
        List<String> viewNodes = template.fragments.stream().flatMap(fragment -> fragment.systemViews().stream()
                        .map(view -> systemViewManager.owningNodes(view.name())).flatMap(List::stream)).distinct()
                .collect(Collectors.toList());

        Set<IgniteTable> tables = template.fragments.stream().flatMap(fragment -> fragment.tables().values().stream())
                .collect(Collectors.toSet());

        CompletableFuture<DistributionHolder> distrFut = distributionProvider.distribution(clock.now(), mapOnBackups, tables,
                localNodeName);

        return distrFut.thenApply(distr -> {
            Int2ObjectMap<ExecutionTarget> targetsById = new Int2ObjectOpenHashMap<>();

            List<String> nodes = Stream.concat(distr.nodes().stream(), viewNodes.stream()).distinct().collect(
                    Collectors.toList());

            List<IntObjectPair<ExecutionTarget>> allTargets = prepareTargets(template, distr, nodes, viewNodes);

            for (IntObjectPair<ExecutionTarget> pair : allTargets) {
                targetsById.put(pair.firstInt(), pair.second());
            }

            MappingContext context = new MappingContext(localNodeName, nodes);

            FragmentMapper mapper = new FragmentMapper(template.cluster.getMetadataQuery(), context, targetsById);

            IdGenerator idGenerator = new IdGenerator(template.nextId);

            List<Fragment> fragments = new ArrayList<>(template.fragments);

            List<FragmentMapping> mappings = mapper.map(fragments, idGenerator);

            Long2ObjectMap<ColocationGroup> groupsBySourceId = new Long2ObjectOpenHashMap<>();
            Long2ObjectMap<List<String>> allSourcesByExchangeId = new Long2ObjectOpenHashMap<>();

            for (FragmentMapping mapping : mappings) {
                Fragment fragment = mapping.fragment();

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

            List<MappedFragment> mappedFragmentsList = new ArrayList<>(mappings.size());
            Set<String> targetNodes = new HashSet<>();
            for (FragmentMapping mapping : mappings) {
                Fragment fragment = mapping.fragment();

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

                MappedFragment mappedFragment = new MappedFragment(
                        fragment,
                        mapping.groups(),
                        sourcesByExchangeId,
                        targetGroup,
                        null
                );

                mappedFragmentsList.add(mappedFragment);

                targetNodes.addAll(mappedFragment.nodes());
            }

            return new MappedFragments(mappedFragmentsList, targetNodes);
        });
    }

    private List<IntObjectPair<ExecutionTarget>> prepareTargets(
            FragmentsTemplate template,
            DistributionHolder distr,
            List<String> nodes,
            List<String> viewNodes
    ) {
        MappingContext context = new MappingContext(localNodeName, nodes);

        ExecutionTargetFactory targetFactory = context.targetFactory();

        Stream<IntObjectPair<ExecutionTarget>> tableTargets = template.fragments.stream().flatMap(fragment ->
                fragment.tables().values().stream()
                        .map(table -> IntObjectPair.of(table.id(), forTable(targetFactory, distr.assignmentsPerTable(table)))));

        Stream<IntObjectPair<ExecutionTarget>> viewTargets = template.fragments.stream().flatMap(fragment -> fragment.systemViews().stream()
                .map(view -> IntObjectPair.of(view.id(), forSystemView(targetFactory, view, viewNodes))));

        return Stream.concat(tableTargets, viewTargets).collect(Collectors.toList());
    }

    ExecutionTarget forSystemView(ExecutionTargetFactory factory, IgniteSystemView view, List<String> nodes) {
        if (nullOrEmpty(nodes)) {
            throw new SqlException(Sql.MAPPING_ERR, format("The view with name '{}' could not be found on"
                    + " any active nodes in the cluster", view.name()));
        }

        return view.distribution() == IgniteDistributions.single()
                ? factory.oneOf(nodes)
                : factory.allOf(nodes);
    }

    ExecutionTarget forTable(ExecutionTargetFactory factory, List<TokenizedAssignments> assignments) {
        return factory.partitioned(assignments);
    }

    private List<MappedFragment> applyPartitionPruning(List<MappedFragment> mappedFragments, MappingParameters parameters) {
        return partitionPruner.apply(mappedFragments, parameters.dynamicParameters());
    }

    private FragmentsTemplate getOrCreateTemplate(MultiStepPlan plan, RelOptCluster cluster) {
        // QuerySplitter is deterministic, thus we can cache result in order to reuse it next time
        return templatesCache.get(plan.id(), key -> {
            IdGenerator idGenerator = new IdGenerator(0);

            List<Fragment> fragments = new QuerySplitter(idGenerator, cluster).split(plan.root());

            return new FragmentsTemplate(
                    idGenerator.nextId(), cluster, fragments
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

    /** Wraps list of mapped fragments with target node names. */
    private static class MappedFragments {
        final List<MappedFragment> fragments;
        final Set<String> nodes;

        MappedFragments(List<MappedFragment> fragments, Set<String> nodes) {
            this.fragments = fragments;
            this.nodes = nodes;
        }
    }

    private static class MappingsCacheValue {
        private final long topologyVersion;
        private final IntSet tableIds;
        private final CompletableFuture<MappedFragments> mappedFragments;

        MappingsCacheValue(long topologyVersion, IntSet tableIds, CompletableFuture<MappedFragments> mappedFragments) {
            this.topologyVersion = topologyVersion;
            this.tableIds = tableIds;
            this.mappedFragments = mappedFragments;
        }
    }

    private static class MappingsCacheKey {
        private final PlanId planId;
        private final boolean mapOnBackups;

        MappingsCacheKey(PlanId planId, boolean mapOnBackups) {
            this.planId = planId;
            this.mapOnBackups = mapOnBackups;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MappingsCacheKey that = (MappingsCacheKey) o;
            return mapOnBackups == that.mapOnBackups && Objects.equals(planId, that.planId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(planId, mapOnBackups);
        }
    }
}
