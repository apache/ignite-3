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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CollectionUtils.toIntMapCollector;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
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
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl.LogicalTopologyHolder.TopologySnapshot;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PlanId;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruner;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of {@link MappingService}.
 *
 * <p>This particular implementation keeps track of changes according to {@link PlacementDriver} assignments.
 * Use distribution information to map a query.
 */
public class MappingServiceImpl implements MappingService, LogicalTopologyEventListener {
    private final LogicalTopologyHolder topologyHolder = new LogicalTopologyHolder();
    private final CompletableFuture<Void> initialTopologyFuture = new CompletableFuture<>();

    private final String localNodeName;
    private final ClockService clock;
    private final Cache<PlanId, FragmentsTemplate> templatesCache;
    // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
    // private final Cache<MappingsCacheKey, MappingsCacheValue> mappingsCache;
    private final PartitionPruner partitionPruner;
    private final ExecutionDistributionProvider distributionProvider;
    private final Executor taskExecutor;

    /**
     * Constructor.
     *
     * @param localNodeName Name of the current Ignite node.
     * @param clock Clock service to get actual time.
     * @param cacheFactory A factory to create cache of fragments.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param partitionPruner Partition pruner.
     * @param distributionProvider Execution distribution provider.
     * @param taskExecutor Mapper service task executor.
     */
    public MappingServiceImpl(
            String localNodeName,
            ClockService clock,
            CacheFactory cacheFactory,
            int cacheSize,
            PartitionPruner partitionPruner,
            ExecutionDistributionProvider distributionProvider,
            Executor taskExecutor
    ) {
        this.localNodeName = localNodeName;
        this.clock = clock;
        this.templatesCache = cacheFactory.create(cacheSize);
        // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
        // this.mappingsCache = cacheFactory.create(cacheSize);
        this.partitionPruner = partitionPruner;
        this.distributionProvider = distributionProvider;
        this.taskExecutor = taskExecutor;
    }

    /** Called when the primary replica has expired. */
    public CompletableFuture<Boolean> onPrimaryReplicaExpired(PrimaryReplicaEventParameters parameters) {
        assert parameters != null;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
        // int zoneId = ((ZonePartitionId) parameters.groupId()).zoneId();
        //
        // TODO https://issues.apache.org/jira/browse/IGNITE-21201 Move complex computations to a different thread.
        // mappingsCache.removeIfValue(value -> value.zoneIds.contains(zoneId));

        return CompletableFutures.falseCompletedFuture();
    }

    @Override
    public CompletableFuture<MappedFragments> map(MultiStepPlan multiStepPlan, MappingParameters parameters) {
        if (initialTopologyFuture.isDone()) {
            return map0(multiStepPlan, parameters);
        }

        return initialTopologyFuture.thenComposeAsync(ignore -> map0(multiStepPlan, parameters), taskExecutor);
    }

    private CompletableFuture<MappedFragments> map0(MultiStepPlan multiStepPlan, MappingParameters parameters) {
        FragmentsTemplate template = getOrCreateTemplate(multiStepPlan);

        boolean mapOnBackups = parameters.mapOnBackups();
        // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache  
        // Predicate<String> nodeExclusionFilter = parameters.nodeExclusionFilter();
        PartitionPruningMetadata partitionPruningMetadata = multiStepPlan.partitionPruningMetadata();
        TopologySnapshot topologySnapshot = topologyHolder.topology();

        CompletableFuture<MappedFragmentsWithNodes> mappedFragments;
        // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
        // if (nodeExclusionFilter != null) {
        //     mappedFragments = mapFragments(
        //             template, mapOnBackups, composeNodeExclusionFilter(topologySnapshot, parameters)
        //     );
        // } else {
        //     mappedFragments = mappingsCache.compute(
        //             new MappingsCacheKey(multiStepPlan.id(), mapOnBackups),
        //             (key, val) -> computeMappingCacheKey(val, topologySnapshot, parameters, template, mapOnBackups)
        //     ).mappedFragments;
        // }
        mappedFragments = mapFragments(
                template, mapOnBackups, composeNodeExclusionFilter(topologySnapshot, parameters)
        );

        return mappedFragments.thenApply(frags -> applyPartitionPruning(frags.fragments, parameters, partitionPruningMetadata))
                .thenApply(frags -> new MappedFragments(frags, topologySnapshot.version));
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod") // TODO: IGNITE-26465 Part of planned caching approach (see commented-out code above)
    private MappingsCacheValue computeMappingCacheKey(
            MappingsCacheValue val,
            TopologySnapshot topologySnapshot,
            MappingParameters parameters,
            FragmentsTemplate template,
            boolean mapOnBackups
    ) {
        if (val == null) {
            IntSet zoneIds = new IntOpenHashSet();
            boolean topologyAware = false;

            for (Fragment fragment : template.fragments) {
                topologyAware = topologyAware || !fragment.systemViews().isEmpty();
                for (IgniteTable source : fragment.tables().values()) {
                    zoneIds.add(source.zoneId());
                }
            }

            long topVer = topologyAware ? topologySnapshot.version() : Long.MAX_VALUE;

            return new MappingsCacheValue(topVer, zoneIds,
                    mapFragments(template, mapOnBackups, composeNodeExclusionFilter(topologySnapshot, parameters)));
        }

        long topologyVer = topologySnapshot.version();

        if (val.topologyVersion < topologyVer) {
            return new MappingsCacheValue(topologyVer, val.zoneIds,
                    mapFragments(template, mapOnBackups, composeNodeExclusionFilter(topologySnapshot, parameters)));
        }

        return val;
    }

    CompletableFuture<DistributionHolder> composeDistributions(
            Set<IgniteSystemView> views,
            Set<IgniteTable> tables,
            boolean mapOnBackups
    ) {
        if (tables.isEmpty() && views.isEmpty()) {
            DistributionHolder holder = new DistributionHolder(Set.of(localNodeName), Int2ObjectMaps.emptyMap(), Int2ObjectMaps.emptyMap());

            return completedFuture(holder);
        } else {
            Int2ObjectMap<CompletableFuture<List<TokenizedAssignments>>> tablesAssignments = new Int2ObjectOpenHashMap<>(tables.size());
            Set<String> allNodes = new HashSet<>();

            allNodes.add(localNodeName);

            for (IgniteTable tbl : tables) {
                CompletableFuture<List<TokenizedAssignments>> assignments = distributionProvider
                        .forTable(clock.current(), tbl, mapOnBackups);

                tablesAssignments.put(tbl.id(), assignments);
            }

            return CompletableFuture.allOf(tablesAssignments.values().toArray(new CompletableFuture[0]))
                    .thenApply(ignore -> {
                        Int2ObjectMap<List<TokenizedAssignments>> assignmentsPerTable = new Int2ObjectOpenHashMap<>(tables.size());

                        tablesAssignments.keySet().forEach(k -> {
                            // this is a safe join, because we have waited for all futures to be completed
                            List<TokenizedAssignments> assignments = tablesAssignments.get(k).join();

                            assignments.stream().flatMap(i -> i.nodes().stream()).map(Assignment::consistentId).forEach(allNodes::add);

                            assignmentsPerTable.put(k, assignments);
                        });

                        return assignmentsPerTable;
                    })
                    .thenApply(assignmentsPerTable -> {
                        Int2ObjectMap<List<String>> nodesPerView = views.stream()
                                .collect(toIntMapCollector(IgniteDataSource::id, distributionProvider::forSystemView));

                        nodesPerView.values().stream().flatMap(List::stream).forEach(allNodes::add);

                        return new DistributionHolder(allNodes, assignmentsPerTable, nodesPerView);
                    });
        }
    }

    private CompletableFuture<MappedFragmentsWithNodes> mapFragments(
            FragmentsTemplate template,
            boolean mapOnBackups,
            Predicate<String> nodeExclusionFilter
    ) {
        Set<IgniteSystemView> views = template.fragments.stream().flatMap(fragment -> fragment.systemViews().stream())
                .collect(Collectors.toSet());

        Set<IgniteTable> tables = template.fragments.stream().flatMap(fragment -> fragment.tables().values().stream())
                .collect(Collectors.toSet());

        CompletableFuture<DistributionHolder> res = composeDistributions(views, tables, mapOnBackups);

        return res.thenApply(assignments -> {
            Int2ObjectMap<ExecutionTarget> targetsById = new Int2ObjectOpenHashMap<>();

            MappingContext context = new MappingContext(
                    localNodeName, assignments.nodes(nodeExclusionFilter), template.cluster
            );

            ExecutionTargetFactory targetFactory = context.targetFactory();

            List<IntObjectPair<ExecutionTarget>> allTargets = prepareTargets(template, assignments, targetFactory);

            for (IntObjectPair<ExecutionTarget> pair : allTargets) {
                targetsById.put(pair.firstInt(), pair.second());
            }

            FragmentMapper mapper = new FragmentMapper(context.cluster().getMetadataQuery(), context, targetsById);

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

            return new MappedFragmentsWithNodes(mappedFragmentsList, targetNodes);
        });
    }

    private static List<IntObjectPair<ExecutionTarget>> prepareTargets(
            FragmentsTemplate template,
            DistributionHolder distr,
            ExecutionTargetFactory targetFactory
    ) {
        Stream<IntObjectPair<ExecutionTarget>> tableTargets = template.fragments.stream().flatMap(fragment ->
                fragment.tables().values().stream()
                        .map(table -> IntObjectPair.of(table.id(),
                                targetFactory.partitioned(distr.tableAssignments(table.id())))));

        Stream<IntObjectPair<ExecutionTarget>> viewTargets = template.fragments.stream().flatMap(fragment -> fragment.systemViews().stream()
                .map(view -> IntObjectPair.of(view.id(), buildTargetForSystemView(targetFactory, view, distr.viewNodes(view.id())))));

        return Stream.concat(tableTargets, viewTargets).collect(Collectors.toList());
    }

    private static ExecutionTarget buildTargetForSystemView(ExecutionTargetFactory factory, IgniteSystemView view, List<String> nodes) {
        if (nullOrEmpty(nodes)) {
            throw new SqlException(Sql.MAPPING_ERR, format("The view with name '{}' could not be found on"
                    + " any active nodes in the cluster", view.name()));
        }

        return view.distribution() == IgniteDistributions.single()
                ? factory.oneOf(nodes)
                : factory.allOf(nodes);
    }

    @Override
    public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
        topologyHolder.update(newTopology);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
        // mappingsCache.clear();
    }

    @Override
    public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
        topologyHolder.update(newTopology);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
        // mappingsCache.clear();
    }

    @Override
    public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
        topologyHolder.update(newTopology);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
        // mappingsCache.clear();
    }

    private List<MappedFragment> applyPartitionPruning(
            List<MappedFragment> mappedFragments, 
            MappingParameters parameters, 
            @Nullable PartitionPruningMetadata partitionPruningMetadata
    ) {
        if (partitionPruningMetadata == null) {
            return mappedFragments;
        }
        return partitionPruner.apply(mappedFragments, parameters.dynamicParameters(), partitionPruningMetadata);
    }

    private FragmentsTemplate getOrCreateTemplate(MultiStepPlan plan) {
        // QuerySplitter is deterministic, thus we can cache result in order to reuse it next time
        return templatesCache.get(plan.id(), key -> {
            IdGenerator idGenerator = new IdGenerator(plan.numSources());

            RelOptCluster cluster = Commons.cluster();

            List<Fragment> fragments = new QuerySplitter(idGenerator, cluster).split(plan.getRel());

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
    private static class MappedFragmentsWithNodes {
        final List<MappedFragment> fragments;
        final Set<String> nodes;

        MappedFragmentsWithNodes(List<MappedFragment> fragments, Set<String> nodes) {
            this.fragments = fragments;
            this.nodes = nodes;
        }
    }

    private static class MappingsCacheValue {
        private final long topologyVersion;
        private final IntSet zoneIds;
        // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
        // private final CompletableFuture<MappedFragments> mappedFragments;

        MappingsCacheValue(long topologyVersion, IntSet zoneIds, CompletableFuture<MappedFragmentsWithNodes> mappedFragments) {
            this.topologyVersion = topologyVersion;
            this.zoneIds = zoneIds;
            // TODO: https://issues.apache.org/jira/browse/IGNITE-26465 enable cache
            // this.mappedFragments = mappedFragments;
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

    /** Returns a predicate that returns {@code true} for nodes which should be excluded from mapping. */ 
    private static Predicate<String> composeNodeExclusionFilter(
            TopologySnapshot topologySnapshot, MappingParameters parameters
    ) {
        Predicate<String> filterFromParameters = parameters.nodeExclusionFilter();
        Predicate<String> deadNodesFilter = node -> !topologySnapshot.nodes.contains(node);

        if (filterFromParameters == null) {
            return deadNodesFilter;
        }

        return filterFromParameters.or(deadNodesFilter);
    }

    /**
     * Holder for topology snapshots that guarantees monotonically increasing versions.
     */
    class LogicalTopologyHolder {
        private volatile TopologySnapshot topology = new TopologySnapshot(Long.MIN_VALUE, Set.of());

        void update(LogicalTopologySnapshot topologySnapshot) {
            synchronized (this) {
                if (topology.version() < topologySnapshot.version()) {
                    topology = new TopologySnapshot(topologySnapshot.version(), deriveNodeNames(topologySnapshot));
                }

                if (initialTopologyFuture.isDone() || !topology.nodes().contains(localNodeName)) {
                    return;
                }
            }

            initialTopologyFuture.complete(null);
        }

        TopologySnapshot topology() {
            return topology;
        }

        private Set<String> deriveNodeNames(LogicalTopologySnapshot topology) {
            return topology.nodes().stream()
                    .map(LogicalNode::name)
                    .collect(Collectors.toUnmodifiableSet());
        }

        class TopologySnapshot {
            private final Set<String> nodes;
            private final long version;

            TopologySnapshot(long version, Set<String> nodes) {
                this.version = version;
                this.nodes = nodes;
            }

            public Set<String> nodes() {
                return nodes;
            }

            public long version() {
                return version;
            }
        }
    }

    private static class DistributionHolder {
        private final Set<String> nodes;
        private final Int2ObjectMap<List<TokenizedAssignments>> assignmentsPerTable;
        private final Int2ObjectMap<List<String>> nodesPerView;

        DistributionHolder(
                Set<String> nodes,
                Int2ObjectMap<List<TokenizedAssignments>> assignmentsPerTable,
                Int2ObjectMap<List<String>> nodesPerView) {
            this.nodes = nodes;
            this.assignmentsPerTable = assignmentsPerTable;
            this.nodesPerView = nodesPerView;
        }

        List<String> nodes(@Nullable Predicate<String> nodeExclusionFilter) {
            if (nodeExclusionFilter == null) {
                return List.copyOf(nodes);
            }

            return nodes.stream()
                    .filter(nodeExclusionFilter.negate())
                    .collect(Collectors.toList());
        }

        List<TokenizedAssignments> tableAssignments(int tableId) {
            return assignmentsPerTable.get(tableId);
        }

        List<String> viewNodes(int viewId) {
            return nodesPerView.get(viewId);
        }
    }
}
