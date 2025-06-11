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
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
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
public class MappingServiceImpl implements MappingService {
    private final String localNodeName;
    private final ClockService clock;
    private final Cache<PlanId, FragmentsTemplate> templatesCache;
    private final Cache<MappingsCacheKey, MappingsCacheValue> mappingsCache;
    private final PartitionPruner partitionPruner;
    private final LongSupplier logicalTopologyVerSupplier;
    private final ExecutionDistributionProvider distributionProvider;

    private final boolean enabledColocation = IgniteSystemProperties.enabledColocation();

    /**
     * Constructor.
     *
     * @param localNodeName Name of the current Ignite node.
     * @param clock Clock service to get actual time.
     * @param cacheFactory A factory to create cache of fragments.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param partitionPruner Partition pruner.
     * @param logicalTopologyVerSupplier Logical topology version supplier.
     * @param distributionProvider Execution distribution provider.
     */
    public MappingServiceImpl(
            String localNodeName,
            ClockService clock,
            CacheFactory cacheFactory,
            int cacheSize,
            PartitionPruner partitionPruner,
            LongSupplier logicalTopologyVerSupplier,
            ExecutionDistributionProvider distributionProvider
    ) {
        this.localNodeName = localNodeName;
        this.clock = clock;
        this.templatesCache = cacheFactory.create(cacheSize);
        this.mappingsCache = cacheFactory.create(cacheSize);
        this.partitionPruner = partitionPruner;
        this.logicalTopologyVerSupplier = logicalTopologyVerSupplier;
        this.distributionProvider = distributionProvider;
    }

    /** Called when the primary replica has expired. */
    public CompletableFuture<Boolean> onPrimaryReplicaExpired(PrimaryReplicaEventParameters parameters) {
        assert parameters != null;

        int tableOrZoneId;

        if (enabledColocation) {
            tableOrZoneId = ((ZonePartitionId) parameters.groupId()).zoneId();
        } else {
            tableOrZoneId = ((TablePartitionId) parameters.groupId()).tableId();
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-21201 Move complex computations to a different thread.
        mappingsCache.removeIfValue(value -> value.tabelOrZoneIds.contains(tableOrZoneId));

        return CompletableFutures.falseCompletedFuture();
    }

    @Override
    public CompletableFuture<List<MappedFragment>> map(MultiStepPlan multiStepPlan, MappingParameters parameters) {
        FragmentsTemplate template = getOrCreateTemplate(multiStepPlan);

        boolean mapOnBackups = parameters.mapOnBackups();
        Predicate<String> nodeExclusionFilter = parameters.nodeExclusionFilter();

        CompletableFuture<MappedFragments> mappedFragments;
        if (nodeExclusionFilter != null) {
            mappedFragments = mapFragments(template, mapOnBackups, nodeExclusionFilter);
        } else {
            mappedFragments = mappingsCache.compute(
                    new MappingsCacheKey(multiStepPlan.id(), mapOnBackups),
                    (key, val) -> computeMappingCacheKey(val, template, mapOnBackups)
            ).mappedFragments;
        }

        return mappedFragments.thenApply(frags -> applyPartitionPruning(frags.fragments, parameters));
    }

    private MappingsCacheValue computeMappingCacheKey(
            MappingsCacheValue val,
            FragmentsTemplate template,
            boolean mapOnBackups
    ) {
        if (val == null) {
            IntSet tableOrZoneIds = new IntOpenHashSet();
            boolean topologyAware = false;

            for (Fragment fragment : template.fragments) {
                topologyAware = topologyAware || !fragment.systemViews().isEmpty();
                for (IgniteTable source : fragment.tables().values()) {
                    if (enabledColocation) {
                        tableOrZoneIds.add(source.zoneId());
                    } else {
                        tableOrZoneIds.add(source.id());
                    }
                }
            }

            long topVer = topologyAware ? logicalTopologyVerSupplier.getAsLong() : Long.MAX_VALUE;

            return new MappingsCacheValue(topVer, tableOrZoneIds, mapFragments(template, mapOnBackups, null));
        }

        long topologyVer = logicalTopologyVerSupplier.getAsLong();

        if (val.topologyVersion < topologyVer) {
            return new MappingsCacheValue(topologyVer, val.tabelOrZoneIds, mapFragments(template, mapOnBackups, null));
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
                        .forTable(clock.now(), tbl, mapOnBackups);

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

    private CompletableFuture<MappedFragments> mapFragments(
            FragmentsTemplate template,
            boolean mapOnBackups,
            @Nullable Predicate<String> nodeExclusionFilter
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

            return new MappedFragments(mappedFragmentsList, targetNodes);
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

    private List<MappedFragment> applyPartitionPruning(List<MappedFragment> mappedFragments, MappingParameters parameters) {
        return partitionPruner.apply(mappedFragments, parameters.dynamicParameters());
    }

    private FragmentsTemplate getOrCreateTemplate(MultiStepPlan plan) {
        // QuerySplitter is deterministic, thus we can cache result in order to reuse it next time
        return templatesCache.get(plan.id(), key -> {
            IdGenerator idGenerator = new IdGenerator(0);

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
        private final IntSet tabelOrZoneIds;
        private final CompletableFuture<MappedFragments> mappedFragments;

        MappingsCacheValue(long topologyVersion, IntSet tabelOrZoneIds, CompletableFuture<MappedFragments> mappedFragments) {
            this.topologyVersion = topologyVersion;
            this.tabelOrZoneIds = tabelOrZoneIds;
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
