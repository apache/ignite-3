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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.ExecutionTargetProviderImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl.LogicalTopologyHolder.TopologySnapshot;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PlanId;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruner;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.util.CompletableFutures;

/**
 * An implementation of {@link MappingService}.
 *
 * <p>This particular implementation keeps track of changes in logical cluster topology.
 * Always uses latest topology snapshot to map query.
 */
public class MappingServiceImpl implements MappingService {
    private final LogicalTopologyHolder topologyHolder = new LogicalTopologyHolder();
    private final CompletableFuture<Void> initialTopologyFuture = new CompletableFuture<>();

    private final String localNodeName;
    private final ClockService clock;
    private final ExecutionTargetProvider targetProvider;
    private final Cache<PlanId, FragmentsTemplate> templatesCache;
    private final Cache<MappingsCacheKey, MappingsCacheValue> mappingsCache;
    private final Executor taskExecutor;
    private final PartitionPruner partitionPruner;
    private final LogicalTopologyService logicalTopologyService;;

    /**
     * Constructor.
     *
     * @param localNodeName Name of the current Ignite node.
     * @param clock Clock service to get actual time.
     * @param targetProvider Execution target provider.
     * @param cacheFactory A factory to create cache of fragments.
     * @param cacheSize Size of the cache of query plans. Should be non negative.
     * @param partitionPruner Partition pruner.
     * @param taskExecutor Mapper service task executor.
     * @param logicalTopologyService Logical topology.
     */
    public MappingServiceImpl(
            String localNodeName,
            ClockService clock,
            ExecutionTargetProvider targetProvider,
            CacheFactory cacheFactory,
            int cacheSize,
            PartitionPruner partitionPruner,
            Executor taskExecutor,
            LogicalTopologyService logicalTopologyService
    ) {
        this.localNodeName = localNodeName;
        this.clock = clock;
        this.targetProvider = targetProvider;
        this.templatesCache = cacheFactory.create(cacheSize);
        this.mappingsCache = cacheFactory.create(cacheSize);
        this.taskExecutor = taskExecutor;
        this.partitionPruner = partitionPruner;
        this.logicalTopologyService = logicalTopologyService;
    }

    // TODO REMOVE
    public MappingServiceImpl(
            String localNodeName,
            ClockService clock,
            ExecutionTargetProvider targetProvider,
            CacheFactory cacheFactory,
            int cacheSize,
            PartitionPruner partitionPruner,
            Executor taskExecutor
    ) {
        this.localNodeName = localNodeName;
        this.clock = clock;
        this.targetProvider = targetProvider;
        this.templatesCache = cacheFactory.create(cacheSize);
        this.mappingsCache = cacheFactory.create(cacheSize);
        this.taskExecutor = taskExecutor;
        this.partitionPruner = partitionPruner;
        this.logicalTopologyService = null;
    }

    @Override
    public CompletableFuture<List<MappedFragment>> map(MultiStepPlan multiStepPlan, MappingParameters parameters) {
        return map0(multiStepPlan, parameters);
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

    private CompletableFuture<List<MappedFragment>> map0(MultiStepPlan multiStepPlan, MappingParameters parameters) {
        FragmentsTemplate template = getOrCreateTemplate(multiStepPlan, MappingContext.CLUSTER);

        List<Fragment> fragments = new ArrayList<>(template.fragments);
        Set<IgniteTable> tables = fragments.stream().flatMap(fragment -> fragment.tables().values().stream()).collect(Collectors.toSet());
        boolean mapOnBackups = parameters.mapOnBackups();

        CompletableFuture<TopologySnapshot> topologySnapshot = topologyHolder.topology(tables, clock.now(), mapOnBackups);

        return topologySnapshot.thenCompose(topology -> {
            MappingContext context = new MappingContext(localNodeName, topology.nodes());

            Map<IgniteTable, List<TokenizedAssignments>> assignmentsPerTable = topology.assignmentsPerTable();

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

                            long topVer = topologyAware ? topology.version() : Long.MAX_VALUE;

                            return new MappingsCacheValue(topVer, tableIds, mapFragments(context, template, assignmentsPerTable));
                        }

                        if (val.topVer < topology.version()) {
                            return new MappingsCacheValue(topology.version(), val.tableIds, mapFragments(context, template, assignmentsPerTable));
                        }

                        return val;
                    }
            );

            return CompletableFuture.completedFuture(applyPartitionPruning(cacheValue.mappedFragments.fragments, parameters));
        });
    }

    private MappedFragments mapFragments(
            MappingContext context,
            FragmentsTemplate template,
            Map<IgniteTable, List<TokenizedAssignments>> assignmentsPerTable
    ) {
        IdGenerator idGenerator = new IdGenerator(template.nextId);
        List<Fragment> fragments = new ArrayList<>(template.fragments);

        Stream<IntObjectPair<ExecutionTarget>> ss1 = fragments.stream().flatMap(fragment -> fragment.tables().values().stream()
                .map(table -> IntObjectPair.of(table.id(), context.targetFactory().partitioned(assignmentsPerTable.get(table)))));

        Stream<IntObjectPair<ExecutionTarget>> ss2 = fragments.stream().flatMap(fragment -> fragment.systemViews().stream()
                .map(view -> IntObjectPair.of(view.id(), targetProvider.forSystemView(context.targetFactory(), view).join()))); /// !!!

        List<IntObjectPair<ExecutionTarget>> targets0 = Stream.concat(ss1, ss2).collect(Collectors.toList());

        Int2ObjectMap<ExecutionTarget> targetsById = new Int2ObjectOpenHashMap<>();

        for (IntObjectPair<ExecutionTarget> pair : targets0) {
            targetsById.put(pair.firstInt(), pair.second());
        }

        FragmentMapper mapper = new FragmentMapper(template.cluster.getMetadataQuery(), context, targetsById);

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
    }

    private List<MappedFragment> applyPartitionPruning(List<MappedFragment> mappedFragments, MappingParameters parameters) {
        return partitionPruner.apply(mappedFragments, parameters.dynamicParameters());
    }

    /**
     * Holder for topology snapshots that guarantees monotonically increasing versions.
     */
    class LogicalTopologyHolder {

        CompletableFuture<TopologySnapshot> topology(Collection<IgniteTable> tables, HybridTimestamp operationTime, boolean mapOnBackups) {
            List<CompletableFuture<List<TokenizedAssignments>>> result = new ArrayList<>();
            Map<IgniteTable, CompletableFuture<List<TokenizedAssignments>>> mapResult = new HashMap<>();
            Map<IgniteTable, List<TokenizedAssignments>> mapResultResolved = new HashMap<>();

            for (IgniteTable t : tables) {
                CompletableFuture<List<TokenizedAssignments>> assignments = ((ExecutionTargetProviderImpl) targetProvider).collectAssignments(
                        t, operationTime, mapOnBackups);

                result.add(assignments);
                mapResult.put(t, assignments);
            }

            CompletableFuture<Void> all = CompletableFuture.allOf(result.toArray(new CompletableFuture[0]));

            CompletableFuture<List<List<TokenizedAssignments>>> fut = all.thenApply(v -> result.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toUnmodifiableList())
            );

            if (tables.isEmpty()) {
                LogicalTopologySnapshot topologySnapshot = logicalTopologyService.localLogicalTopology();
                // todo !! fix check for null !!! local node need to be enough or not ?
                TopologySnapshot ret = new TopologySnapshot(topologySnapshot.version(),
                        topologySnapshot.nodes().stream().map(ClusterNodeImpl::name).collect(Collectors.toList()), null);
                return CompletableFuture.completedFuture(ret);
            } else {
                CompletableFuture<List<String>> participantNodes = fut.thenApply(
                        v -> v.stream().flatMap(List::stream).flatMap(i -> i.nodes().stream()).map(Assignment::consistentId)
                                .collect(Collectors.toList()));

                return participantNodes.thenApply(l -> {
                    l.add(localNodeName);

                    mapResult.forEach((k, v) -> mapResultResolved.put(k, v.join()));

                    return new TopologySnapshot(100, l, mapResultResolved);
                });
            }
        }

        private List<String> deriveNodeNames(LogicalTopologySnapshot topology) {
            return topology.nodes().stream()
                    .map(LogicalNode::name)
                    .collect(Collectors.toUnmodifiableList());
        }

        class TopologySnapshot {
            private final List<String> nodes;
            private final Map<IgniteTable, List<TokenizedAssignments>> assignmentsPerTable;
            private final long version;

            TopologySnapshot(long version, List<String> nodes, Map<IgniteTable, List<TokenizedAssignments>> assignmentsPerTable) {
                this.version = version;
                this.nodes = nodes;
                this.assignmentsPerTable = assignmentsPerTable;
            }

            public List<String> nodes() {
                return nodes;
            }

            public long version() {
                return version;
            }

            public Map<IgniteTable, List<TokenizedAssignments>> assignmentsPerTable() {
                return assignmentsPerTable;
            }
        }
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
        private final long topVer;
        private final IntSet tableIds;
        private final MappedFragments mappedFragments;

        MappingsCacheValue(long topVer, IntSet tableIds, MappedFragments mappedFragments) {
            this.topVer = topVer;
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
