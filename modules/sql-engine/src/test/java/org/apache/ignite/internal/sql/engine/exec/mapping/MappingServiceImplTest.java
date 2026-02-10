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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruner;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class to verify {@link MappingServiceImpl}.
 */
@SuppressWarnings("ThrowFromFinallyBlock")
public class MappingServiceImplTest extends BaseIgniteAbstractTest {
    private static final String ZONE_NAME_1 = "ZONE1";
    private static final String ZONE_NAME_2 = "ZONE2";

    private static final MultiStepPlan PLAN;
    private static final MultiStepPlan PLAN_WITH_SYSTEM_VIEW;
    private static final TestCluster cluster;
    private static final ClockService CLOCK_SERVICE = new TestClockService(new TestHybridClock(System::currentTimeMillis));
    private static final MappingParameters PARAMS = MappingParameters.EMPTY;
    private static final PartitionPruner PARTITION_PRUNER = (fragments, dynParams, ppMetadata) -> fragments;

    static {
        // @formatter:off
        cluster = TestBuilders.cluster()
                .nodes("N1")
                .addSystemView(SystemViews.<Object[]>clusterViewBuilder()
                        .name("TEST_VIEW")
                        .addColumn("ID", NativeTypes.INT64, v -> v[0])
                        .addColumn("WORD", NativeTypes.stringOf(64), v -> v[1])
                        .dataProvider(SubscriptionUtils.fromIterable(Collections.singleton(new Object[]{42L, "blah"})))
                        .build())
                .build();
        // @formatter:on

        cluster.start();

        //noinspection ConcatenationWithEmptyString
        cluster.node("N1").initSchema("" 
                + format("CREATE ZONE {} STORAGE PROFILES ['default'];", ZONE_NAME_1)
                + format("CREATE ZONE {} STORAGE PROFILES ['default'];", ZONE_NAME_2)
                + format("CREATE TABLE t1 (id INT PRIMARY KEY, val INT) ZONE {};", ZONE_NAME_1)
                + format("CREATE TABLE t2 (id INT PRIMARY KEY, val INT) ZONE {};", ZONE_NAME_2)
        );

        try {
            PLAN = (MultiStepPlan) cluster.node("N1").prepare("SELECT * FROM t1");
            PLAN_WITH_SYSTEM_VIEW = (MultiStepPlan) cluster.node("N1").prepare("SELECT * FROM system.test_view");
        } finally {
            try {
                cluster.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26465")
    public void cacheOnStableTopology() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of(localNodeName, "NODE1");

        // Initialize mapping service.
        TestExecutionDistributionProvider execProvider = Mockito.spy(new TestExecutionDistributionProvider(nodeNames));

        MappingServiceImpl mappingService = Mockito.spy(new MappingServiceImpl(
                localNodeName,
                CLOCK_SERVICE,
                CaffeineCacheFactory.INSTANCE,
                100,
                PARTITION_PRUNER,
                execProvider,
                Runnable::run
        ));

        LogicalTopology logicalTopology = TestBuilders.logicalTopology(nodeNames);

        mappingService.onTopologyLeap(logicalTopology.getLogicalTopology());

        MappedFragments defaultMapping = await(mappingService.map(PLAN, PARAMS));
        MappedFragments mappingOnBackups = await(mappingService.map(PLAN, MappingParameters.MAP_ON_BACKUPS));

        verify(execProvider, times(2)).forTable(any(HybridTimestamp.class), any(IgniteTable.class), anyBoolean());

        assertSame(defaultMapping, await(mappingService.map(PLAN, PARAMS)));
        assertSame(mappingOnBackups, await(mappingService.map(PLAN, MappingParameters.MAP_ON_BACKUPS)));
        assertNotSame(defaultMapping, mappingOnBackups);
    }

    @Test
    public void serviceInitializationTest() {
        String localNodeName = "NODE0";

        MappingServiceImpl mappingService = createMappingServiceNoCache(localNodeName, List.of(localNodeName));

        CompletableFuture<MappedFragments> mappingFuture = mappingService.map(PLAN, PARAMS);

        assertThat(mappingFuture, willSucceedFast());
    }

    @Test
    void mappingWithNodeFilter() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of("NODE1", "NODE2");

        MappingService service = createMappingService(localNodeName, nodeNames, 100);

        MappedFragments defaultMapping = await(service.map(PLAN_WITH_SYSTEM_VIEW, PARAMS));

        assertThat(defaultMapping.fragments(), hasSize(2));

        MappedFragment leafFragment = defaultMapping.fragments().stream()
                .filter(fragment -> !fragment.fragment().rootFragment())
                .findFirst()
                .orElseThrow();

        assertThat(leafFragment.nodes(), hasSize(1));

        String nodeToExclude = leafFragment.nodes().get(0);

        MappingParameters params = MappingParameters.create(new Object[0], false, nodeToExclude::equals);
        MappedFragments mappingWithExclusion = await(service.map(PLAN_WITH_SYSTEM_VIEW, params));

        assertNotSame(defaultMapping, mappingWithExclusion);

        for (MappedFragment fragment : mappingWithExclusion.fragments()) {
            assertThat(nodeToExclude, not(in(fragment.nodes())));
        }
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26465")
    public void testCacheInvalidationOnTopologyChange() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of(localNodeName, "NODE1");

        TestExecutionDistributionProvider execProvider = Mockito.spy(new TestExecutionDistributionProvider(nodeNames));

        MappingServiceImpl mappingService = Mockito.spy(new MappingServiceImpl(
                localNodeName,
                CLOCK_SERVICE,
                CaffeineCacheFactory.INSTANCE,
                100,
                PARTITION_PRUNER,
                execProvider,
                Runnable::run
        ));

        LogicalTopology logicalTopology = TestBuilders.logicalTopology(nodeNames);

        mappingService.onTopologyLeap(logicalTopology.getLogicalTopology());

        MappedFragments tableOnlyMapping = await(mappingService.map(PLAN, PARAMS));
        MappedFragments sysViewMapping = await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS));

        verify(execProvider, times(1)).forTable(any(HybridTimestamp.class), any(IgniteTable.class), anyBoolean());
        verify(execProvider, times(1)).forSystemView(any());

        verify(mappingService, times(2)).composeDistributions(anySet(), anySet(), anyBoolean());

        assertSame(tableOnlyMapping, await(mappingService.map(PLAN, PARAMS)));
        assertSame(sysViewMapping, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));

        verifyNoMoreInteractions(execProvider);

        // Update topology
        LogicalNode node = logicalTopology.getLogicalTopology().nodes().iterator().next();
        logicalTopology.removeNodes(Set.of(node));
        logicalTopology.putNode(node);
        mappingService.onTopologyLeap(logicalTopology.getLogicalTopology());

        assertNotSame(sysViewMapping, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));
        assertNotSame(tableOnlyMapping, await(mappingService.map(PLAN, PARAMS)));

        verify(execProvider, times(2)).forTable(any(HybridTimestamp.class), any(IgniteTable.class), anyBoolean());
        verify(execProvider, times(2)).forSystemView(any());
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26465")
    public void testCacheInvalidationOnPrimaryZoneExpiration() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of(localNodeName, "NODE1");

        Function<String, PrimaryReplicaEventParameters> prepareEvtParams = (name) -> {
            CatalogZoneDescriptor zoneDescriptor = cluster.catalogManager().latestCatalog().zone(name);

            assertNotNull(zoneDescriptor);

            return new PrimaryReplicaEventParameters(
                    0, new ZonePartitionId(zoneDescriptor.id(), 0), new UUID(0, 0), "ignored", HybridTimestamp.MIN_VALUE);
        };

        // Initialize mapping service.
        ExecutionDistributionProvider execProvider = Mockito.spy(new TestExecutionDistributionProvider(nodeNames));

        MappingServiceImpl mappingService = Mockito.spy(new MappingServiceImpl(
                localNodeName,
                CLOCK_SERVICE,
                CaffeineCacheFactory.INSTANCE,
                100,
                PARTITION_PRUNER,
                execProvider,
                Runnable::run
        ));

        LogicalTopology logicalTopology = TestBuilders.logicalTopology(nodeNames);

        mappingService.onTopologyLeap(logicalTopology.getLogicalTopology());

        MappedFragments mappedFragments = await(mappingService.map(PLAN, PARAMS));
        verify(execProvider, times(1)).forTable(any(HybridTimestamp.class), any(IgniteTable.class), anyBoolean());

        // Simulate expiration of the primary replica for non-mapped table - the cache entry should not be invalidated.
        await(mappingService.onPrimaryReplicaExpired(prepareEvtParams.apply(ZONE_NAME_2)));
        assertSame(mappedFragments, await(mappingService.map(PLAN, PARAMS)));

        verify(mappingService, times(1)).composeDistributions(anySet(), anySet(), anyBoolean());

        // Simulate expiration of the primary replica for mapped table - the cache entry should be invalidated.
        await(mappingService.onPrimaryReplicaExpired(prepareEvtParams.apply(ZONE_NAME_1)));
        assertNotSame(mappedFragments, await(mappingService.map(PLAN, PARAMS)));
        verify(execProvider, times(2)).forTable(any(HybridTimestamp.class), any(IgniteTable.class), anyBoolean());
    }

    private static MappingServiceImpl createMappingServiceNoCache(String localNodeName, List<String> nodeNames) {
        return createMappingService(localNodeName, nodeNames, 0);
    }

    private static MappingServiceImpl createMappingService(String localNodeName, List<String> nodeNames, int cacheSize) {
        ExecutionDistributionProvider execProvider = new TestExecutionDistributionProvider(nodeNames);

        var service = new MappingServiceImpl(
                localNodeName,
                CLOCK_SERVICE,
                CaffeineCacheFactory.INSTANCE,
                cacheSize,
                PARTITION_PRUNER,
                execProvider,
                Runnable::run
        );

        Set<String> allNodes = new HashSet<>(nodeNames);
        allNodes.add(localNodeName);

        LogicalTopology logicalTopology = TestBuilders.logicalTopology(allNodes);

        service.onTopologyLeap(logicalTopology.getLogicalTopology());

        return service;
    }

    /** Test distribution provider. */
    public static class TestExecutionDistributionProvider implements ExecutionDistributionProvider {
        private final List<String> nodeNames;
        private Supplier<RuntimeException> exceptionSupplier = () -> null;

        TestExecutionDistributionProvider(List<String> nodeNames) {
            this.nodeNames = nodeNames;
        }

        /** Constructor. */
        public TestExecutionDistributionProvider(List<String> nodeNames, Supplier<RuntimeException> exceptionSupplier) {
            this.nodeNames = nodeNames;
            this.exceptionSupplier = exceptionSupplier;
        }

        private static TokenizedAssignments mapAssignment(String peer) {
            Set<Assignment> peers = Set.of(Assignment.forPeer(peer));
            return new TokenizedAssignmentsImpl(peers, 1L);
        }

        @Override
        public CompletableFuture<List<TokenizedAssignments>> forTable(HybridTimestamp operationTime, IgniteTable table,
                boolean includeBackups) {
            if (exceptionSupplier.get() != null) {
                return CompletableFuture.failedFuture(exceptionSupplier.get());
            }

            return CompletableFuture.completedFuture(nodeNames.stream()
                    .map(TestExecutionDistributionProvider::mapAssignment).collect(Collectors.toList()));
        }

        @Override
        public List<String> forSystemView(IgniteSystemView view) {
            return nodeNames;
        }
    }
}
